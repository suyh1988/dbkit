package sync

import (
	"context"
	"database/sql"
	"example.com/m/v2/conf"
	"example.com/m/v2/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"strings"
	"sync"
	"time"
)

// InitMongoDB 初始化 MongoDB 客户端
func InitMongoDB(config *conf.Config) (*mongo.Client, error) {
	// 设置连接超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 解析配置中的选项
	clientOptions := options.Client().ApplyURI(config.Target.MongoDB.URI).
		SetMaxPoolSize(uint64(config.MongoDB.Options.MaxPoolSize)).
		SetConnectTimeout(time.Millisecond * time.Duration(config.MongoDB.Options.ConnectTimeoutMS)) // 确保单位正确

	// 连接到 MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// 确保连接可用
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx) // 连接不可用，立即关闭
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	log.Info().Msg("Connected to MongoDB server")
	return client, nil
}

func DumpFullMySQLTableToMongoDB(db *sql.DB, conf *conf.Config, mongoClient *mongo.Client, options *model.DaemonOptions,
	concurrencyLimit int) (*mysql.Position, error) {
	if concurrencyLimit <= 0 {
		concurrencyLimit = 3 // 默认并行导出表的数量
	}

	var (
		binlogPos mysql.Position
		err       error
	)

	// 开启一致性视图
	tx, err := db.BeginTx(options.Ctx, nil)
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// 获取当前的 binlog 位点
	row := tx.QueryRow("SHOW MASTER STATUS")
	var discard1, discard2, discard3 interface{}
	err = row.Scan(&binlogPos.Name, &binlogPos.Pos, &discard1, &discard2, &discard3)
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to get binlog position: %v", err)
	}

	// 使用 Goroutines 并行导出多个表的数据
	var wg sync.WaitGroup

	// 遍历配置，处理每个表的数据
	for _, mapping := range conf.Mapping {
		for _, table := range mapping.Tables {
			err = dumpSingleTableDataBatchToMongo(tx,
				mongoClient,
				mapping.Database,
				table.Table,
				conf.MongoDB.Primary,
				options)
			if err != nil {
				return nil, err
				log.Error().Err(err).Msgf("Failed to dump table %s.%s", mapping.Database, table.Table)
			}
		}
	}

	// 等待所有 Goroutines 完成
	wg.Wait()

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to commit transaction: %v", err)
	}

	// 返回 binlog 位点
	return &binlogPos, nil
}

// 从 MySQL 读取数据并批量插入 MongoDB
func dumpSingleTableDataBatchToMongo(tx *sql.Tx, mongoClient *mongo.Client, dbName, tableName, primary string, options *model.DaemonOptions) error {

	// 获取 MongoDB 集合
	collection := mongoClient.Database(dbName).Collection(tableName)

	// mongodb集合索引处理
	// 如果MySQL是符合主键索引时，mongodb的集合创建复合唯一索引与之对应
	// 如果MySQL表是单字段主键索引时，根据配置文件mongodb.primary的值，true: Mongodb集合会将这个字段名改成_id,作为mongodb的集合主键索引; false: 同上，建唯一键索引
	if len(options.MysqlSync.PrimaryKeyColumnNames[dbName+"."+tableName]) > 1 && primary != "ture" {
		err := createMongoUniqueIndex(collection, options.MysqlSync.PrimaryKeyColumnNames[dbName+"."+tableName])
		if err != nil {
			log.Error().Err(err).Msgf("%s.%s add unique index failed.", dbName, tableName)
		}
	}

	// 查询 MySQL 表数据
	query := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(options.MysqlSync.TableColumnMap[dbName+"."+tableName], ", "), dbName, tableName)
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", dbName, tableName, err)
	}
	defer rows.Close()

	// 读取列名
	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %v", err)
	}

	// 预分配存储行数据的切片
	var documents []interface{}

	// 遍历查询结果
	for rows.Next() {
		// 用于存储当前行的数据
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i] // 指针存储每列数据
		}

		// 读取一行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 构造 BSON 文档
		doc := bson.M{}
		for i, colName := range columnNames {
			// **确保 `values[i]` 类型正确**
			var value interface{}
			switch v := values[i].(type) {
			case nil:
				value = nil // 直接存 `null`
			case []byte:
				value = string(v) // 确保 `BinData` 变成 `string`
			case time.Time:
				value = v.Format("2006-01-02 15:04:05") // 统一转换为字符串格式
			case sql.NullString:
				if v.Valid {
					value = v.String
				} else {
					value = nil
				}
			case sql.NullInt64:
				if v.Valid {
					value = v.Int64
				} else {
					value = nil
				}
			default:
				value = fmt.Sprintf("%v", v) // 确保转换为可读格式
			}

			pkNames := options.MysqlSync.PrimaryKeyColumnNames[dbName+"."+tableName]
			if Contains(pkNames, colName) && len(pkNames) == 1 && primary == "true" {
				doc["_id"] = value
			} else {
				doc[colName] = value
			}
		}

		documents = append(documents, doc)

		// 每 batchSize 条数据执行一次批量插入
		if len(documents) >= options.MysqlSync.WriteBatchSize {
			if err := insertDocuments(collection, documents); err != nil {
				return fmt.Errorf("failed to insert batch into MongoDB: %v", err)
			}
			documents = documents[:0] // 清空切片，准备下一批数据
		}
	}

	// 插入剩余的数据
	if len(documents) > 0 {
		if err := insertDocuments(collection, documents); err != nil {
			return fmt.Errorf("failed to insert remaining documents into MongoDB: %v", err)
		}
	}

	return nil
}

// 增量同步主逻辑
func syncBinlogToMongoDB(syncer *replication.BinlogSyncer, mongoClient *mongo.Client, position *mysql.Position, syncConf *conf.Config, options *model.DaemonOptions, db *sql.DB) error {
	streamer, err := syncer.StartSync(*position)
	if err != nil {
		log.Error().Err(err).Msg("启动 Binlog 同步失败")
		return err
	}
	log.Info().Msg("increase sync begin running...")

	var i int64
	t := time.Duration(options.MysqlSync.WriteTimeInterval) * time.Second
	timer := time.NewTimer(t)
	defer timer.Stop() // 确保退出时停止计时器

	for {
		select {
		// 定时器触发刷新位点
		case <-timer.C:
			err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, fmt.Sprintf("%s:%d", position.Name, position.Pos))
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("定时器触发时保存 Binlog 位点失败: %s", position.String()))
				return err
			}
			log.Debug().Msg(fmt.Sprintf("定时器触发保存 Binlog 位点成功: %s", position.String()))
			// 重置计时器
			timer.Reset(t)

		// 处理 Binlog 事件
		default:
			ev, err := streamer.GetEvent(options.Ctx)
			if err != nil {
				log.Error().Err(err).Msg("读取 Binlog 事件失败")
				return err
			}
			position.Pos = ev.Header.LogPos

			// 每达到事件阈值刷新位点
			if i%options.MysqlSync.WriteEventInterval == 0 {
				err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, fmt.Sprintf("%s:%d", position.Name, position.Pos))
				if err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("事件触发时保存 Binlog 位点失败: %s", position.String()))
					return err
				}
				log.Debug().Msg(fmt.Sprintf("事件触发保存 Binlog 位点成功: %s", position.String()))
				// 重置计时器
				timer.Reset(t)
			}
			i++

			switch e := ev.Event.(type) {
			case *replication.RowsEvent:
				for _, mapping := range syncConf.Mapping {
					for _, table := range mapping.Tables {
						if string(e.Table.Schema) == mapping.Database && string(e.Table.Table) == table.Table {
							processRowsEventMongoDB(mongoClient, ev, syncConf, options)
						}
					}
				}
			case *replication.RotateEvent:
				position.Name = string(e.NextLogName)
				position.Pos = uint32(e.Position)
				log.Debug().Msg(fmt.Sprintf("切换到 Binlog 文件: %s, 位点: %d", position.Name, position.Pos))

			case *replication.QueryEvent:
				// 检测表的 DDL 变更并刷新列名
				sqlStr := strings.ReplaceAll(strings.ToUpper(string(e.Query)), "`", "")
				ddlRegex := regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(?:\w+\.)?(users)\s+ADD\s+COLUMN`)
				match := ddlRegex.FindStringSubmatch(sqlStr)
				if len(match) > 0 {
					err = FlushColumnNames(options, db, syncConf)
					if err != nil {
						log.Error().Err(err).Msg("刷新表列名失败")
						return err
					}
				}

			default:
				// 处理其他类型事件
			}
		}
	}
}

// 处理行事件
func processRowsEventMongoDB(mongoClient *mongo.Client, event *replication.BinlogEvent, syncConf *conf.Config, options *model.DaemonOptions) {
	// 确保事件是 RowsEvent 类型
	rowsEvent, ok := event.Event.(*replication.RowsEvent)
	if !ok {
		log.Warn().Msgf("未处理的事件类型: %v", event.Header.EventType)
		return
	}

	eventDB := string(rowsEvent.Table.Schema)
	eventTable := string(rowsEvent.Table.Table)

	// 根据事件类型进行处理
	switch event.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		_ = handleInsertEventMongo(mongoClient, syncConf, rowsEvent, options)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		_ = handleUpdateEventMongo(mongoClient, syncConf, rowsEvent, options)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		_ = handleDeleteEventMongo(mongoClient, syncConf, rowsEvent, options)
	default:
		log.Debug().Msgf("未处理的事件类型: %s.%s - EventType: %v", eventDB, eventTable, event.Header.EventType)
	}
}

// 处理 binlog 插入事件并写入 MongoDB
func handleInsertEventMongo(client *mongo.Client, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	var rows []interface{} // MongoDB 批量插入的格式

	eventDB := string(e.Table.Schema)
	eventTable := string(e.Table.Table)

	// 遍历映射配置
	for _, confMap := range syncConf.Mapping {
		if confMap.Database != eventDB {
			continue
		}

		for _, table := range confMap.Tables {
			if table.Table != eventTable {
				continue
			}

			collection := client.Database(eventDB).Collection(eventTable)
			columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
			log.Debug().Msgf("%s.%s columns(%d):%v", eventDB, eventTable, len(columns), columns)
			if len(table.Columns) == 0 {
				table.Columns = columns
			}

			// 遍历 binlog 的行数据
			for _, row := range e.Rows {
				// 构造当前行的 BSON 文档
				rowData := bson.M{}
				for i, col := range columns {
					if i < len(row) { // 避免索引越界
						for _, selectColumn := range table.Columns {
							if strings.EqualFold(col, selectColumn) { // 忽略大小写匹配
								var value interface{}
								switch v := row[i].(type) {
								case []byte:
									value = string(v) // 转换成字符串，避免存入 BinData
								case time.Time:
									value = v.Format("2006-01-02 15:04:05") // 如果你不想存 ISODate，可以改为字符串
								default:
									value = fmt.Sprintf("%v", v)
								}

								pkNames := options.MysqlSync.PrimaryKeyColumnNames[eventDB+"."+eventTable]
								if Contains(pkNames, col) && len(pkNames) == 1 && syncConf.MongoDB.Primary == "true" {
									rowData["_id"] = value
								} else {
									rowData[col] = value
								}
							}
						}
					}
				}

				// 添加到批量缓存
				rows = append(rows, rowData)

				// 达到批量大小，写入 MongoDB
				if len(rows) >= options.MysqlSync.WriteBatchSize {
					if err := insertDocuments(collection, rows); err != nil {
						log.Error().Err(err).Msg("Failed to insert into MongoDB")
						return err
					}
					rows = rows[:0] // 清空
				}
			}

			// 插入剩余数据
			if len(rows) > 0 {
				if err := insertDocuments(collection, rows); err != nil {
					log.Error().Err(err).Msg("Failed to insert remaining documents into MongoDB")
					return err
				}
				rows = rows[:0]
			}
		}
	}

	return nil
}

func insertDocuments(collection *mongo.Collection, rows []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := collection.InsertMany(ctx, rows)
	if err != nil {
		log.Error().Err(err).Msg("insert many docs failed")
		return fmt.Errorf("failed to insert documents: %v", err)
	}

	return nil
}

// 处理 binlog 更新事件并写入 MongoDB
func handleUpdateEventMongo(client *mongo.Client, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	eventDB := string(e.Table.Schema)
	eventTable := string(e.Table.Table)

	// 遍历映射配置
	for _, confMap := range syncConf.Mapping {
		if confMap.Database != eventDB {
			continue
		}

		for _, table := range confMap.Tables {
			if table.Table != eventTable {
				continue
			}

			collection := client.Database(eventDB).Collection(eventTable)
			columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
			log.Info().Msgf("%s.%s columns(%d):%v", eventDB, eventTable, len(columns), columns)
			if len(table.Columns) == 0 {
				table.Columns = columns
			}

			// 判断update Event 的rows是否符合预期
			if len(e.Rows)%2 != 0 {
				log.Warn().Msgf("Unexpected row event length: %d, skipping update event", len(e.Rows))
				continue
			}

			// 遍历 binlog 更新事件 (两条数据一组)
			for i := 0; i < len(e.Rows); i += 2 {
				before := e.Rows[i]  // 旧数据 (更新前)
				after := e.Rows[i+1] // 新数据 (更新后)
				log.Info().Msgf("before: %v", before)
				log.Info().Msgf("after: %v", after)

				// 构造 WHERE 过滤条件
				filter := bson.M{}
				//MySQL有主键的按主键删除即可
				var deleteFilterColumnNames []string
				pkNames := options.MysqlSync.PrimaryKeyColumnNames[eventDB+"."+eventTable]
				if len(pkNames) > 0 {
					//用索引去过滤时，需要考虑单字段主键且配置文件mongodb.primary="true"时的特殊情况
					if len(pkNames) == 1 && syncConf.MongoDB.Primary == "true" {
						deleteFilterColumnNames = []string{"_id"}
					} else {
						deleteFilterColumnNames = pkNames
					}
				} else {
					deleteFilterColumnNames = table.Columns
				}
				for i, col := range columns {
					if i < len(before) {
						for _, selectColumn := range deleteFilterColumnNames {
							if strings.EqualFold(col, selectColumn) {
								var value interface{}
								switch v := before[i].(type) {
								case []byte:
									value = string(v)
								case time.Time:
									value = v // 保持时间类型
								case int64, int32, int:
									value = v
								default:
									value = fmt.Sprintf("%v", v) // 确保转换为可读格式
								}
								if value != nil { // 避免 null 值影响匹配
									filter[col] = value
								}
							}
						}
					}
				}
				log.Info().Msgf("Adjusted filter: %v", filter)

				// 构造更新内容 (SET)
				update := bson.M{}
				for i, col := range columns {
					if i < len(after) { // 避免超出索引范围
						for _, selectColumn := range table.Columns {
							if strings.EqualFold(col, selectColumn) {
								var value interface{}
								switch v := after[i].(type) {
								case []byte:
									value = string(v)
								case time.Time:
									value = v.Format("2006-01-02 15:04:05")
								default:
									value = v
								}
								update[col] = value
							}
						}
					}
				}
				log.Info().Msgf("update: %v", update)

				// 执行更新
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				result, err := collection.UpdateOne(ctx, filter, bson.M{"$set": update})
				if err != nil {
					log.Error().Err(err).Msg("Failed to update MongoDB")
					return err
				}

				log.Info().Msgf("Matched %v document(s), modified %v document(s)", result.MatchedCount, result.ModifiedCount)
			}
		}
	}

	return nil
}

// 处理 binlog 删除事件并写入 MongoDB
func handleDeleteEventMongo(client *mongo.Client, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	eventDB := string(e.Table.Schema)
	eventTable := string(e.Table.Table)

	// 遍历映射配置
	for _, confMap := range syncConf.Mapping {
		if confMap.Database != eventDB {
			continue
		}

		for _, table := range confMap.Tables {
			if table.Table != eventTable {
				continue
			}

			collection := client.Database(eventDB).Collection(eventTable)
			columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
			log.Debug().Msgf("%s.%s columns(%d):%v", eventDB, eventTable, len(columns), columns)
			if len(table.Columns) == 0 {
				table.Columns = columns
			}

			// 遍历删除的行
			for _, row := range e.Rows {
				filter := bson.M{} // 过滤条件 (WHERE)
				//MySQL有主键的按主键删除即可
				var deleteFilterColumnNames []string
				pkNames := options.MysqlSync.PrimaryKeyColumnNames[eventDB+"."+eventTable]
				if len(pkNames) > 0 {
					//用索引去过滤时，需要考虑单字段主键且配置文件mongodb.primary="true"时的特殊情况
					if len(pkNames) == 1 && syncConf.MongoDB.Primary == "true" {
						deleteFilterColumnNames = []string{"_id"}
					} else {
						deleteFilterColumnNames = pkNames
					}
				} else {
					deleteFilterColumnNames = table.Columns
				}
				for i, col := range columns {
					if i < len(row) { // 避免超出索引范围
						for _, selectColumn := range deleteFilterColumnNames {
							if strings.EqualFold(col, selectColumn) { // 忽略大小写匹配列名
								var value interface{}
								switch v := row[i].(type) {
								case []byte:
									value = string(v) // 转换成字符串，避免存入 BinData
								case time.Time:
									value = v.Format("2006-01-02 15:04:05") // 如果你不想存 ISODate，可以改为字符串
								default:
									value = fmt.Sprintf("%v", v) // 确保转换为可读格式
								}
								filter[col] = value
							}
						}
					}
				}

				// 执行删除
				fmt.Printf("%s\n", filter)
				result, err := collection.DeleteOne(options.Ctx, filter)
				if err != nil {
					log.Error().Err(err).Msg("Failed to delete document from MongoDB")
					return err
				}

				log.Debug().Msgf("Deleted %v document(s)", result.DeletedCount)
			}
		}
	}

	return nil
}

func createMongoUniqueIndex(collection *mongo.Collection, pkNames []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	index := bson.M{}
	for _, pkName := range pkNames {
		index[pkName] = 1
	}
	indexModel := mongo.IndexModel{
		Keys:    index,
		Options: options.Index().SetUnique(true), // 设置唯一索引
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	return err
}
