package sync

import (
	"database/sql"
	"example.com/m/v2/conf"
	"example.com/m/v2/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"regexp"
	"strings"
	"sync"
	"time"
)

var PrimaryKeyIndex []int

// 初始化 Redis 客户端
func initRedis(config *conf.Config) (redis.UniversalClient, string, error) {
	switch config.Redis.Mode {
	case "standalone":
		return redis.NewClient(&redis.Options{
			Addr:     config.Redis.Standalone.Addr,
			Password: config.Redis.Standalone.Password,
			DB:       config.Redis.Standalone.DB,
		}), config.Redis.Mode + " " + config.Redis.Standalone.Addr, nil
	case "sentinel":
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    config.Redis.Sentinel.MasterName,
			SentinelAddrs: config.Redis.Sentinel.Addrs,
			Password:      config.Redis.Sentinel.Password,
			DB:            config.Redis.Sentinel.DB,
		}), config.Redis.Mode + " " + strings.Join(config.Redis.Sentinel.Addrs, ";"), nil
	case "cluster":
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    config.Redis.Cluster.Addrs,
			Password: config.Redis.Cluster.Password,
		}), config.Redis.Mode + " " + strings.Join(config.Redis.Cluster.Addrs, ";"), nil
	default:
		return nil, "", fmt.Errorf("不支持的 Redis 模式: %s", config.Redis.Mode)
	}
}

// 全量同步到redis
func DumpFullMySQLTableToRedis(db *sql.DB, conf *conf.Config, rdb *redis.UniversalClient,
	options *model.DaemonOptions, concurrencyLimit int) (*mysql.Position, error) {
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
			if options.MysqlSync.WriteMode == "batch" {
				err = dumpSingleTableDataBatchToRedis(tx, *rdb, mapping.Database, table.Table, options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table], table.Columns, options, options.MysqlSync.WriteBatchSize)
			} else {
				err = dumpSingleTableDataToRdis(tx, *rdb, mapping.Database, table.Table, options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table], table.Columns, options, concurrencyLimit)
			}
			if err != nil {
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

func dumpSingleTableDataToRdis(tx *sql.Tx, rdb redis.UniversalClient, schema string, table string,
	PKColNames []string, columns []string, options *model.DaemonOptions, conLimit int) error {
	columnList := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM %s.%s;", columnList, schema, table)

	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", schema, table, err)
	}
	defer rows.Close()

	dbColumns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names for %s.%s: %v", schema, table, err)
	}
	if len(dbColumns) != len(columns) {
		return fmt.Errorf("mismatch in requested and retrieved columns for %s.%s", schema, table)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, conLimit)

	for rows.Next() {
		// 存储当前行数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// 读取数据并创建独立拷贝
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan rows for %s.%s: %v", schema, table, err)
		}

		rowData := make(map[string]string)
		for i, col := range columns {
			rowData[col] = string(values[i])
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(rowData map[string]string) {
			defer wg.Done()
			defer func() { <-sem }()

			redisKey := generateRedisKey(table, scanArgs, columns, PKColNames)

			if err := rdb.HMSet(options.Ctx, redisKey, rowData).Err(); err != nil {
				log.Error().Err(err).Msgf("Failed to write to Redis: key=%s", redisKey)
			}
		}(rowData)
	}

	// 等待所有 Goroutine 完成
	wg.Wait()

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error for %s.%s: %v", schema, table, err)
	}

	return nil
}

func dumpSingleTableDataBatchToRedis(tx *sql.Tx, rdb redis.UniversalClient, schema string, table string,
	PKColNames []string, columns []string, options *model.DaemonOptions, batchSize int) error {
	columnList := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM %s.%s;", columnList, schema, table)

	// 执行查询
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", schema, table, err)
	}
	defer rows.Close()

	// 获取列名
	dbColumns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names for %s.%s: %v", schema, table, err)
	}
	if len(dbColumns) != len(columns) {
		return fmt.Errorf("mismatch in requested and retrieved columns for %s.%s", schema, table)
	}

	// 缓存批量写入的键值对
	batch := make(map[string]map[string]interface{})

	// 定义批量写入逻辑
	writeBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		pipe := rdb.Pipeline() // 创建 Pipeline
		for key, fields := range batch {
			pipe.HMSet(options.Ctx, key, fields)
		}

		if _, err := pipe.Exec(options.Ctx); err != nil {
			log.Error().Err(err).Msg("Failed to execute batch Redis writes")
			return err
		}

		//每个批量间间隔50毫秒
		time.Sleep(50 * time.Millisecond)
		log.Debug().Msgf("Successfully wrote %d keys to Redis", len(batch))
		batch = make(map[string]map[string]interface{}) // 清空缓存
		return nil
	}

	for rows.Next() {
		// 存储当前行数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// 读取数据并创建独立拷贝
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan rows for %s.%s: %v", schema, table, err)
		}

		// 将数据添加到批量缓存中
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = string(values[i])
		}

		redisKey := generateRedisKey(table, scanArgs, columns, PKColNames)
		batch[redisKey] = rowData

		// 如果达到批量大小，写入 Redis
		if len(batch) >= batchSize {
			if err := writeBatch(); err != nil {
				return err
			}
		}
	}

	// 写入剩余未达到批量大小的数据
	if err := writeBatch(); err != nil {
		return err
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error for %s.%s: %v", schema, table, err)
	}

	return nil
}

// 增量同步主逻辑
func syncBinlogToRedis(syncer *replication.BinlogSyncer, redisClient *redis.UniversalClient, position *mysql.Position,
	syncConf *conf.Config, options *model.DaemonOptions, db *sql.DB) error {
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
							processRowsEvent(redisClient, ev, syncConf, options)
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
func processRowsEvent(redisClient *redis.UniversalClient, event *replication.BinlogEvent, syncConf *conf.Config, options *model.DaemonOptions) {
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
		_ = handleInsertEvent(*redisClient, syncConf, rowsEvent, options)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		_ = handleUpdateEvent(*redisClient, syncConf, rowsEvent, options)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		_ = handleDeleteEvent(*redisClient, syncConf, rowsEvent, options)
	default:
		log.Debug().Msgf("未处理的事件类型: %s.%s - EventType: %v", eventDB, eventTable, event.Header.EventType)
	}
}

func handleInsertEvent(client redis.UniversalClient, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	var redisKey string
	// 遍历配置的映射关系
	for _, confMap := range syncConf.Mapping {
		if confMap.Database == string(e.Table.Schema) {
			for _, table := range confMap.Tables {
				if table.Table == string(e.Table.Table) {
					//data := make(map[string]interface{})
					eventDB := string(e.Table.Schema)
					eventTable := string(e.Table.Table)
					columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
					rowsBatch := make(map[string]map[string]interface{}) // 批量缓存

					for _, row := range e.Rows {
						redisKey = generateRedisKey(
							table.Table,
							row,
							table.Columns,
							options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table],
						)
						//log.Info().Msg(fmt.Sprintf("redisKey:%s", redisKey))

						// 构造当前行的数据
						data := make(map[string]interface{})
						for i, col := range columns {
							if i <= len(row)-1 { // 避免因为数据表加了字段，导致从元数据获得的列大于 binlog 中的列
								for _, selectColumn := range table.Columns {
									if strings.ToLower(col) == strings.ToLower(selectColumn) {
										data[col] = row[i]
									}
								}
							}
						}

						// 添加到批量缓存
						rowsBatch[redisKey] = data

						// 如果达到批量大小，写入 Redis
						if len(rowsBatch) >= options.MysqlSync.WriteBatchSize {
							if err := writeBatchToRedis(client, rowsBatch, options); err != nil {
								log.Error().Err(err).Msg("Failed to write batch to Redis")
							}
							rowsBatch = make(map[string]map[string]interface{}) // 清空缓存
						}
					}

					// 写入剩余数据
					if len(rowsBatch) > 0 {
						if err := writeBatchToRedis(client, rowsBatch, options); err != nil {
							log.Error().Err(err).Msg("Failed to write final batch to Redis")
						}
					}

				}
			}
		}
	}

	return nil
}

func handleUpdateEvent(client redis.UniversalClient, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	var redisKey string
	// 遍历配置的映射关系
	for _, confMap := range syncConf.Mapping {
		if confMap.Database == string(e.Table.Schema) {
			for _, table := range confMap.Tables {
				if table.Table == string(e.Table.Table) {
					//data := make(map[string]interface{})
					eventDB := string(e.Table.Schema)
					eventTable := string(e.Table.Table)
					columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
					/*
						for i := 0; i < len(e.Rows); i += 2 {
							_ = e.Rows[i]
							after := e.Rows[i+1]
							// 生成 Redis Key
							redisKey = generateRedisKey(table.Table, after, table.Columns, options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table])
							for i, col := range columns {
								for _, selectColumn := range table.Columns {
									if strings.ToLower(col) == strings.ToLower(selectColumn) {
										data[col] = after[i]
									}
								}
							}
							// 写入 Redis
							if err := client.HMSet(options.Ctx, redisKey, data).Err(); err != nil {
								log.Error().Err(err).Msg(fmt.Sprintf("Failed to write to Redis: key=%s", redisKey))
							}

							// 打印调试信息
							log.Debug().Str("key", redisKey).Interface("data", data).Msg("Debugging Redis HSet update")
						}
					*/

					rowsBatch := make(map[string]map[string]interface{}) // 批量缓存

					for i := 0; i < len(e.Rows); i += 2 {
						_ = e.Rows[i]
						after := e.Rows[i+1]
						redisKey = generateRedisKey(
							table.Table,
							after,
							table.Columns,
							options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table],
						)
						//log.Info().Msg(fmt.Sprintf("redisKey:%s", redisKey))

						// 构造当前行的数据
						data := make(map[string]interface{})
						for i, col := range columns {
							if i <= len(after)-1 { // 避免因为数据表加了字段，导致从元数据获得的列大于 binlog 中的列
								for _, selectColumn := range table.Columns {
									if strings.ToLower(col) == strings.ToLower(selectColumn) {
										data[col] = after[i]
									}
								}
							}
						}

						// 添加到批量缓存
						rowsBatch[redisKey] = data

						// 如果达到批量大小，写入 Redis
						if len(rowsBatch) >= options.MysqlSync.WriteBatchSize {
							if err := writeBatchToRedis(client, rowsBatch, options); err != nil {
								log.Error().Err(err).Msg("Failed to write batch to Redis")
							}
							rowsBatch = make(map[string]map[string]interface{}) // 清空缓存
						}
					}

					// 写入剩余数据
					if len(rowsBatch) > 0 {
						if err := writeBatchToRedis(client, rowsBatch, options); err != nil {
							log.Error().Err(err).Msg("Failed to write final batch to Redis")
						}
					}
				}
			}
		}
	}

	return nil
}

func handleDeleteEvent(client redis.UniversalClient, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	var redisKeys []string
	// 遍历配置的映射关系
	for _, confMap := range syncConf.Mapping {
		if confMap.Database == string(e.Table.Schema) {
			for _, table := range confMap.Tables {
				if table.Table == string(e.Table.Table) {
					for _, row := range e.Rows {
						redisKey := generateRedisKey(table.Table, row, table.Columns, options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table])
						redisKeys = append(redisKeys, redisKey)
					}
					re := client.Del(options.Ctx, redisKeys...)

					// 打印调试信息
					log.Info().Msg(fmt.Sprintf("batch delete result:%s", re.String()))
				}
			}
		}
	}

	return nil
}

// 构建rediskey
func generateRedisKey(table string, row []interface{}, columns []string, primaryKeyColumnNames []string) string {
	var keyParts []string
	keyParts = append(keyParts, table)
	//log.Info().Msg(fmt.Sprintf("row data: %v, columns: %v, pk name:%v", row, columns, primaryKeyColumnNames))

	// 使用主键列名找到对应的值，并构建 Redis 键
	for _, pk := range primaryKeyColumnNames {
		for i, col := range columns {
			//log.Info().Msg(fmt.Sprintf("debug: col-%v pk-%v", col, pk))
			if strings.ToLower(col) == strings.ToLower(pk) {
				value := row[i]
				switch v := value.(type) {
				case []byte:
					keyParts = append(keyParts, value.(string))
				case *sql.RawBytes:
					keyParts = append(keyParts, string(*v))
				case string:
					keyParts = append(keyParts, v)
				default:
					keyParts = append(keyParts, fmt.Sprintf("%v", v))
				}
			}
		}
	}
	return strings.Join(keyParts, ":")
}

func writeBatchToRedis(client redis.UniversalClient, batch map[string]map[string]interface{}, options *model.DaemonOptions) error {
	pipe := client.Pipeline()
	for key, fields := range batch {
		pipe.HMSet(options.Ctx, key, fields)
	}

	_, err := pipe.Exec(options.Ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute batch Redis writes")
		return err
	}

	log.Debug().Msgf("Successfully wrote %d keys to Redis", len(batch))
	return nil
}
