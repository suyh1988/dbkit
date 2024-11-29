package sync

import (
<<<<<<< HEAD
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
	"time"
=======
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
)

var PrimaryKeyIndex []int

<<<<<<< HEAD
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

// 增量同步主逻辑
func syncBinlogToRedis(syncer *replication.BinlogSyncer, redisClient *redis.UniversalClient, position *mysql.Position, syncConf *conf.Config, options *model.DaemonOptions, db *sql.DB) error {
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
						if len(rowsBatch) >= options.MysqlSync.RedisWriteBatchSize {
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
						if len(rowsBatch) >= options.MysqlSync.RedisWriteBatchSize {
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
=======
func handleInsertEvent(ctx context.Context, client *redis.Client, schema, table string, row []interface{}, db *sql.DB, e *replication.RowsEvent) {
	var key string

	key = generateRedisKey(schema, table, row, db)
	data := generateRedisData(row, db, e)

	log.Info().Str("key", key).Interface("data", data).Msg("Debugging Redis HSet input")
	// 调用 HSet 插入数据到 Redis
	_, err := client.HMSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to insert data into Redis")
	}
}

func handleUpdateEvent(client *redis.Client, schema, table string, newRow []interface{}, db *sql.DB, e *replication.RowsEvent) {
	var key string

	key = generateRedisKey(schema, table, newRow, db)

	log.Info().Msg(fmt.Sprintf("key: %s", key))
	data := generateRedisData(newRow, db, e)
	_, err := client.HMSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to update data in Redis")
	}
}

func handleDeleteEvent(client *redis.Client, schema, table string, row []interface{}, db *sql.DB) {
	var key string

	key = generateRedisKey(schema, table, row, db)

	_, err := client.Del(ctx, key).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete data from Redis")
	}
}

func generateRedisKey(schema, table string, row []interface{}, db *sql.DB) string {
	var primaryKeys []string

	primaryKeyIndex, err := getPrimaryKeyIndex(db, schema, table)
	if err == nil {
		for _, idx := range primaryKeyIndex {
			fmt.Printf("suyh: primary key %s \n", strconv.FormatInt(int64(row[idx].(int32)), 10))
			primaryKeys = append(primaryKeys, strconv.FormatInt(int64(row[idx].(int32)), 10))
		}
	} else {
		log.Error().Err(err).Msg(fmt.Sprintf("get %s.%s not exist primary, sync data to redis must have a mysql primary key", schema, table))
		return ""
	}
	primaryKeyStr := strings.Join(primaryKeys, ":")
	log.Info().Msg(primaryKeyStr)
	return fmt.Sprintf("%s:%s", table, primaryKeyStr)
}

func generateRedisData(row []interface{}, db *sql.DB, e *replication.RowsEvent) map[string]interface{} {
	// Generate the data map from the row
	eventDB := string(e.Table.Schema)
	eventTable := string(e.Table.Table)
	data := make(map[string]interface{})
	columns, _ := getColumnNames(db, eventDB, eventTable)
	for i, column := range columns {
		data[column] = row[i]
	}
	return data
}

func getColumnNames(db *sql.DB, schema, table string) ([]string, error) {
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION;"
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, columnName)
	}
	return columnNames, nil
}

func getPrimaryKeyIndex(db *sql.DB, schema, table string) ([]int, error) {
	if len(PrimaryKeyIndex) == 0 {
		//log.Info().Msg(fmt.Sprintf("Init %s.%s primary key index", schema, table))
		query := "SELECT B.ORDINAL_POSITION-1 AS Idx FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN INFORMATION_SCHEMA.COLUMNS B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME WHERE A.TABLE_SCHEMA = ? AND A.TABLE_NAME = ? AND A.CONSTRAINT_NAME = 'PRIMARY' ORDER BY B.ORDINAL_POSITION;"
		rows, err := db.Query(query, schema, table)
		if err != nil {
			log.Error().Err(err).Msg("get table primary key index failed")
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var columnName int
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
			log.Info().Msg(fmt.Sprintf("primary key index:%d\n", columnName))
			PrimaryKeyIndex = append(PrimaryKeyIndex, columnName)
		}
	}
	log.Info().Msg(fmt.Sprintf("primary keys length %d\n", len(PrimaryKeyIndex)))
	return PrimaryKeyIndex, nil
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
}
