package sync

import (
	"database/sql"
	"dbkit/conf"
	"dbkit/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"regexp"
	"strings"
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
					data := make(map[string]interface{})
					eventDB := string(e.Table.Schema)
					eventTable := string(e.Table.Table)
					columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
					for _, row := range e.Rows {
						redisKey = generateRedisKey(table.Table, row, table.Columns, options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table])
						log.Info().Msg(fmt.Sprintf("redisKey:%s", redisKey))
						for i, col := range columns {
							if i <= len(row)-1 { //避免因为数据表加了字段，导致从元数据获得的列大于binlog中的列
								for _, selectColumn := range table.Columns {
									if strings.ToLower(col) == strings.ToLower(selectColumn) {
										data[col] = row[i]
									}
								}
							}
						}
						// 写入 Redis
						if err := client.HMSet(options.Ctx, redisKey, data).Err(); err != nil {
							log.Error().Err(err).Msg(fmt.Sprintf("Failed to write to Redis: key=%s", redisKey))
						}

						// 打印调试信息
						log.Debug().Str("key", redisKey).Interface("data", data).Msg("Debugging Redis HSet input")
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
					data := make(map[string]interface{})
					eventDB := string(e.Table.Schema)
					eventTable := string(e.Table.Table)
					columns := options.MysqlSync.TableColumnMap[eventDB+"."+eventTable]
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

				}
			}
		}
	}

	return nil
}

func handleDeleteEvent(client redis.UniversalClient, syncConf *conf.Config, e *replication.RowsEvent, options *model.DaemonOptions) error {
	var redisKey string
	// 遍历配置的映射关系
	for _, confMap := range syncConf.Mapping {
		if confMap.Database == string(e.Table.Schema) {
			for _, table := range confMap.Tables {
				if table.Table == string(e.Table.Table) {
					data := make(map[string]interface{})

					for _, row := range e.Rows {
						redisKey = generateRedisKey(table.Table, row, table.Columns, options.MysqlSync.PrimaryKeyColumnNames[confMap.Database+"."+table.Table])
						client.Del(options.Ctx, redisKey)
					}

					// 打印调试信息
					log.Debug().Str("key", redisKey).Interface("data", data).Msg("Debugging Redis Del")
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
