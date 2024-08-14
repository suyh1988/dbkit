package sync

import (
	"context"
	"database/sql"
	"dbkit/command/binlogsql"
	"dbkit/conf"
	"dbkit/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
)

var ctx = context.Background()

func Run(options *model.DaemonOptions, _args []string) error {
	var (
		serverID   = options.MysqlSync.ServerID
		mysqlIP    = options.MysqlSync.SourceIP
		mysqlPort  = options.MysqlSync.SourcePort
		mysqlUser  = options.MysqlSync.SourceUser
		mysqlPwd   = options.MysqlSync.SourcePassWord
		syncMode   = options.MysqlSync.SyncMode   //increase :from the new pose ; full: dump data first, and read change from binlog pose
		targetType = options.MysqlSync.TargetType //sync data to redis ,redis default struct is hash
		dbName     = options.MysqlSync.DBName
		tbName     = options.MysqlSync.TableName // Added table name
		charset    = options.MysqlSync.CharSet
		binlogPos  = options.MysqlSync.BinlogPos

		targetIP   = options.MysqlSync.TargetIP
		targetPort = options.MysqlSync.TargetPort
		targetUser = options.MysqlSync.TargetUser
		targetPwd  = options.MysqlSync.TargetPassword
		targetDB   = options.MysqlSync.RedisDB
	)
	if options.MysqlSync.ConfigFile != "" {
		SyncConfig, err := conf.ReadConf(options.MysqlSync.ConfigFile)
		if err != nil {
			fmt.Printf("load configuration file faild, please recheck:%s", options.MysqlSync.ConfigFile)
		}
		serverID = SyncConfig.MySQLSync.ServerID
		mysqlIP = SyncConfig.MySQLSync.MySQLIP
		mysqlPort = SyncConfig.MySQLSync.MysqlPort
		mysqlUser = SyncConfig.MySQLSync.MysqlUser
		mysqlPwd = SyncConfig.MySQLSync.MysqlPassword
		syncMode = SyncConfig.MySQLSync.SyncMode     //increase :from the new pose ; full: dump data first, and read change from binlog pose
		targetType = SyncConfig.MySQLSync.TargetType //sync data to redis ,redis default struct is hash
		dbName = SyncConfig.MySQLSync.DbName
		tbName = SyncConfig.MySQLSync.TableName // Added table name
		charset = SyncConfig.MySQLSync.Charset
		binlogPos = SyncConfig.MySQLSync.BinlogPos

		targetIP = SyncConfig.Redis.IP
		targetPort = SyncConfig.Redis.Port
		targetUser = SyncConfig.Redis.User
		targetPwd = SyncConfig.Redis.Password
		targetDB = SyncConfig.Redis.DB

	}

	var db *sql.DB
	var err error
	var position *mysql.Position

	if mysqlIP != "" && mysqlPort != 0 && mysqlUser != "" && mysqlPwd != "" {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPwd, mysqlIP, mysqlPort, dbName)
		log.Info().Msg(dsn)
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Error().Err(err)
			return err
		}
		defer db.Close()
	}

	if targetType == "redis" {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     targetIP + ":" + targetPort,
			Username: targetUser,
			Password: targetPwd,
			DB:       targetDB,
		})
		defer redisClient.Close()

		cfg := replication.BinlogSyncerConfig{
			ServerID: uint32(serverID),
			Flavor:   "mysql",
			Host:     mysqlIP,
			Port:     uint16(mysqlPort),
			User:     mysqlUser,
			Password: mysqlPwd,
			Charset:  charset,
		}

		syncer := replication.NewBinlogSyncer(cfg)
		defer syncer.Close()

		if syncMode == "increase" {
			//增量同步如果提供的位点不合法就全量同步
			err, position = CheckBinlogPosOK(binlogPos, db)
			if err != nil {
				syncMode = "full"
			}
		}
		if syncMode == "full" {
			log.Info().Msg(fmt.Sprintf("dump mysql full data %s.%s to redis startting", dbName, tbName))
			position, err = DumpMySQLTableToRedis(db, dbName, tbName, redisClient)
			if err != nil {
				log.Error().Err(err).Msg("dump mysql data to redis failed")
				return err
			}
			log.Info().Msg(fmt.Sprintf("dump mysql full data %s.%s to redis success", dbName, tbName))
		}

		log.Info().Msg(fmt.Sprintf("sync begin at %s:%d to redis", position.Name, position.Pos))
		streamer, err := syncer.StartSync(*position)
		if err != nil {
			log.Error().Err(err)
			return err
		}

		var lastBinlogFilename, currentBinlogFilename string
		lastBinlogFilename = position.Name
		currentBinlogFilename = position.Name
		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				log.Error().Err(err)
				return err
			}

			// 获取当前事件的位点信息
			currentPos := ev.Header.LogPos
			err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, currentBinlogFilename+":"+strconv.Itoa(int(currentPos)))
			if err != nil {
				log.Info().Err(err).Msg(fmt.Sprintf("update binlog pos to %s failed", options.MysqlSync.ConfigFile))
			}

			switch e := ev.Event.(type) {
			case *replication.QueryEvent:
				// 检查是否是事务开始的 QueryEvent
				if strings.ToUpper(strings.TrimSpace(string(e.Query))) == "BEGIN" {
					// 忽略事务开始的 QueryEvent
					continue
				}
				// 如果是DDL语句，检查是否作用于指定的db和table
				if binlogsql.IsDDL(string(e.Query)) {
					ddlDB, ddlTable := binlogsql.ParseDDL(string(e.Query))
					if (dbName != "" && ddlDB != dbName) || (tbName != "" && ddlTable != tbName) {
						continue
					}
				}
				//fmt.Printf("[%s] %s\n", ev.Header.EventType, e.Query)
			case *replication.RowsEvent:
				eventDB := string(e.Table.Schema)
				eventTable := string(e.Table.Table)

				if dbName != "" && eventDB != dbName {
					continue
				}

				if tbName != "" && eventTable != tbName {
					continue
				}

				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					for _, row := range e.Rows {
						handleInsertEvent(ctx, redisClient, eventDB, eventTable, row, db, e)
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					for i := 0; i < len(e.Rows); i += 2 {
						handleUpdateEvent(redisClient, eventDB, eventTable, e.Rows[i+1], db, e)
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					for _, row := range e.Rows {
						handleDeleteEvent(redisClient, eventDB, eventTable, row, db)
					}
				}
			case *replication.RotateEvent:
				currentBinlogFilename = string(e.NextLogName)
				fmt.Printf("Rotate to %s, pos %d\n", e.NextLogName, e.Position)
			}

			// 如果未遇到 RotateEvent，可以使用上一次的 filename
			if currentBinlogFilename == "" {
				currentBinlogFilename = lastBinlogFilename
			}

			fmt.Printf("Current binlog filename: %s, position: %d\n", currentBinlogFilename, currentPos)

			// 更新上一次的文件名
			lastBinlogFilename = currentBinlogFilename
		}

	}

	return nil
}
