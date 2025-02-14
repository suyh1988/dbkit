package sync

import (
	"errors"
	"example.com/m/v2/command/binlogsql"
	"example.com/m/v2/conf"
	"example.com/m/v2/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

func Run(options *model.DaemonOptions, _args []string) error {
	var (
		SyncConfig *conf.Config
		err        error
	)

	options.MysqlSync.PrimaryKeyColumnNames = make(map[string][]string)
	options.MysqlSync.TableColumnMap = make(map[string][]string)

	// 检查配置文件是否存在，优先加载文件配置
	if options.MysqlSync.ConfigFile != "" {
		SyncConfig, err = conf.ReadConf(options.MysqlSync.ConfigFile)
		if err != nil {
			log.Error().Err(err).Msgf("加载配置文件失败: %s", options.MysqlSync.ConfigFile)
			return err
		}
		log.Info().Msg(fmt.Sprintf("read configure file success:%s", options.MysqlSync.ConfigFile))
	}

	// 初始化数据源 MySQL 连接
	db, err := initMySQL(SyncConfig)
	if err != nil {
		log.Error().Err(err).Msg("init mysql connection error")
		return err
	}
	defer db.Close()

	// 配置 BinlogSyncer
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(SyncConfig.Source.ServerID),
		Flavor:   "mysql",
		Host:     SyncConfig.Source.IP,
		Port:     uint16(SyncConfig.Source.Port),
		User:     SyncConfig.Source.User,
		Password: SyncConfig.Source.Password,
		Charset:  SyncConfig.Source.Charset,
		Logger:   &binlogsql.NoOpLogger{},
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	//获取同步数据表的表头,增量同步需要依赖原始表头
	err = FlushColumnNames(options, db, SyncConfig)
	if err != nil {
		log.Error().Err(err).Msg(fmt.Sprintf("init get all table column name failed."))
		return err
	}

	// 获取所有表的主键字段
	for _, mapping := range SyncConfig.Mapping {
		for _, table := range mapping.Tables {
			primaryKeyColumnNames, err := getPrimaryKeysColumnNames(db, mapping.Database, table.Table)
			if err != nil {
				return fmt.Errorf("failed to get primary keys for %s.%s: %v", mapping.Database, table.Table, err)
			}
			options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table] = primaryKeyColumnNames
		}
	}

	// 检查数据源同步模式
	var position *mysql.Position
	switch SyncConfig.Source.Mode {
	case "increase":
		err, position = CheckBinlogPosOK(SyncConfig.Source.Pos, db)
		if err != nil {
			log.Info().Msg("增量同步位点无效，切换为全量同步")
			SyncConfig.Source.Mode = "full"
		} else {
			log.Info().Msg(fmt.Sprintf("sync mode increase ,begin at %s", position.String()))
		}

	case "full":
		log.Info().Msg(fmt.Sprintf("sync mode:%s", SyncConfig.Source.Mode))
	default:
		msg := fmt.Sprintf("sync mode configrue error: %s", SyncConfig.Source.Mode)
		log.Error().Msg(msg)
		return errors.New(msg)
	}

	//目标端处理
	switch SyncConfig.Target.Type {
	case "redis":
		// 初始化目标客户端（Redis）
		redisClient, addrInfo, err := initRedis(SyncConfig)
		if err != nil {
			log.Error().Err(err).Msg("init redis client error")
			return err
		}
		defer redisClient.Close()

		if SyncConfig.Source.Mode == "full" {
			log.Info().Msg(fmt.Sprintf("开始全量同步 MySQL 数据到 Redis %s", addrInfo))
			//全量同步
			position, err = DumpFullMySQLTableToRedis(db, SyncConfig, &redisClient, options, 3)
			if err != nil {
				log.Error().Err(err).Msg("全量同步失败")
				return err
			}
			log.Info().Msg("全量同步成功")
			err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, fmt.Sprintf("%s:%s", position.Name, position.Pos))
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("save binlog to %s failed: %s", options.MysqlSync.ConfigFile, position.String()))
				return err
			}
			log.Error().Err(err).Msg(fmt.Sprintf("save binlog to %s success: %s", options.MysqlSync.ConfigFile, position.String()))
		}

		// 开始增量同步
		log.Info().Msg(fmt.Sprintf("增量同步到Redis %s, pos %s:%d", addrInfo, position.Name, position.Pos))
		return syncBinlogToRedis(syncer, &redisClient, position, SyncConfig, options, db)

	case "mongodb":
		// 初始化目标客户端（mongodb）
		mongoClient, err := InitMongoDB(SyncConfig)
		if err != nil {
			log.Error().Err(err).Msg("init mongo client error")
			return err
		}

		//获取数据表列名
		err = FlushColumnNames(options, db, SyncConfig)
		if err != nil {
			log.Error().Err(err).Msg("get table column Name error")
			return err
		}

		if SyncConfig.Source.Mode == "full" {
			log.Info().Msg(fmt.Sprintf("开始全量同步 MySQL 数据到 %s ", SyncConfig.Target.Type))
			//全量同步
			position, err = DumpFullMySQLTableToMongoDB(db, SyncConfig, mongoClient, options, 3)
			if err != nil {
				log.Error().Err(err).Msg("全量同步失败")
				return err
			}
			log.Info().Msg("全量同步成功")
			err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, fmt.Sprintf("%s:%s", position.Name, position.Pos))
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("save binlog to %s failed: %s", options.MysqlSync.ConfigFile, position.String()))
				return err
			}
			log.Info().Msg(fmt.Sprintf("save binlog to %s success: %s", options.MysqlSync.ConfigFile, position.String()))
		}

		// 开始增量同步
		log.Info().Msg(fmt.Sprintf("增量同步到MongoDB %s, pos %s:%d", SyncConfig.Target.MongoDB.URI, position.Name, position.Pos))
		return syncBinlogToMongoDB(syncer, mongoClient, position, SyncConfig, options, db)

	case "elasticsearch":
		return nil
	case "kafka":
		return nil
	default:
		return nil
	}
}
