package sync

import (
	"dbkit/command/binlogsql"
	"dbkit/conf"
	"dbkit/model"
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

	// 初始化数据源同步模式
	var position *mysql.Position
	if SyncConfig.Source.Mode == "increase" {
		err, position = CheckBinlogPosOK(SyncConfig.Source.Pos, db)
		if err != nil {
			log.Warn().Msg("增量同步位点无效，切换为全量同步")
			SyncConfig.Source.Mode = "full"
		}
	}

	//目标端处理
	switch SyncConfig.Target.Type {
	case "redis":
		// 初始化目标客户端（Redis）
		redisClient, err := initRedis(SyncConfig)
		if err != nil {
			return err
		}
		defer redisClient.Close()

		if SyncConfig.Source.Mode == "full" {
			log.Info().Msg("开始全量同步 MySQL 数据到 Redis")
			//全量同步
			position, err = DumpFullMySQLTableToRedis(db, SyncConfig, &redisClient, options, 3)
			if err != nil {
				log.Error().Err(err).Msg("全量同步失败")
				return err
			}
			log.Info().Msg("全量同步成功")
			err = conf.UpdateBinlogPos(options.MysqlSync.ConfigFile, position.String())
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("save binlog to %s failed: %s", options.MysqlSync.ConfigFile, position.String()))
				return err
			}
			log.Error().Err(err).Msg(fmt.Sprintf("save binlog to %s success: %s", options.MysqlSync.ConfigFile, position.String()))
		}

		// 开始增量同步
		log.Info().Msgf("增量同步开始，位点: %s:%d", position.Name, position.Pos)
		return syncBinlogToRedis(syncer, &redisClient, position, SyncConfig, options, db)

	case "mongodb":
		return nil
	case "elasticsearch":
		return nil
	case "kafka":
		return nil
	default:
		return nil
	}
}
