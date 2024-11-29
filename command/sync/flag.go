package sync

import (
	"example.com/m/v2/model"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

func BinlogActionFlag(options *model.DaemonOptions) []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:        "conf",
			Value:       "",
			Usage:       "sync configuration file",
			Destination: &options.MysqlSync.ConfigFile,
		},
		cli.Int64Flag{
			Name:        "rewrite_event_interval",
			Value:       100,
			Usage:       "write position to configure file interval of event",
			Destination: &options.MysqlSync.WriteEventInterval,
		},
		cli.Int64Flag{
			Name:        "rewrite_time_interval",
			Value:       30,
			Usage:       "write position to configure file interval of time(second)",
			Destination: &options.MysqlSync.WriteTimeInterval,
		},
		cli.StringFlag{
			Name:        "redis_write_mode",
			Value:       "batch", // batch single
			Usage:       "write data to redis mode when full dump ",
			Destination: &options.MysqlSync.RedisWriteMode,
		},
		cli.IntFlag{
			Name:        "redis_write_batch_size",
			Value:       1000,
			Usage:       "write data to redis batch size when full dump",
			Destination: &options.MysqlSync.RedisWriteBatchSize,
		},
	}
}

func NewSyncCommand(options *model.DaemonOptions) cli.Command {
	return cli.Command{
		Name:  "sync",
		Usage: "mysql sync data to other database",
		Flags: BinlogActionFlag(options),
		Action: func(c *cli.Context) error {
			if options.Debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
			}
			// 执行实际操作
			if err := Run(options, c.Args()); err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("sync run failed!"))
				return err
			}
			return nil
		},
	}
}
