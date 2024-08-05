package sync

import (
	"dbkit/model"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

func BinlogActionFlag(options *model.DaemonOptions) []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:        "sourceIP",
			Value:       "",
			Usage:       "data source mysql ip",
			Destination: &options.MysqlSync.SourceIP,
		},
		cli.IntFlag{
			Name:        "sourcePort",
			Value:       3306,
			Usage:       "data source mysql port",
			Destination: &options.MysqlSync.SourcePort,
		},
		cli.StringFlag{
			Name:        "sourceUser",
			Value:       "",
			Usage:       "data source mysql user",
			Destination: &options.MysqlSync.SourceUser,
		},
		cli.StringFlag{
			Name:        "sourcePassword",
			Value:       "",
			Usage:       "data source mysql password",
			Destination: &options.MysqlSync.SourcePassWord,
		},
		cli.StringFlag{
			Name:        "targetType",
			Value:       "",
			Usage:       "sync data target database teype:mongo/redis/kafka/pika/ES",
			Destination: &options.MysqlSync.TargetType,
		},
		cli.StringFlag{
			Name:        "syncMode",
			Value:       "increase",
			Usage:       "sync mode: full/increase",
			Destination: &options.MysqlSync.DBName,
		},
		cli.StringFlag{
			Name:        "db",
			Value:       "",
			Usage:       "database of mysql to sync",
			Destination: &options.MysqlSync.DBName,
		},
		cli.StringFlag{
			Name:        "table",
			Value:       "",
			Usage:       "table of mysql to sync",
			Destination: &options.MysqlSync.TableName,
		},
		cli.StringFlag{
			Name:        "targetIP",
			Value:       "",
			Usage:       "target database IP",
			Destination: &options.MysqlSync.TargetIP,
		},

		cli.StringFlag{
			Name:        "targetPort",
			Value:       "",
			Usage:       "target database port",
			Destination: &options.MysqlSync.TargetPort,
		},

		cli.StringFlag{
			Name:        "targetUser",
			Value:       "",
			Usage:       "target database user",
			Destination: &options.MysqlSync.TargetUser,
		},
		cli.StringFlag{
			Name:        "targetPassword",
			Value:       "",
			Usage:       "target database password",
			Destination: &options.MysqlSync.TargetPassword,
		},

		cli.StringFlag{
			Name:        "charset",
			Value:       "utf8mb4",
			Usage:       "data source mysql charset",
			Destination: &options.MysqlSync.CharSet,
		},
		cli.IntFlag{
			Name:        "serverid",
			Value:       8818,
			Usage:       "mysql server id",
			Destination: &options.MysqlSync.ServerID,
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
