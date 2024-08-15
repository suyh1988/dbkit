package binlogsql

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
			Name:        "ip",
			Value:       "",
			Usage:       ".",
			Destination: &options.BinlogSql.IP,
		},
		cli.IntFlag{
			Name:        "port",
			Value:       0,
			Usage:       "hdfs cluster e.g sz/wx.",
			Destination: &options.BinlogSql.Port,
		},
		cli.StringFlag{
			Name:        "user",
			Value:       "",
			Usage:       "master user name",
			Destination: &options.BinlogSql.User,
		},
		cli.StringFlag{
			Name:        "password",
			Value:       "",
			Usage:       "master user password",
			Destination: &options.BinlogSql.PassWord,
		},
		cli.StringFlag{
			Name:        "db",
			Value:       "",
			Usage:       "master database name",
			Destination: &options.BinlogSql.DBName,
		},
		cli.StringFlag{
			Name:        "table",
			Value:       "",
			Usage:       "master table name",
			Destination: &options.BinlogSql.TableName,
		},
		cli.StringFlag{
			Name:        "mode",
			Value:       "general",
			Usage:       "mysql binlog sql return mode:flashback/general; \nflashback:return restore sql; general:return general sql",
			Destination: &options.BinlogSql.Mode,
		},
		cli.IntFlag{
			Name:        "serverid",
			Value:       8818,
			Usage:       "mysql server id",
			Destination: &options.BinlogSql.ServerID,
		},

		cli.StringFlag{
			Name:        "charset",
			Value:       "utf8mb4",
			Usage:       "mysql charset",
			Destination: &options.BinlogSql.CharSet,
		},
		cli.StringFlag{
			Name:        "startFile",
			Value:       "",
			Usage:       "",
			Destination: &options.BinlogSql.StartFile,
		},
		cli.StringFlag{
			Name:        "stopFile",
			Value:       "",
			Usage:       "",
			Destination: &options.BinlogSql.StopFile,
		},
		cli.IntFlag{
			Name:        "startPose",
			Value:       0,
			Usage:       "binlog start pose",
			Destination: &options.BinlogSql.StartPose,
		},
		cli.IntFlag{
			Name:        "stopPose",
			Value:       0,
			Usage:       "binlog start pose",
			Destination: &options.BinlogSql.StopPose,
		},
		cli.StringFlag{
			Name:        "startTime",
			Value:       "",
			Usage:       "binlog start start time",
			Destination: &options.BinlogSql.StartTime,
		},
		cli.StringFlag{
			Name:        "stopTime",
			Value:       "",
			Usage:       "binlog start start time",
			Destination: &options.BinlogSql.StopTime,
		},
		cli.IntFlag{
			Name:        "list",
			Value:       0,
			Usage:       "binlog start start time",
			Destination: &options.BinlogSql.List,
		},
	}
}

func NewBinlogSqlCommand(options *model.DaemonOptions) cli.Command {
	return cli.Command{
		Name:  "binlogserver",
		Usage: "mysql binlog server",
		Flags: BinlogActionFlag(options),
		Action: func(c *cli.Context) error {
			if options.Debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
			}
			// 执行实际操作
			if err := Run(options, c.Args()); err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("binlogserver run failed!"))
				return err
			}
			return nil
		},
	}
}
