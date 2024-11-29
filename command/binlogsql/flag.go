package binlogsql

import (
<<<<<<< HEAD
	"example.com/m/v2/model"
=======
	"dbkit/model"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
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
<<<<<<< HEAD
			Usage:       "sql mode: flashback(restore sql); general(get binlog sql); stat(get binlog file statistics of write info)",
=======
			Usage:       "mysql binlog sql return mode:flashback/general; \nflashback:return restore sql; general:return general sql",
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
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
<<<<<<< HEAD
		cli.StringFlag{
			Name:        "output",
			Value:       "",
			Usage:       "sql output file",
			Destination: &options.BinlogSql.OutFile,
		},
		cli.StringFlag{
			Name:        "stopNever",
			Value:       "false",
			Usage:       "keep running when read all binlog files",
			Destination: &options.BinlogSql.StopNever,
		},
		cli.StringFlag{
			Name:        "ddl",
			Value:       "false",
			Usage:       "including ddl sql",
			Destination: &options.BinlogSql.DDL,
		},
		cli.StringFlag{
			Name:        "rotate",
			Value:       "false",
			Usage:       "show binlog file rotate event",
			Destination: &options.BinlogSql.RotateFlag,
		},
		cli.StringFlag{
			Name:        "binlogDir",
			Value:       "",
			Usage:       "binlog file dir",
			Destination: &options.BinlogSql.BinlogDir,
=======
		cli.IntFlag{
			Name:        "list",
			Value:       0,
			Usage:       "binlog start start time",
			Destination: &options.BinlogSql.List,
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
		},
	}
}

func NewBinlogSqlCommand(options *model.DaemonOptions) cli.Command {
	return cli.Command{
<<<<<<< HEAD
		Name:  "binlogsql",
		Usage: "get sql or flash back from binlog",
=======
		Name:  "binlogserver",
		Usage: "mysql binlog server",
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
		Flags: BinlogActionFlag(options),
		Action: func(c *cli.Context) error {
			if options.Debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
			}
			// 执行实际操作
			if err := Run(options, c.Args()); err != nil {
<<<<<<< HEAD
				log.Error().Err(err).Msg(fmt.Sprintf("binlogsql run failed!"))
=======
				log.Error().Err(err).Msg(fmt.Sprintf("binlogserver run failed!"))
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
				return err
			}
			return nil
		},
	}
}
