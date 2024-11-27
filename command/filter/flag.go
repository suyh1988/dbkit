package filter

import (
	"example.com/m/v2/model"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

func FilterActionFlag(options *model.DaemonOptions) []cli.Flag {
	return []cli.Flag{

		cli.StringFlag{
			Name:        "in",
			Value:       "",
			Usage:       "filter input mysqldump file",
			Destination: &options.MysqlDumpFilter.InFile,
		},
		cli.StringFlag{
			Name:        "out",
			Value:       "",
			Usage:       "filtered mysqldump file",
			Destination: &options.MysqlDumpFilter.OutFile,
		},
		cli.StringFlag{
			Name:        "schema",
			Value:       "",
			Usage:       "需要过滤的表名,表明前面必须带上数据库名,支持导出多个库,或指定的库表,示例：db1.*,db1.table1,db2.table2",
			Destination: &options.MysqlDumpFilter.TableList,
		},
		cli.IntFlag{
			Name:        "buffer",
			Value:       10,
			Usage:       "read dump file line buffer",
			Destination: &options.MysqlDumpFilter.BufferSize,
		},
		cli.IntFlag{
			Name:        "linePrefixLongByte",
			Value:       100,
			Usage:       "the size of prefix when read line from mysqldump file",
			Destination: &options.MysqlDumpFilter.LinePrefixLongByte,
		},
	}
}

func NewFilterCommand(options *model.DaemonOptions) cli.Command {
	return cli.Command{
		Name:  "filter",
		Usage: "mysqldump file filter by database and table",
		Flags: FilterActionFlag(options),
		Action: func(c *cli.Context) error {
			if options.Debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
			}
			// 执行实际操作
			log.Info().Msg(fmt.Sprintf("filter tool %s start running...", options.ActionType))
			if err := Run(options, c.Args()); err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("%s run failed!", options.ActionType))
				return err
			}
			return nil
		},
	}
}
