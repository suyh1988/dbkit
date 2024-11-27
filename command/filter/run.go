package filter

import (
	"errors"
	"example.com/m/v2/model"
	"github.com/rs/zerolog/log"
)

func Run(options *model.DaemonOptions, _args []string) error {
	//参数检查
	if options.MysqlDumpFilter.InFile == "" || options.MysqlDumpFilter.OutFile == "" {
		return errors.New("The input parameter is incorrect. You need to enter the '--in/--out' parameter")
	}
	log.Info().Msgf("start filter %s to %s database and tables:%s", options.MysqlDumpFilter.InFile, options.MysqlDumpFilter.OutFile, options.MysqlDumpFilter.TableList)

	return FilterSQLByDatabasesSeq(options)

}
