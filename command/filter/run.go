package filter

import (
	"errors"
	"example.com/m/v2/model"
	"github.com/rs/zerolog/log"
	"strings"
)

func Run(options *model.DaemonOptions, _args []string) error {
	//参数检查
	if options.MysqlDumpFilter.InFile == "" || options.MysqlDumpFilter.OutFile == "" {
		return errors.New("The input parameter is incorrect. You need to enter the '--in/--out' parameter")
	}
	log.Info().Msgf("start filter %s to %s database and tables:%s", options.MysqlDumpFilter.InFile, options.MysqlDumpFilter.OutFile, options.MysqlDumpFilter.TableList)

	// 创建一个新的 DatabaseStruct 实例
	dbStruct := NewDatabaseStruct()
	dbTables := strings.Split(options.MysqlDumpFilter.TableList, ",")
	for _, dbTableStrings := range dbTables {
		dbTable := strings.Split(dbTableStrings, ".")
		if len(dbTable) != 2 {
			return errors.New("the option schema input error")
		}

		// 添加数据库
		dbStruct.AddDatabase(dbTable[0])

		// 添加表,此时table有可能是*
		dbStruct.AddTable(dbTable[0], dbTable[1])
	}

	return FilterSQLByDatabasesSeq(options)

}
