package filter

import (
	"bufio"
	"compress/gzip"
	"example.com/m/v2/model"
	"regexp"

	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
)

func FilterSQLByDatabasesSeq(options *model.DaemonOptions) error {
	dumpFile := options.MysqlDumpFilter.InFile
	outFile := options.MysqlDumpFilter.OutFile
	dbTables := strings.Split(options.MysqlDumpFilter.TableList, ",")

	// 创建一个新的 DatabaseStruct 实例
	dbStruct := NewDatabaseStruct()

	for _, dbTableStrings := range dbTables {
		dbTable := strings.Split(dbTableStrings, ".")
		if len(dbTable) != 2 {
			return errors.New("the option schema input error")
		}

		// 添加数据库
		dbStruct.AddDatabase(dbTable[0])

		// 添加表
		dbStruct.AddTable(dbTable[0], dbTable[1])

	}

	// 获取 context
	ctx := options.Ctx
	if ctx == nil {
		return errors.New("context is nil in options")
	}

	var (
		targerFlag   bool
		headFlag     bool = true
		lastDB       string
		currentDB    string
		currentTable string
		linePrefix   string
		finish       bool
	)

	// 打开 mysqldump 文件
	file, err := os.Open(dumpFile)
	if err != nil {
		return fmt.Errorf("无法打开 mysqldump 文件: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	// 创建输出文件
	outputFile, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %v", err)
	}
	defer outputFile.Close()

	// 创建 scanner 来逐行读取 dump 文件
	scanner := bufio.NewScanner(gzReader)
	maxCapacity := options.MysqlDumpFilter.BufferSize * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	writer := bufio.NewWriterSize(outputFile, maxCapacity)
	defer writer.Flush()

	i := 0
	// 遍历每一行
	for scanner.Scan() {

		// 检查取消信号ctrl+c
		select {
		case <-ctx.Done():
			log.Warn().Msg("Task canceled during file scanning")
			return ctx.Err()
		default:
			// 继续执行
		}

		//打印进度到日志
		if i%1000 == 0 {
			log.Info().Msgf("handle %d lines", i)
		}
		i++

		line := scanner.Text()

		//前缀,提升匹配的效率
		if len(line) > options.MysqlDumpFilter.LinePrefixLongByte {
			linePrefix = strings.ToUpper(line[0 : options.MysqlDumpFilter.LinePrefixLongByte-1])
		} else {
			linePrefix = strings.ToUpper(line)
		}

		// 根据每一行去进行匹配
		switch {
		//一个库的第一行
		case strings.HasPrefix(linePrefix, strings.ToUpper("-- Current Database")):
			lastDB = currentDB
			targerFlag, currentDB = judgeDbExists(linePrefix, dbStruct)
			if currentDB != "" {
				log.Info().Msg(fmt.Sprintf("DB %s start :%v", currentDB, targerFlag))
			}
			headFlag = false

			//完成一个库
			if lastDB != "" {
				dbStruct.databases[lastDB].finish = true
				log.Info().Msg(fmt.Sprintf("DB %s end :%v", lastDB, targerFlag))
			}

			//判断是否全部库都完成了
			if filterFinish(dbStruct) {
				log.Info().Msg(fmt.Sprintf("filter finish"))
				finish = true
			}

		//一个表的第一行
		case strings.HasPrefix(linePrefix, strings.ToUpper("-- Table structure for table ")):
			if currentDB == "" {
				currentTable = ""
				continue
			}
			targerFlag, currentTable = judgeDbTableExists(linePrefix, currentDB, dbStruct)
			if currentTable != "" {
				log.Info().Msg(fmt.Sprintf("table %s.%s start:%v", currentDB, currentTable, targerFlag))
			}

		// 匹配到的表的最后一行/也可能时库的最后一行
		case strings.Contains(linePrefix, "UNLOCK TABLES"):
			if currentTable != "" {
				targerFlag = true
				log.Info().Msg(fmt.Sprintf("table %s.%s end :%v", currentDB, currentTable, targerFlag))
				currentTable = "" //防止下一个unlock tables 继续用

				//更新完成状态
				dbStruct.databases[currentDB].tables[currentTable] = true
				tableInDbFinishFlag := true
				for table := range dbStruct.databases[currentDB].tables {
					if !dbStruct.databases[currentDB].tables[table] {
						tableInDbFinishFlag = false
						break
					}
				}
				if tableInDbFinishFlag {
					log.Info().Msg(fmt.Sprintf("all table in %s are filtered.", currentDB))
					dbStruct.databases[currentDB].finish = true
				}

				//判断是否全部库都完成了
				if filterFinish(dbStruct) {
					log.Info().Msg(fmt.Sprintf("filter finish"))
					finish = true
				}

			} else {
				targerFlag = false
			}

		default:
			if headFlag { //头部
				targerFlag = true
				log.Debug().Msg(fmt.Sprintf("head line (%s.%s-%v)%s", currentDB, currentTable, targerFlag, linePrefix))
			} else { //最近匹配到的dbName和TableName不为空时，当前行有效
				if currentDB != "" && currentTable != "" {
					targerFlag = true
				} else {
					targerFlag = false
				}
				log.Debug().Msg(fmt.Sprintf("default line (%s.%s-%v)%s", currentDB, currentTable, targerFlag, linePrefix))
			}
		}

		//输出符合条件的行
		if targerFlag {
			_, err := outputFile.WriteString(line + "\n")
			if err != nil {
				return fmt.Errorf("无法写入输出文件: %v", err)
			}
		}
		if finish {
			log.Info().Msg("all filter database and tables are finish, stop to read the remain file.")
			return nil
		}
	}

	log.Info().Msg(fmt.Sprintf("complete filter %s to %s database and tables:%s", dumpFile, outFile, dbTables))
	return nil
}

func judgeDbExists(linePrefix string, dbStruct *DatabaseStruct) (ifExist bool, currentDB string) {
	ifExist = false
	currentDB = ""
	for db := range dbStruct.databases {
		// 构造一个匹配完整单词的正则表达式
		pattern := fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(db))
		matched, err := regexp.MatchString(pattern, linePrefix)
		if err != nil {
			continue // 如果正则匹配出错，跳过当前数据库
		}
		if matched {
			currentDB = db
			ifExist = true
			break
		}
	}
	return ifExist, currentDB
}

func judgeDbTableExists(linePrefix, currentDB string, dbStruct *DatabaseStruct) (ifExist bool, currentTable string) {
	ifExist = false
	currentTable = ""

	for table := range dbStruct.databases[currentDB].tables {
		// 构造一个匹配完整单词的正则表达式
		pattern := fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(table))
		matched, err := regexp.MatchString(pattern, linePrefix)
		if err != nil {
			continue // 如果正则匹配出错，跳过当前数据库
		}
		if matched {
			currentTable = table
			ifExist = true
			break
		}
	}
	return ifExist, currentTable
}

func filterFinish(dbStruct *DatabaseStruct) bool {
	//判断是否全部库都完成了
	complete := true
	for db := range dbStruct.databases {
		if !dbStruct.databases[db].finish {
			complete = false
		}
	}
	if complete {
		return true
	} else {
		return false
	}
}
