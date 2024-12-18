package filter

import (
	"bufio"
	"bytes"
	"errors"
	"example.com/m/v2/model"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"strings"
)

func FilterSQLByDatabasesBuffer(options *model.DaemonOptions) error {
	dumpFile := options.MysqlDumpFilter.InFile
	outFile := options.MysqlDumpFilter.OutFile
	dbTables := strings.Split(options.MysqlDumpFilter.TableList, ",")

	// 创建一个新的 DatabaseStruct 实例,并初始化
	dbStruct, err := NewDatabaseStructInit(dbTables)
	if err != nil {
		return err
	}

	// 获取 context
	ctx := options.Ctx
	if ctx == nil {
		return errors.New("context is nil in options")
	}

	var (
		targerFlag        bool
		headFlag          bool = true
		lastDB            string
		currentDB         string
		currentTable      string
		linePrefix        string
		finish            bool
		dbLevelFlag       bool
		currentDBString   string = strings.ToUpper("-- Current Database")
		tableStructString string = strings.ToUpper("-- Table structure for table ")
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
	maxCapacity := options.MysqlDumpFilter.BufferSize * 1024 * 1024
	outputFile, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %v", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriterSize(outputFile, maxCapacity)
	defer writer.Flush()

	// 创建 reader按块 来逐行读取 dump 文件
	bufReader := bufio.NewReader(gzReader)
	chunkSize := options.MysqlDumpFilter.BufferSize * 1024 * 1024
	buffer := make([]byte, chunkSize)

	var leftover []byte // 保存上一块未处理完的数据
	var line []byte
	i := 1
	for {
		// 检查取消信号ctrl+c
		select {
		case <-ctx.Done():
			log.Warn().Msg("Task canceled during file scanning")
			return ctx.Err()
		default:
			// 继续执行
		}

		n, err := bufReader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error().Err(err).Msgf("reader read data error")
		}

		// 拼接上次的剩余数据
		chunk := append(leftover, buffer[:n]...)

		// 按行分割
		lines := bytes.Split(chunk, []byte("\n"))

		// 保留最后一行（可能不完整）
		leftover = lines[len(lines)-1]
		lines = lines[:len(lines)-1]

		// 处理每一行
		for _, lineByte := range lines {
			//打印进度到日志
			if i%1000 == 0 {
				log.Info().Msgf("handle %d lines", i)
			}
			i++

			line = lineByte
			if len(lineByte) > options.MysqlDumpFilter.LinePrefixLongByte {
				lineByte = lineByte[:options.MysqlDumpFilter.LinePrefixLongByte-1]
			}
			linePrefix = strings.ToUpper(string(lineByte))
			// 根据每一行去进行匹配
			switch {
			//一个库的第一行
			case strings.HasPrefix(linePrefix, currentDBString):
				lastDB = currentDB
				targerFlag, currentDB = judgeDbExists(linePrefix, dbStruct)
				if currentDB != "" {
					log.Info().Msg(fmt.Sprintf("DB %s start :%v", currentDB, targerFlag))
				}
				headFlag = false
				dbLevelFlag = true

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
			case strings.HasPrefix(linePrefix, tableStructString):
				dbLevelFlag = false
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

					//更新完成状态
					dbStruct.databases[currentDB].tables[currentTable] = true
					tableInDbFinishFlag := true
					fmt.Printf("check all table process in dbStruct\n")
					for table := range dbStruct.databases[currentDB].tables {
						fmt.Printf("process: %s.%s:%v\n", currentDB, table, dbStruct.databases[currentDB].tables[table])
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

					currentTable = "" //防止下一个unlock tables 继续用
				} else {
					targerFlag = false
				}

			default:
				if headFlag { //头部
					targerFlag = true
					log.Debug().Msg(fmt.Sprintf("head line (%s.%s-%v)%s", currentDB, currentTable, targerFlag, linePrefix))
				} else { //最近匹配到的dbName和TableName不为空时，当前行有效
					if currentDB != "" && currentTable != "" || dbLevelFlag {
						targerFlag = true
					} else {
						targerFlag = false
					}
					log.Debug().Msg(fmt.Sprintf("default line (%s.%s-%v)%s", currentDB, currentTable, targerFlag, linePrefix))
				}
			}

			//输出符合条件的行
			if targerFlag {
				_, err := outputFile.Write(append(line, '\n'))
				if err != nil {
					return fmt.Errorf("无法写入输出文件: %v", err)
				}
			}
			if finish {
				log.Info().Msg("all filter database and tables are finish, stop to read the remain file.")
				return nil
			}
		}
	}

	log.Info().Msg(fmt.Sprintf("complete filter %s to %s database and tables:%s", dumpFile, outFile, dbTables))
	return nil
}
