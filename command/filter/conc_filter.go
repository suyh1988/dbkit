package filter

import (
	"bufio"
	"bytes"
	"errors"
	"example.com/m/v2/model"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/mmap"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// 分块大小（适用于每个 goroutine 处理的块大小）
const chunkSize = 4 * 1024 * 1024 // 4MB

// 解压任务结构体
type decompressTask struct {
	data []byte
}

// 并行解压缩函数
func decompressGzipConcurrently(options *model.DaemonOptions) error {
	log.Info().Msg("gunzip handle start")
	tempFile := strings.Replace(options.MysqlDumpFilter.InFile, ".tar.gz", "", -1)

	// 获取 context
	ctx := options.Ctx
	if ctx == nil {
		return errors.New("context is nil in options")
	}

	// 打开 gzip 文件
	file, err := os.Open(options.MysqlDumpFilter.InFile)
	if err != nil {
		return fmt.Errorf("无法打开 gzip 文件: %v", err)
	}
	defer file.Close()

	// 初始化 gzip.Reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("无法创建 gzip 解压流: %v", err)
	}
	defer gzReader.Close()

	// 创建临时文件
	tmpFile, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("无法创建临时文件: %v", err)
	}
	defer tmpFile.Close()

	// 创建共享 writer（带缓冲区）
	writer := bufio.NewWriter(tmpFile)
	defer writer.Flush()

	// 使用通道分发任务
	taskCh := make(chan *decompressTask, options.MysqlDumpFilter.Concurrent*2)
	var wg sync.WaitGroup

	// 启动 worker
	var mu sync.Mutex
	for i := 0; i < options.MysqlDumpFilter.Concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				// 解压缩后写入文件
				mu.Lock()
				_, err := writer.Write(task.data)
				mu.Unlock()
				if err != nil {
					log.Error().Err(err).Msgf("写入临时文件失败: %v", err)
				}
			}
		}()
	}

	// 按块读取 gzip 文件并发送给 worker
	buffer := make([]byte, chunkSize)
	for {
		// 检查取消信号ctrl+c
		select {
		case <-ctx.Done():
			log.Warn().Msg("Task canceled during file scanning")
			return ctx.Err()
		default:
			// 继续执行
		}

		n, err := gzReader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取 gzip 文件失败: %v", err)
		}

		// 分发任务
		task := &decompressTask{
			data: append([]byte(nil), buffer[:n]...),
		}
		taskCh <- task
	}

	close(taskCh) // 关闭任务通道
	wg.Wait()     // 等待所有 worker 完成
	log.Info().Msg("gunzip handle complete")
	return nil
}

// 使用 mmap 对临时文件进行过滤
func filterFileUsingMmap(options *model.DaemonOptions) error {
	// 创建一个新的 DatabaseStruct 实例,并初始化
	dbStruct, err := NewDatabaseStructInit(strings.Split(options.MysqlDumpFilter.TableList, ","))
	if err != nil {
		return err
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
		tempFile          string = strings.Replace(options.MysqlDumpFilter.InFile, ".tar.gz", "", -1)
		outFile           string = options.MysqlDumpFilter.OutFile
	)

	// 获取 context
	ctx := options.Ctx
	if ctx == nil {
		return errors.New("context is nil in options")
	}

	// 创建输出文件
	maxCapacity := options.MysqlDumpFilter.BufferSize * 1024 * 1024
	outputFile, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %v", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriterSize(outputFile, maxCapacity)
	defer writer.Flush()

	// 使用 mmap 映射临时文件
	reader, err := mmap.Open(tempFile)
	if err != nil {
		return fmt.Errorf("无法 mmap 文件: %v", err)
	}
	defer reader.Close()

	// 将 mmap 的内容读取为字节切片
	var line, data []byte
	var lines [][]byte
	for {
		data = make([]byte, reader.Len())
		_, err = reader.ReadAt(data, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("读取 mmap 数据失败: %v", err)
		}
		lines = bytes.Split(data, []byte("\n"))
		//判断是否所有目标库表对象都已经解压出来了
		var Finish bool = true
		var DbName string
		for _, line := range lines {
			if bytes.Contains(line, []byte("-- Current Database")) {
				for db := range dbStruct.databases {
					if !bytes.Contains(line, []byte(db)) {
						Finish = false
						DbName = db
					}
				}
			}
			if DbName != "" {
				if bytes.Contains(line, []byte("-- Table structure for table ")) {
					for tb := range dbStruct.databases[DbName].tables {
						if !bytes.Contains(line, []byte(tb)) {
							Finish = false
						}
					}
				}
			}

		}
		if Finish {
			log.Info().Msg("all database and tables are decompressed.")
			break
		}
		time.Sleep(10 * time.Second)
	}

	// 按行分割
	lines = bytes.Split(data, []byte("\n"))

	for _, lineByte := range lines {
		// 检查取消信号ctrl+c
		select {
		case <-ctx.Done():
			log.Warn().Msg("Task canceled during file scanning")
			return ctx.Err()
		default:
			// 继续执行
		}

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

	return nil
}
