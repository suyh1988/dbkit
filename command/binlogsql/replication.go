package binlogsql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

type BinlogInfo struct {
	Name       string
	StartTime  time.Time
	EndTime    time.Time
	DbTableMap map[string]struct{}
}

func getBinlogFiles(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var binlogFiles []string
	for rows.Next() {
		var logName string
		var size int64
		if err := rows.Scan(&logName, &size); err != nil {
			return nil, err
		}
		binlogFiles = append(binlogFiles, logName)
	}
	return binlogFiles, nil
}

func analyzeBinlogFile(fileName string, binlogDir string, parser *replication.BinlogParser) (*BinlogInfo, error) {
	binlogInfo := &BinlogInfo{
		Name:       fileName,
		DbTableMap: make(map[string]struct{}),
	}

	var firstEvent bool = true

	// 处理事件的回调函数
	onEvent := func(ev *replication.BinlogEvent) error {
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			// 忽略 RotateEvent
			return nil
		case *replication.FormatDescriptionEvent:
			// 忽略 FormatDescriptionEvent
			return nil
		case *replication.QueryEvent:
			// 使用 QueryEvent 时间更新 binlog 的开始时间
			if firstEvent {
				binlogInfo.StartTime = time.Unix(int64(ev.Header.Timestamp), 0)
				firstEvent = false
			}
			binlogInfo.EndTime = time.Unix(int64(ev.Header.Timestamp), 0)
			// 解析 SQL 语句
			sql := strings.ReplaceAll(strings.ToUpper(string(e.Query)), "`", "")
			if strings.HasPrefix(sql, "CREATE TABLE") || strings.HasPrefix(sql, "DROP TABLE") || strings.HasPrefix(sql, "ALTER TABLE") {
				tableName, err := extractTableName(sql)
				if err != nil {
					log.Info().Err(err).Msg("get table name failed")
				}
				dbTable := fmt.Sprintf("%s.%s", strings.ToLower(string(e.Schema)), strings.ToLower(tableName))
				binlogInfo.DbTableMap[dbTable] = struct{}{}
			}
		case *replication.RowsEvent:
			// 使用 RowsEvent 时间更新 binlog 的开始和结束时间
			if firstEvent {
				binlogInfo.StartTime = time.Unix(int64(ev.Header.Timestamp), 0)
				firstEvent = false
			}
			binlogInfo.EndTime = time.Unix(int64(ev.Header.Timestamp), 0)

			dbTable := fmt.Sprintf("%s.%s", strings.ToLower(string(e.Table.Schema)), strings.ToLower(string(e.Table.Table)))
			binlogInfo.DbTableMap[dbTable] = struct{}{}
		default:

		}
		return nil
	}

	// 解析 binlog 文件
	err := parser.ParseFile(binlogDir+"/"+fileName, 0, onEvent)
	if err != nil {
		return nil, err
	}

	return binlogInfo, nil
}

func extractTableName(sql string) (string, error) {
	// 定义正则表达式，用于匹配不同类型的SQL语句中的表名
	patterns := []string{
		`(?i)INSERT\s+INTO\s+([^\s\(\)]+)`,  // INSERT INTO table_name
		`(?i)UPDATE\s+([^\s]+)`,             // UPDATE table_name
		`(?i)DELETE\s+FROM\s+([^\s]+)`,      // DELETE FROM table_name
		`(?i)CREATE\s+TABLE\s+([^\s\(\)]+)`, // CREATE TABLE table_name
		`(?i)ALTER\s+TABLE\s+([^\s]+)`,      // ALTER TABLE table_name
		`(?i)DROP\s+TABLE\s+([^\s]+)`,       // DROP TABLE table_name
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			return matches[1], nil
		}
	}

	return "", fmt.Errorf("could not extract table name from SQL: %s", sql)
}

func GetBinlogInfo(db *sql.DB) error {
	binlogDir, err := getBinlogDirectory(db)
	if err != nil {
		return err
	}
	// 获取所有的 binlog 文件
	binlogFiles, err := getBinlogFiles(db)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	// 初始化 binlog 解析器
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)

	for _, binlogFile := range binlogFiles {
		binlogInfo, err := analyzeBinlogFile(binlogFile, binlogDir, parser)
		if err != nil {
			log.Printf("Error analyzing binlog file %s: %v", binlogFile, err)
			continue
		}

		// 打印 binlog 文件的信息
		fmt.Println()
		fmt.Printf("%s %s %s:\n", binlogInfo.Name, binlogInfo.StartTime.Format("2006-01-02 15:04:05.000"), binlogInfo.EndTime.Format("2006-01-02 15:04:05.000"))
		fmt.Printf("-------------------------------------------------------------------------------------------------------------------------\n")
		for dbTable := range binlogInfo.DbTableMap {
			fmt.Printf("\t%s\n", dbTable)
		}
		fmt.Printf("-------------------------------------------------------------------------------------------------------------------------\n")
		fmt.Println()

	}
	return nil
}

func getBinlogDirectory(db *sql.DB) (string, error) {
	var Variable_name, logBinBasename string

	// 执行查询获取 binlog 的基础路径
	rows, err := db.Query("SHOW VARIABLES LIKE 'log_bin_basename';")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&Variable_name, &logBinBasename); err != nil {
			return "", err
		}
	}

	// 从路径中获取目录部分
	binlogDir := logBinBasename[:strings.LastIndex(logBinBasename, "/")]

	return binlogDir, nil
}
