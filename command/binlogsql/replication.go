package binlogsql

import (
	"database/sql"
	"dbkit/model"
	"errors"
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
	Sqls       []string
}

func getBinlogFiles(db *sql.DB, optionBinlogDir string) ([]string, error) {
	var binlogFiles []string
	var err error
	if optionBinlogDir == "" {
		rows, err := db.Query("SHOW BINARY LOGS")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var logName string
			var size int64
			if err := rows.Scan(&logName, &size); err != nil {
				return nil, err
			}
			binlogFiles = append(binlogFiles, logName)
		}
	} else {
		binlogFiles, err = GetFileNameByDir(optionBinlogDir)
		if err != nil {
			return []string{""}, err
		}
	}

	return binlogFiles, nil
}

func analyzeBinlogFile(fileName string, binlogDir string, parser *replication.BinlogParser, db *sql.DB, options *model.DaemonOptions) (*BinlogInfo, error) {
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
			if options.BinlogSql.DDL != "false" {
				// 使用 QueryEvent 时间更新 binlog 的开始时间
				if firstEvent {
					binlogInfo.StartTime = time.Unix(int64(ev.Header.Timestamp), 0)
					firstEvent = false
				}
				binlogInfo.EndTime = time.Unix(int64(ev.Header.Timestamp), 0)
				// 解析 SQL 语句
				sqlStr := strings.ReplaceAll(strings.ToUpper(string(e.Query)), "`", "")
				ddlRegex := regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|RENAME|TRUNCATE|ADD|INDEX)\s+(TABLE\s+)?(?:\w+\.)?(?P<table>\w+)`)
				match := ddlRegex.FindStringSubmatch(sqlStr)
				if len(match) > 0 {
					tableName, err := extractTableName(strings.ToUpper(fmt.Sprintf("%v", e.Query)))
					if err != nil {
						log.Info().Err(err).Msg("get table name failed")
					}

					dbTable := fmt.Sprintf("%s.%s", strings.ToLower(string(e.Schema)), strings.ToLower(fmt.Sprintf("%v", tableName)))
					binlogInfo.DbTableMap[dbTable] = struct{}{}

					sqlStr := fmt.Sprintf("/*%s:%d, Executed At: %s*/\n%s;", fileName, ev.Header.LogPos, time.Unix(int64(ev.Header.Timestamp), 0).Format("2006-01-02 15:04:05"), sqlStr)
					binlogInfo.Sqls = append(binlogInfo.Sqls, sqlStr)
				}
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
			transactionID := ev.Header.LogPos
			eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
			sql, err := generateSQL(db, ev.Header.EventType, e, options.BinlogSql.Mode, transactionID, eventTime, fileName)
			if err != nil {
				fmt.Printf("parse mysql 5.5 binlog error\n")
			} else {
				binlogInfo.Sqls = append(binlogInfo.Sqls, sql)
			}

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
		// DML Statements
		`(?i)INSERT\s+INTO\s+([^\s\(\)]+)`, // INSERT INTO table_name
		`(?i)UPDATE\s+([^\s]+)`,            // UPDATE table_name
		`(?i)DELETE\s+FROM\s+([^\s]+)`,     // DELETE FROM table_name

		// DDL Statements
		`(?i)CREATE\s+TABLE\s+([^\s\(\)]+)`, // CREATE TABLE table_name
		`(?i)ALTER\s+TABLE\s+([^\s]+)`,      // ALTER TABLE table_name
		`(?i)DROP\s+TABLE\s+([^\s]+)`,       // DROP TABLE table_name
		`(?i)TRUNCATE\s+TABLE\s+([^\s]+)`,   // TRUNCATE TABLE table_name

		// Index-related DDL Statements
		`(?i)CREATE\s+INDEX\s+([^\s]+)\s+ON\s+([^\s\(\)]+)`, // CREATE INDEX index_name ON table_name
		`(?i)DROP\s+INDEX\s+([^\s]+)\s+ON\s+([^\s]+)`,       // DROP INDEX index_name ON table_name

		// Schema and Database DDL
		`(?i)CREATE\s+DATABASE\s+([^\s]+)`, // CREATE DATABASE db_name
		`(?i)DROP\s+DATABASE\s+([^\s]+)`,   // DROP DATABASE db_name
	}
	log.Info().Msg(sql)

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			return matches[1], nil
		}
	}

	return "", fmt.Errorf("could not extract table name from SQL: %s", sql)
}

func GetBinlogInfo(db *sql.DB, optionBinlogDir string, options *model.DaemonOptions) error {
	var (
		err       error
		binlogDir string
	)

	if optionBinlogDir == "" {
		binlogDir, err = getBinlogDirectory(db)
		if err != nil {
			fmt.Printf("please check the mysql version, some version can not get binlog file with mysql dsn, please give the binglog directory by --binlogDir\n")
			return err
		}
	} else {
		binlogDir = optionBinlogDir
	}

	// 获取所有的 binlog 文件
	binlogFiles, err := getBinlogFiles(db, optionBinlogDir)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	// 初始化 binlog 解析器
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	fmt.Printf("| binlog file name | start time | end time | (tables included file)\n")
	for _, binlogFile := range binlogFiles {
		binlogInfo, err := analyzeBinlogFile(binlogFile, binlogDir, parser, db, options)
		if err != nil {
			log.Printf("Error analyzing binlog file %s: %v", binlogFile, err)
			continue
		}

		// 打印 binlog 文件的信息
		fmt.Printf("| %s | %s | %s |\n", binlogInfo.Name, binlogInfo.StartTime.Format("2006-01-02 15:04:05.000"), binlogInfo.EndTime.Format("2006-01-02 15:04:05.000"))
		fmt.Printf("----------------------------------------------------------------------\n")
		for dbTable := range binlogInfo.DbTableMap {
			fmt.Printf("\t%s\n", dbTable)
		}
		fmt.Printf("----------------------------------------------------------------------\n")
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
	if len(logBinBasename) > 0 {
		binlogDir := logBinBasename[:strings.LastIndex(logBinBasename, "/")]
		return binlogDir, nil
	} else {
		return "", errors.New("binlog directory is not exists")
	}

}

func GetBinlogSql(db *sql.DB, binlogFile string, options *model.DaemonOptions) error {
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	binlogInfo, err := analyzeBinlogFile(binlogFile, options.BinlogSql.BinlogDir, parser, db, options)
	if err != nil {
		log.Printf("Error analyzing binlog file %s: %v", binlogFile, err)
		return err
	}

	// 打印 或输出 解析到的sql
	for _, sql := range binlogInfo.Sqls {
		if options.BinlogSql.OutFile == "" {
			fmt.Printf("%s\n", sql)
		} else {
			err := AppendToFile(options.BinlogSql.OutFile, sql)
			if err != nil {
				log.Error().Err(err).Msg("append SQL to output file failed")
			}
		}
	}

	return nil
}
