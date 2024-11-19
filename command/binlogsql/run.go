package binlogsql

import (
	"context"
	"database/sql"
	"dbkit/common"
	"dbkit/model"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"os"
	"regexp"
	"strings"
	"time"
)

type Column struct {
	Name string // 列名
	Type string // 数据类型
}

type TableSchema struct {
	DbName    string
	TableName string
	Columns   []Column
}

func Run(options *model.DaemonOptions, _args []string) error {
	var (
		serverID  = options.BinlogSql.ServerID
		host      = options.BinlogSql.IP
		port      = options.BinlogSql.Port
		user      = options.BinlogSql.User
		password  = options.BinlogSql.PassWord
		charset   = options.BinlogSql.CharSet
		dbName    = options.BinlogSql.DBName
		startFile = options.BinlogSql.StartFile
		startPose = options.BinlogSql.StartPose
		outFile   = options.BinlogSql.OutFile
	)

	var db *sql.DB
	var err error
	var version string
	var ctx context.Context
	var cancel context.CancelFunc

	if options.BinlogSql.StopNever == "false" || options.BinlogSql.StopNever == "0" {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	} else {
		ctx = context.Background()
	}

	defer cancel() // 确保在函数结束时释放资源

	//输入参数检查
	if host != "" && port != 0 && user != "" && password != "" {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName)
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Error().Err(err).Msg(fmt.Sprintf("connection to mysql '%s' failed ", dsn))
			return err
		}
		version, err = model.GetMysqlVersion(db)
		if err != nil {
			fmt.Printf("get mysql version error:%v\n", err)
		}

		defer db.Close()
		if outFile != "" {
			err := CreateFile(outFile)
			if err != nil {
				return errors.New(fmt.Sprintf("output file %s check not pass: %v", err))
			}
		}
	} else {
		fmt.Printf("action %s must give ip,port,user,password, and the user must have replication slave,replication client ,super privileges\n", options.ActionType)
		return errors.New("options given error")
	}

	//模式 stat, 统计binlog文件有哪些表有写入
	if options.BinlogSql.Mode == "stat" {
		err := GetBinlogInfo(db, options.BinlogSql.BinlogDir, options)
		return err
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverID),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
		Charset:  charset,
		Logger:   &NoOpLogger{},
	}

	syncer := replication.NewBinlogSyncer(cfg)

	defer syncer.Close()

	position := mysql.Position{
		Name: startFile,
		Pos:  uint32(startPose),
	}

	if strings.HasPrefix(version, "5.5") {
		if options.BinlogSql.BinlogDir == "" {
			errMsg := fmt.Sprintf("The 5.5 version must give the binglog directory by --binlogDir")
			return errors.New(errMsg)
		}

		// 获取所有的 binlog 文件
		binlogFiles, err := getBinlogFiles(db, options.BinlogSql.BinlogDir)
		if err != nil {
			log.Error().Err(err)
			return err
		}

		// 初始化 binlog 解析器
		parser := replication.NewBinlogParser()
		parser.SetVerifyChecksum(true)
		for _, binFile := range binlogFiles {
			err := GetBinlogSql(db, binFile, options)
			if err != nil {
				fmt.Printf("parse sql from binlog file %s error\n", binFile)
				return err
			}
		}
		return nil
	} else {
		streamer, err := syncer.StartSync(position)
		if err != nil {
			log.Error().Err(err)
			return err
		}
		var schema TableSchema
		for {
			ev, err := streamer.GetEvent(ctx)

			if err != nil {
				// 检查是否是超时导致的退出
				if errors.Is(err, context.DeadlineExceeded) {
					log.Info().Msg(fmt.Sprintf("Context deadline exceeded: exiting binlog stream."))
					return nil
				}
				// 处理其他错误
				log.Info().Msg(fmt.Sprintf("Error in binlog streaming: %v\n", err))
				return err
			}

			err = ParseBinlogSQL(db, ev, options, syncer.GetNextPosition().Name, &schema)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("parse binlog to sql err."))
			}
		}
	}

}

func ParseBinlogSQL(db *sql.DB, ev *replication.BinlogEvent, options *model.DaemonOptions, fileName string, schema *TableSchema) error {
	eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
	if (options.BinlogSql.StartTime != "" && eventTime.Before(parseTime(options.BinlogSql.StartTime))) || (options.BinlogSql.StopTime != "" && eventTime.After(parseTime(options.BinlogSql.StopTime))) {
		//continue
		return nil
	}

	transactionID := ev.Header.LogPos

	switch e := ev.Event.(type) {
	case *replication.QueryEvent:
		// 检查是否是事务开始的 QueryEvent
		if strings.ToUpper(strings.TrimSpace(string(e.Query))) == "BEGIN" {
			// 忽略事务开始的 QueryEvent
			//continue
			return nil
		}

		// 如果是DDL语句，检查是否作用于指定的db和table
		if IsDDL(string(e.Query)) {
			schema.DbName, schema.TableName = ParseDDL(string(e.Query))
			if (options.BinlogSql.DBName != "" && schema.DbName != options.BinlogSql.DBName) || (options.BinlogSql.TableName != "" && schema.TableName != options.BinlogSql.TableName) {
				//continue
				return nil
			}
		}
		if options.BinlogSql.DDL != "false" {
			if options.BinlogSql.OutFile != "" {
				err := AppendToFile(options.BinlogSql.OutFile, string(e.Query))
				if err != nil {
					log.Error().Err(err).Msg("append SQL to output file failed")
				}
			} else {
				fmt.Printf("/*%s:%d, Executed At: %s*/\n %s;\n", fileName, transactionID, eventTime.Format("2006-01-02 15:04:05"), e.Query)
			}
		}
		return nil

	case *replication.RowsEvent:
		schema.DbName = string(e.Table.Schema)
		schema.TableName = string(e.Table.Table)

		if (options.BinlogSql.DBName != "" && schema.DbName != options.BinlogSql.DBName) || (options.BinlogSql.TableName != "" && schema.TableName != options.BinlogSql.TableName) {
			//continue
			return nil
		}

		sql, err := generateSQL(db, ev.Header.EventType, e, options.BinlogSql.Mode, transactionID, eventTime, fileName)
		if err != nil {
			log.Error().Err(err).Msg("Error generating SQL")
			return err
		}
		if options.BinlogSql.OutFile != "" {
			err := AppendToFile(options.BinlogSql.OutFile, sql)
			if err != nil {
				log.Error().Err(err).Msg("append SQL to output file failed")
			}
		} else {
			fmt.Printf("%s\n", sql)
		}
		return nil

	case *replication.RotateEvent:
		if options.BinlogSql.StopFile != "" && string(e.NextLogName) == options.BinlogSql.StopFile && e.Position == uint64(options.BinlogSql.StopPose) {
			return nil
		}
		rotate := fmt.Sprintf("Rotate to %s, pos %d\n", e.NextLogName, e.Position)
		if options.BinlogSql.RotateFlag != "false" {
			if options.BinlogSql.OutFile != "" {
				err := AppendToFile(options.BinlogSql.OutFile, rotate)
				if err != nil {
					log.Error().Err(err).Msg("append SQL to output file failed")
				}
			} else {
				fmt.Printf("-- %s\n", rotate)
			}
		}
		return nil
	case *replication.XIDEvent:
		if (options.BinlogSql.DBName != "" && schema.DbName != options.BinlogSql.DBName) || (options.BinlogSql.TableName != "" && schema.TableName != options.BinlogSql.TableName) {
			//continue
			schema.DbName = ""
			schema.TableName = ""
			return nil
		}
		fmt.Printf("/* Xid=%d, Position=%d */\n", e.XID, ev.Header.LogPos)
		return nil
	default:
		log.Info().Msg(fmt.Sprintf("event is not define: %v", e))
		return nil

	}

}

func IsDDL(query string) bool {
	ddlRegex := regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|RENAME|TRUNCATE)\s+`)
	return ddlRegex.MatchString(query)
}

func ParseDDL(query string) (string, string) {
	// 假设DDL语句的格式为: CREATE TABLE db.table (...), ALTER TABLE db.table ..., DROP TABLE db.table ...
	ddlRegex := regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|RENAME|TRUNCATE)\s+(TABLE\s+)?(?P<db>\w+)\.(?P<table>\w+)`)
	match := ddlRegex.FindStringSubmatch(query)

	if len(match) == 0 {
		return "", ""
	}

	paramsMap := make(map[string]string)
	for i, name := range ddlRegex.SubexpNames() {
		if i != 0 && name != "" {
			paramsMap[name] = match[i]
		}
	}

	return paramsMap["db"], paramsMap["table"]
}

func parseTime(timeStr string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		log.Error().Err(err).Msg("Error parsing time")
		os.Exit(1)
	}
	return t
}

func generateSQL(db *sql.DB, eventType replication.EventType, e *replication.RowsEvent, mode string, transactionID uint32, eventTime time.Time, fileName string) (string, error) {
	schema := string(e.Table.Schema)
	table := string(e.Table.Table)

	tableColumn, err := getColumn(db, schema, table)
	if err != nil {
		return "", err
	}

	var sqls []string
	switch eventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if mode == "flashback" {
			sqls = generateDeleteSQL(tableColumn, e.Rows)
		} else {
			sqls = generateInsertSQL(tableColumn, e.Rows)
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if mode == "flashback" {
			sqls = generateReverseUpdateSQL(tableColumn, e.Rows)
		} else {
			sqls = generateUpdateSQL(tableColumn, e.Rows)
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if mode == "flashback" {
			sqls = generateInsertSQL(tableColumn, e.Rows)
		} else {
			sqls = generateDeleteSQL(tableColumn, e.Rows)
		}
	default:
		return "", fmt.Errorf("unsupported event type: %v", eventType)
	}

	// 将事务 ID 和执行时间添加到每条 SQL 语句中
	for i, sql := range sqls {
		sqls[i] = fmt.Sprintf("/*%s:%d, Executed At: %s*/\n%s", fileName, transactionID, eventTime.Format("2006-01-02 15:04:05"), sql)
	}

	return strings.Join(sqls, "\n"), nil
}

func getColumn(db *sql.DB, schema, table string) (TableSchema, error) {
	var tableColumn TableSchema
	var column Column
	if db == nil {
		return tableColumn, fmt.Errorf("database connection is not available")
	}
	tableColumn.DbName = schema
	tableColumn.TableName = table
	query := "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION;"
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return tableColumn, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return tableColumn, err
		}

		tableColumn.Columns = append(tableColumn.Columns, column)
	}
	return tableColumn, nil
}

// 生成列-值子句的通用函数
func generateClauses(columnNames []Column, values []interface{}, insertFlag bool) []string {
	clauses := []string{}

	for i, value := range values {
		if value == nil || value == "" {
			continue
		}

		valType, v := common.FormatValue(value)
		var clause string
		if valType == "string" {
			if insertFlag {
				clause = fmt.Sprintf("'%v'", v)
			} else {
				if columnNames[i].Type == "json" {
					continue
				}
				clause = fmt.Sprintf("%s='%v'", columnNames[i].Name, v)
			}
		} else if valType == "int" {
			if insertFlag {
				clause = fmt.Sprintf("%v", v)
			} else {
				if columnNames[i].Type == "json" {
					continue
				}
				clause = fmt.Sprintf("%s=%v", columnNames[i].Name, v)
			}
		} else {
			if insertFlag {
				clause = fmt.Sprintf("%v", v)
			} else {
				if columnNames[i].Type == "json" {
					continue
				}
				clause = fmt.Sprintf("%s=%v", columnNames[i].Name, v)
			}
		}
		clauses = append(clauses, clause)
	}
	return clauses
}

func generateInsertSQL(tableColumn TableSchema, rows [][]interface{}) []string {
	var sqls []string
	var columnNames []string
	for _, colName := range tableColumn.Columns {
		columnNames = append(columnNames, colName.Name)
	}
	columns := strings.Join(columnNames, ", ")

	// 用来存储所有行的 VALUES 子句
	var valuesClauses []string
	for _, row := range rows {
		values := generateClauses(tableColumn.Columns, row, true)
		valuesClause := fmt.Sprintf("(%s)", strings.Join(values, ", "))
		valuesClauses = append(valuesClauses, valuesClause)
	}

	// 将所有 VALUES 子句拼接成一条 SQL 语句
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", tableColumn.TableName, columns, strings.Join(valuesClauses, ", "))
	sqls = append(sqls, sql)
	return sqls
}

func generateUpdateSQL(tableColumn TableSchema, rows [][]interface{}) []string {
	var sqls []string
	for i := 0; i < len(rows); i += 2 {
		before := rows[i]
		after := rows[i+1]
		setClauses := generateClauses(tableColumn.Columns, after, false)
		whereClauses := generateClauses(tableColumn.Columns, before, false)
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableColumn.TableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return sqls
}

func generateReverseUpdateSQL(tableColumn TableSchema, rows [][]interface{}) []string {
	var sqls []string
	for i := 0; i < len(rows); i += 2 {
		before := rows[i]
		after := rows[i+1]
		setClauses := generateClauses(tableColumn.Columns, before, false)
		whereClauses := generateClauses(tableColumn.Columns, after, false)
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableColumn.TableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return sqls
}

func generateDeleteSQL(tableColumn TableSchema, rows [][]interface{}) []string {
	var sqls []string
	for _, row := range rows {
		whereClauses := generateClauses(tableColumn.Columns, row, false)
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s;", tableColumn.TableName, strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return sqls
}
