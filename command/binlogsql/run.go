package binlogsql

import (
	"context"
	"database/sql"
	"dbkit/model"
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

func Run(options *model.DaemonOptions, _args []string) error {
	var (
		serverID  = options.BinlogSql.ServerID
		host      = options.BinlogSql.IP
		port      = options.BinlogSql.Port
		user      = options.BinlogSql.User
		password  = options.BinlogSql.PassWord
		charset   = options.BinlogSql.CharSet
		mode      = options.BinlogSql.Mode
		dbName    = options.BinlogSql.DBName
		tbName    = options.BinlogSql.TableName // Added table name
		startFile = options.BinlogSql.StartFile
		stopFile  = options.BinlogSql.StopFile
		startPose = options.BinlogSql.StartPose
		stopPose  = options.BinlogSql.StopPose
		startTime = options.BinlogSql.StartTime
		stopTime  = options.BinlogSql.StopTime
	)

	var db *sql.DB
	var err error

	if host != "" && port != 0 && user != "" && password != "" {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName)
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Error().Err(err)
			return err
		}
		defer db.Close()
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverID),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
		Charset:  charset,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	position := mysql.Position{
		Name: startFile,
		Pos:  uint32(startPose),
	}

	streamer, err := syncer.StartSync(position)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	ctx := context.Background()
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			log.Error().Err(err)
			return err
		}

		eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
		if (startTime != "" && eventTime.Before(parseTime(startTime))) || (stopTime != "" && eventTime.After(parseTime(stopTime))) {
			continue
		}

		switch e := ev.Event.(type) {
		case *replication.QueryEvent:
			// 检查是否是事务开始的 QueryEvent
			if strings.ToUpper(strings.TrimSpace(string(e.Query))) == "BEGIN" {
				// 忽略事务开始的 QueryEvent
				continue
			}
			// 如果是DDL语句，检查是否作用于指定的db和table
			if isDDL(string(e.Query)) {
				ddlDB, ddlTable := parseDDL(string(e.Query))
				if (dbName != "" && ddlDB != dbName) || (tbName != "" && ddlTable != tbName) {
					continue
				}
			}
			fmt.Printf("[%s] %s\n", ev.Header.EventType, e.Query)
		case *replication.RowsEvent:
			eventDB := string(e.Table.Schema)
			eventTable := string(e.Table.Table)

			if dbName != "" && eventDB != dbName {
				continue
			}

			if tbName != "" && eventTable != tbName {
				continue
			}

			sql, err := generateSQL(db, ev.Header.EventType, e, mode)
			if err != nil {
				log.Printf("Error generating SQL: %v\n", err)
				continue
			}
			fmt.Printf("Generated SQL: %s\n", sql)
		case *replication.RotateEvent:
			if stopFile != "" && string(e.NextLogName) == stopFile && e.Position == uint64(stopPose) {
				return nil
			}
			fmt.Printf("Rotate to %s, pos %d\n", e.NextLogName, e.Position)
		}
	}
}

func isDDL(query string) bool {
	ddlRegex := regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|RENAME|TRUNCATE)\s+`)
	return ddlRegex.MatchString(query)
}

func parseDDL(query string) (string, string) {
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

func generateSQL(db *sql.DB, eventType replication.EventType, e *replication.RowsEvent, mode string) (string, error) {
	schema := string(e.Table.Schema)
	table := string(e.Table.Table)
	tableName := fmt.Sprintf("%s.%s", schema, table)
	columnNames, err := getColumnNames(db, schema, table)
	if err != nil {
		return "", err
	}

	switch eventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if mode == "flashback" {
			return generateDeleteSQL(tableName, columnNames, e.Rows), nil
		}
		return generateInsertSQL(tableName, columnNames, e.Rows), nil
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if mode == "flashback" {
			return generateReverseUpdateSQL(tableName, columnNames, e.Rows), nil
		}
		return generateUpdateSQL(tableName, columnNames, e.Rows), nil
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if mode == "flashback" {
			return generateInsertSQL(tableName, columnNames, e.Rows), nil
		}
		return generateDeleteSQL(tableName, columnNames, e.Rows), nil
	default:
		return "", fmt.Errorf("unsupported event type: %v", eventType)
	}
}

func getColumnNames(db *sql.DB, schema, table string) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is not available")
	}
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION;"
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, columnName)
	}
	return columnNames, nil
}

func generateInsertSQL(tableName string, columnNames []string, rows [][]interface{}) string {
	var sqls []string
	columns := strings.Join(columnNames, ", ")
	for _, row := range rows {
		values := make([]string, len(row))
		for i, value := range row {
			values[i] = fmt.Sprintf("'%v'", value)
		}
		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, columns, strings.Join(values, ", "))
		sqls = append(sqls, sql)
	}
	return strings.Join(sqls, "\n")
}

func generateUpdateSQL(tableName string, columnNames []string, rows [][]interface{}) string {
	var sqls []string
	for i := 0; i < len(rows); i += 2 {
		before := rows[i]
		after := rows[i+1]
		setClauses := make([]string, len(after))
		whereClauses := make([]string, len(before))
		for j, value := range after {
			setClauses[j] = fmt.Sprintf("%s='%v'", columnNames[j], value)
		}
		for j, value := range before {
			whereClauses[j] = fmt.Sprintf("%s='%v'", columnNames[j], value)
		}
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return strings.Join(sqls, "\n")
}

func generateReverseUpdateSQL(tableName string, columnNames []string, rows [][]interface{}) string {
	var sqls []string
	for i := 0; i < len(rows); i += 2 {
		before := rows[i]
		after := rows[i+1]
		setClauses := make([]string, len(before))
		whereClauses := make([]string, len(after))
		for j, value := range before {
			setClauses[j] = fmt.Sprintf("%s='%v'", columnNames[j], value)
		}
		for j, value := range after {
			whereClauses[j] = fmt.Sprintf("%s='%v'", columnNames[j], value)
		}
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return strings.Join(sqls, "\n")
}

func generateDeleteSQL(tableName string, columnNames []string, rows [][]interface{}) string {
	var sqls []string
	for _, row := range rows {
		whereClauses := make([]string, len(row))
		for i, value := range row {
			whereClauses[i] = fmt.Sprintf("%s='%v'", columnNames[i], value)
		}
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, strings.Join(whereClauses, " AND "))
		sqls = append(sqls, sql)
	}
	return strings.Join(sqls, "\n")
}
