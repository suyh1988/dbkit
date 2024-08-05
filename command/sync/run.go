package sync

import (
	"context"
	"database/sql"
	"dbkit/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

var ctx = context.Background()

func Run(options *model.DaemonOptions, _args []string) error {
	var (
		serverID  = options.MysqlSync.ServerID
		mysqlIP   = options.MysqlSync.SourceIP
		mysqlPort = options.MysqlSync.SourcePort
		mysqlUser = options.MysqlSync.SourceUser
		mysqlPwd  = options.MysqlSync.SourcePassWord
		//syncMode   = options.MysqlSync.SyncMode   //increase :from the new pose ; full: dump data first, and read change from binlog pose
		//targetType = options.MysqlSync.TargetType //sync data to redis ,redis default struct is hash
		dbName  = options.MysqlSync.DBName
		tbName  = options.MysqlSync.TableName // Added table name
		charset = options.MysqlSync.CharSet

		targetIP   = options.MysqlSync.TargetIP
		targetPort = options.MysqlSync.TargetPort
		//targetUser = options.MysqlSync.TargetUser
		targetPwd = options.MysqlSync.TargetPassword
	)

	var db *sql.DB
	var err error

	if mysqlIP != "" && mysqlPort != 0 && mysqlUser != "" && mysqlPwd != "" {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPwd, mysqlIP, mysqlPort, dbName)
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Error().Err(err)
			return err
		}
		defer db.Close()
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     targetIP + ":" + targetPort,
		Password: targetPwd,
		DB:       0,
	})

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverID),
		Flavor:   "mysql",
		Host:     mysqlIP,
		Port:     uint16(mysqlPort),
		User:     mysqlUser,
		Password: mysqlPwd,
		Charset:  charset,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	position := mysql.Position{}

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

		switch e := ev.Event.(type) {
		case *replication.QueryEvent:
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

			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				for _, row := range e.Rows {
					handleInsertEvent(redisClient, eventTable, row, db)
				}
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				for i := 0; i < len(e.Rows); i += 2 {
					handleUpdateEvent(redisClient, eventTable, e.Rows[i], e.Rows[i+1], db)
				}
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				for _, row := range e.Rows {
					handleDeleteEvent(redisClient, eventTable, row, db)
				}
			}
		case *replication.RotateEvent:
			fmt.Printf("Rotate to %s, pos %d\n", e.NextLogName, e.Position)
		}
	}

	return nil
}

func handleInsertEvent(client *redis.Client, table string, row []interface{}, db *sql.DB) {
	key := generateRedisKey(table, row, db)
	data := generateRedisData(row, db)
	_, err := client.HSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to insert data into Redis")
	}
}

func handleUpdateEvent(client *redis.Client, table string, oldRow []interface{}, newRow []interface{}, db *sql.DB) {
	key := generateRedisKey(table, newRow, db)
	data := generateRedisData(newRow, db)
	_, err := client.HSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to update data in Redis")
	}
}

func handleDeleteEvent(client *redis.Client, table string, row []interface{}, db *sql.DB) {
	key := generateRedisKey(table, row, db)
	_, err := client.Del(ctx, key).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete data from Redis")
	}
}

func generateRedisKey(table string, row []interface{}, db *sql.DB) string {
	// Assuming the primary key is the first column in the row
	primaryKey := fmt.Sprintf("%v", row[0])
	return fmt.Sprintf("%s:%s", table, primaryKey)
}

func generateRedisData(row []interface{}, db *sql.DB) map[string]interface{} {
	// Generate the data map from the row
	data := make(map[string]interface{})
	columns, _ := getColumnNames(db, "your_schema_name", "your_table_name") // Replace with actual schema and table name
	for i, column := range columns {
		data[column] = row[i]
	}
	return data
}

func getColumnNames(db *sql.DB, schema, table string) ([]string, error) {
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
