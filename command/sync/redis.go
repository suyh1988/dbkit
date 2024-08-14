package sync

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
)

var PrimaryKeyIndex []int

func handleInsertEvent(ctx context.Context, client *redis.Client, schema, table string, row []interface{}, db *sql.DB, e *replication.RowsEvent) {
	var key string

	key = generateRedisKey(schema, table, row, db)
	data := generateRedisData(row, db, e)

	log.Info().Str("key", key).Interface("data", data).Msg("Debugging Redis HSet input")
	// 调用 HSet 插入数据到 Redis
	_, err := client.HMSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to insert data into Redis")
	}
}

func handleUpdateEvent(client *redis.Client, schema, table string, newRow []interface{}, db *sql.DB, e *replication.RowsEvent) {
	var key string

	key = generateRedisKey(schema, table, newRow, db)

	log.Info().Msg(fmt.Sprintf("key: %s", key))
	data := generateRedisData(newRow, db, e)
	_, err := client.HMSet(ctx, key, data).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to update data in Redis")
	}
}

func handleDeleteEvent(client *redis.Client, schema, table string, row []interface{}, db *sql.DB) {
	var key string

	key = generateRedisKey(schema, table, row, db)

	_, err := client.Del(ctx, key).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete data from Redis")
	}
}

func generateRedisKey(schema, table string, row []interface{}, db *sql.DB) string {
	var primaryKeys []string

	primaryKeyIndex, err := getPrimaryKeyIndex(db, schema, table)
	if err == nil {
		for _, idx := range primaryKeyIndex {
			fmt.Printf("suyh: primary key %s \n", strconv.FormatInt(int64(row[idx].(int32)), 10))
			primaryKeys = append(primaryKeys, strconv.FormatInt(int64(row[idx].(int32)), 10))
		}
	} else {
		log.Error().Err(err).Msg(fmt.Sprintf("get %s.%s not exist primary, sync data to redis must have a mysql primary key", schema, table))
		return ""
	}
	primaryKeyStr := strings.Join(primaryKeys, ":")
	log.Info().Msg(primaryKeyStr)
	return fmt.Sprintf("%s:%s", table, primaryKeyStr)
}

func generateRedisData(row []interface{}, db *sql.DB, e *replication.RowsEvent) map[string]interface{} {
	// Generate the data map from the row
	eventDB := string(e.Table.Schema)
	eventTable := string(e.Table.Table)
	data := make(map[string]interface{})
	columns, _ := getColumnNames(db, eventDB, eventTable)
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

func getPrimaryKeyIndex(db *sql.DB, schema, table string) ([]int, error) {
	if len(PrimaryKeyIndex) == 0 {
		//log.Info().Msg(fmt.Sprintf("Init %s.%s primary key index", schema, table))
		query := "SELECT B.ORDINAL_POSITION-1 AS Idx FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN INFORMATION_SCHEMA.COLUMNS B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME WHERE A.TABLE_SCHEMA = ? AND A.TABLE_NAME = ? AND A.CONSTRAINT_NAME = 'PRIMARY' ORDER BY B.ORDINAL_POSITION;"
		rows, err := db.Query(query, schema, table)
		if err != nil {
			log.Error().Err(err).Msg("get table primary key index failed")
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var columnName int
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
			log.Info().Msg(fmt.Sprintf("primary key index:%d\n", columnName))
			PrimaryKeyIndex = append(PrimaryKeyIndex, columnName)
		}
	}
	log.Info().Msg(fmt.Sprintf("primary keys length %d\n", len(PrimaryKeyIndex)))
	return PrimaryKeyIndex, nil
}
