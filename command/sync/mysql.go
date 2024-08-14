package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
	"sync"
)

type BinlogPosition struct {
	File     string
	Position uint32
}

// DumpMySQLTableToRedis 将 MySQL 的表数据导出到 Redis 中
func DumpMySQLTableToRedis(db *sql.DB, schema, table string, rdb *redis.Client) (*mysql.Position, error) {
	ctx := context.Background()
	var binlogPos mysql.Position

	// 获取表的主键字段
	primaryKeys, err := getPrimaryKeys(db, schema, table)
	if err != nil {
		return &binlogPos, errors.New(fmt.Sprintf("failed to get primary keys: %v", err))
	}

	// 开启一致性视图
	tx, err := db.Begin()
	if err != nil {
		log.Error().Err(err).Msg("start transaction for get mysql binlog position error")
	}
	defer tx.Rollback()

	// 获取当前的 binlog 位点
	row := tx.QueryRow("SHOW MASTER STATUS")
	var discard1, discard2, discard3 interface{}
	err = row.Scan(&binlogPos.Name, &binlogPos.Pos, &discard1, &discard2, &discard3)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get binlog position")
	}

	// 查询表数据
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", schema, table)
	rows, err := tx.Query(query)
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to query MySQL: %v", err)
	}
	defer rows.Close()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to get column names: %v", err)
	}

	var wg sync.WaitGroup
	concurrencyLimit := 10                       // 并发数量限制
	sem := make(chan struct{}, concurrencyLimit) // 信号量用于控制并发量

	// 遍历查询结果并写入 Redis
	for rows.Next() {
		// 存储每一行的数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return &binlogPos, fmt.Errorf("failed to scan rows: %v", err)
		}

		wg.Add(1)
		sem <- struct{}{} // 获取一个信号量
		go func(scanArgs []interface{}, values []sql.RawBytes) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			// 构建 Redis key
			redisKey := generateRedisKey2(table, scanArgs, columns, primaryKeys)

			// 将数据写入 Redis 的 hash 结构
			data := make(map[string]interface{})
			for i, col := range columns {
				data[col] = string(values[i])
			}

			if err := rdb.HMSet(ctx, redisKey, data).Err(); err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to write to Redis: key=%s", redisKey))
			}
		}(scanArgs, values)
	}
	// 等待所有 Goroutines 完成
	wg.Wait()

	if err = rows.Err(); err != nil {
		return &binlogPos, fmt.Errorf("rows iteration error: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to commit transaction: %v", err)
	}

	// 返回 binlog 位点
	return &binlogPos, nil
}

func getPrimaryKeys(db *sql.DB, schema, table string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, pk)
	}
	return primaryKeys, nil
}
func generateRedisKey2(table string, row []interface{}, columns []string, primaryKeys []string) string {
	var keyParts []string
	keyParts = append(keyParts, table)

	// 使用主键列名找到对应的值，并构建 Redis 键
	for _, pk := range primaryKeys {
		for i, col := range columns {
			if col == pk {
				value := row[i]
				switch v := value.(type) {
				case []byte:
					keyParts = append(keyParts, value.(string))
				case *sql.RawBytes:
					keyParts = append(keyParts, string(*v))
				case string:
					keyParts = append(keyParts, v)
				default:
					keyParts = append(keyParts, fmt.Sprintf("%v", v))
				}
				break
			}
		}
	}

	return strings.Join(keyParts, ":")
}

func CheckBinlogPosOK(pos string, db *sql.DB) (error, *mysql.Position) {
	var position *mysql.Position
	parts := strings.Split(pos, ":")
	if len(parts) != 2 {
		log.Error().Msg("Invalid binlog position format")
	}
	position.Name = parts[0]
	Pos, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Error().Err(err).Msg("Invalid binlog position")
	}
	position.Pos = uint32(Pos)
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		log.Error().Err(err).Msg("Failed to query binary logs")
		return err, nil
	}
	defer rows.Close()

	var binlogSizes []uint64
	for rows.Next() {
		var file string
		var size uint64
		if err := rows.Scan(&file, &size); err != nil {
			log.Error().Err(err).Msg("Failed to scan binary logs")
		}
		binlogSizes = append(binlogSizes, size)

		if file == position.Name && uint32(size) >= position.Pos {
			return nil, position
		}
	}
	return errors.New(fmt.Sprintf("Invalid binlog position: %s", pos)), nil

}
