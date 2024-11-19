package sync

import (
	"database/sql"
	"dbkit/conf"
	"dbkit/model"
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

type TablePrimayKey map[string][]string

// 初始化 MySQL 连接
func initMySQL(config *conf.Config) (*sql.DB, error) {
	if config.Source.IP == "" || config.Source.Port == 0 || config.Source.User == "" || config.Source.Password == "" {
		return nil, fmt.Errorf("MySQL 配置信息不完整")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=16777216&readTimeout=30s", config.Source.User, config.Source.Password, config.Source.IP, config.Source.Port, config.Mapping[0].Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error().Err(err).Msg("连接 MySQL 失败")
		return nil, err
	}
	return db, nil
}

func DumpFullMySQLTableToRedis(db *sql.DB, conf *conf.Config, rdb *redis.UniversalClient, options *model.DaemonOptions, concurrencyLimit int) (*mysql.Position, error) {
	if concurrencyLimit <= 0 {
		concurrencyLimit = 3 // 默认并行导出表的数量
	}

	var (
		binlogPos mysql.Position
		err       error
	)

	// 获取所有表的主键字段
	for _, mapping := range conf.Mapping {
		for _, table := range mapping.Tables {
			primaryKeyColumnNames, err := getPrimaryKeysColumnNames(db, mapping.Database, table.Table)
			if err != nil {
				return &binlogPos, fmt.Errorf("failed to get primary keys for %s.%s: %v", mapping.Database, table.Table, err)
			}
			options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table] = primaryKeyColumnNames
		}
	}

	// 开启一致性视图
	tx, err := db.BeginTx(options.Ctx, nil)
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// 获取当前的 binlog 位点
	row := tx.QueryRow("SHOW MASTER STATUS")
	var discard1, discard2, discard3 interface{}
	err = row.Scan(&binlogPos.Name, &binlogPos.Pos, &discard1, &discard2, &discard3)
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to get binlog position: %v", err)
	}

	// 使用 Goroutines 并行导出多个表的数据
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrencyLimit) // 控制并发的信号量

	// 遍历配置，处理每个表的数据
	for _, mapping := range conf.Mapping {
		for _, table := range mapping.Tables {
			wg.Add(1)
			sem <- struct{}{} // 获取信号量
			go func(schema, tableName string, primaryKeyColumnNames []string, columns []string) {
				defer wg.Done()
				defer func() { <-sem }() // 释放信号量

				err := dumpSingleTableData(tx, *rdb, schema, tableName, primaryKeyColumnNames, columns, options, concurrencyLimit)
				if err != nil {
					log.Error().Err(err).Msgf("Failed to dump table %s.%s", schema, tableName)
				}
			}(mapping.Database, table.Table, options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table], table.Columns)
		}
	}

	// 等待所有 Goroutines 完成
	wg.Wait()

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to commit transaction: %v", err)
	}

	// 返回 binlog 位点
	return &binlogPos, nil
}

func getPrimaryKeysColumnNames(db *sql.DB, schema, table string) ([]string, error) {
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

func CheckBinlogPosOK(pos string, db *sql.DB) (error, *mysql.Position) {
	var position mysql.Position
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
			return nil, &position
		}
	}
	return errors.New(fmt.Sprintf("Invalid binlog position: %s", pos)), nil

}

func dumpSingleTableData(tx *sql.Tx, rdb redis.UniversalClient, schema string, table string, primaryKeyColumnNames []string, columns []string, options *model.DaemonOptions, concurrencyLimit int) error {
	// 构建仅查询指定列的 SQL 语句
	columnList := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM %s.%s;", columnList, schema, table)

	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", schema, table, err)
	}
	defer rows.Close()

	// 获取列名（与传入的列应该一致，但做校验以防错误）
	dbColumns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names for %s.%s: %v", schema, table, err)
	}
	if len(dbColumns) != len(columns) {
		return fmt.Errorf("mismatch in requested and retrieved columns for %s.%s", schema, table)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrencyLimit) // 控制并发数量

	for rows.Next() {
		// 存储一行的数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("failed to scan rows for %s.%s: %v", schema, table, err)
		}

		wg.Add(1)
		sem <- struct{}{} // 获取信号量
		go func(scanArgs []interface{}, values []sql.RawBytes) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			// 构建 Redis key
			redisKey := generateRedisKey(table, scanArgs, columns, primaryKeyColumnNames)
			data := make(map[string]interface{})
			for i, col := range columns {
				data[col] = string(values[i])
			}

			if err := rdb.HMSet(options.Ctx, redisKey, data).Err(); err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to write to Redis: key=%s", redisKey))
			}
		}(scanArgs, values)
	}

	// 等待所有 Goroutines 完成
	wg.Wait()

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error for %s.%s: %v", schema, table, err)
	}

	return nil
}

func FlushColumnNames(options *model.DaemonOptions, db *sql.DB, syncConf *conf.Config) error {
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION;"
	for _, m := range syncConf.Mapping {
		for _, table := range m.Tables {
			rows, err := db.Query(query, m.Database, table.Table)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("flush table column infomation failed becuase get meta data from mysql"))
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var columnName string
				if err := rows.Scan(&columnName); err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("flush table column infomation failed"))
					return err
				}
				options.MysqlSync.TableColumnMap[m.Database+"."+table.Table] = append(options.MysqlSync.TableColumnMap[m.Database+"."+table.Table], columnName)
			}
		}

	}
	log.Info().Msg(fmt.Sprintf("flush table column infomation success"))

	return nil
}
