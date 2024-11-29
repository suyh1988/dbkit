package sync

import (
<<<<<<< HEAD
	"database/sql"
	"errors"
	"example.com/m/v2/conf"
	"example.com/m/v2/model"
=======
	"context"
	"database/sql"
	"errors"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
	"sync"
<<<<<<< HEAD
	"time"
=======
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
)

type BinlogPosition struct {
	File     string
	Position uint32
}

<<<<<<< HEAD
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
=======
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
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600

	// 获取当前的 binlog 位点
	row := tx.QueryRow("SHOW MASTER STATUS")
	var discard1, discard2, discard3 interface{}
	err = row.Scan(&binlogPos.Name, &binlogPos.Pos, &discard1, &discard2, &discard3)
	if err != nil {
<<<<<<< HEAD
		return &binlogPos, fmt.Errorf("failed to get binlog position: %v", err)
	}

	// 使用 Goroutines 并行导出多个表的数据
	var wg sync.WaitGroup

	// 遍历配置，处理每个表的数据
	for _, mapping := range conf.Mapping {
		for _, table := range mapping.Tables {
			if options.MysqlSync.RedisWriteMode == "batch" {
				err = dumpSingleTableDataBatch(tx, *rdb, mapping.Database, table.Table, options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table], table.Columns, options, options.MysqlSync.RedisWriteBatchSize)
			} else {
				err = dumpSingleTableData(tx, *rdb, mapping.Database, table.Table, options.MysqlSync.PrimaryKeyColumnNames[mapping.Database+"."+table.Table], table.Columns, options, concurrencyLimit)
			}
			if err != nil {
				log.Error().Err(err).Msgf("Failed to dump table %s.%s", mapping.Database, table.Table)
			}
		}
	}

	// 等待所有 Goroutines 完成
	wg.Wait()

=======
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

>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	// 提交事务
	err = tx.Commit()
	if err != nil {
		return &binlogPos, fmt.Errorf("failed to commit transaction: %v", err)
	}

	// 返回 binlog 位点
	return &binlogPos, nil
}

<<<<<<< HEAD
func getPrimaryKeysColumnNames(db *sql.DB, schema, table string) ([]string, error) {
=======
func getPrimaryKeys(db *sql.DB, schema, table string) ([]string, error) {
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
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
<<<<<<< HEAD
=======
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
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600

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
<<<<<<< HEAD

func dumpSingleTableDataBatch(tx *sql.Tx, rdb redis.UniversalClient, schema string, table string, PKColNames []string, columns []string, options *model.DaemonOptions, batchSize int) error {
	columnList := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM %s.%s;", columnList, schema, table)

	// 执行查询
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", schema, table, err)
	}
	defer rows.Close()

	// 获取列名
	dbColumns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names for %s.%s: %v", schema, table, err)
	}
	if len(dbColumns) != len(columns) {
		return fmt.Errorf("mismatch in requested and retrieved columns for %s.%s", schema, table)
	}

	// 缓存批量写入的键值对
	batch := make(map[string]map[string]interface{})

	// 定义批量写入逻辑
	writeBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		pipe := rdb.Pipeline() // 创建 Pipeline
		for key, fields := range batch {
			pipe.HMSet(options.Ctx, key, fields)
		}

		if _, err := pipe.Exec(options.Ctx); err != nil {
			log.Error().Err(err).Msg("Failed to execute batch Redis writes")
			return err
		}

		//每个批量间间隔50毫秒
		time.Sleep(50 * time.Millisecond)
		log.Debug().Msgf("Successfully wrote %d keys to Redis", len(batch))
		batch = make(map[string]map[string]interface{}) // 清空缓存
		return nil
	}

	for rows.Next() {
		// 存储当前行数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// 读取数据并创建独立拷贝
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan rows for %s.%s: %v", schema, table, err)
		}

		// 将数据添加到批量缓存中
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = string(values[i])
		}

		redisKey := generateRedisKey(table, scanArgs, columns, PKColNames)
		batch[redisKey] = rowData

		// 如果达到批量大小，写入 Redis
		if len(batch) >= batchSize {
			if err := writeBatch(); err != nil {
				return err
			}
		}
	}

	// 写入剩余未达到批量大小的数据
	if err := writeBatch(); err != nil {
		return err
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error for %s.%s: %v", schema, table, err)
	}

	return nil
}

func dumpSingleTableData(tx *sql.Tx, rdb redis.UniversalClient, schema string, table string, PKColNames []string, columns []string, options *model.DaemonOptions, conLimit int) error {
	columnList := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM %s.%s;", columnList, schema, table)

	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %v", schema, table, err)
	}
	defer rows.Close()

	dbColumns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names for %s.%s: %v", schema, table, err)
	}
	if len(dbColumns) != len(columns) {
		return fmt.Errorf("mismatch in requested and retrieved columns for %s.%s", schema, table)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, conLimit)

	for rows.Next() {
		// 存储当前行数据
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// 读取数据并创建独立拷贝
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan rows for %s.%s: %v", schema, table, err)
		}

		rowData := make(map[string]string)
		for i, col := range columns {
			rowData[col] = string(values[i])
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(rowData map[string]string) {
			defer wg.Done()
			defer func() { <-sem }()

			redisKey := generateRedisKey(table, scanArgs, columns, PKColNames)

			if err := rdb.HMSet(options.Ctx, redisKey, rowData).Err(); err != nil {
				log.Error().Err(err).Msgf("Failed to write to Redis: key=%s", redisKey)
			}
		}(rowData)
	}

	// 等待所有 Goroutine 完成
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
=======
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
