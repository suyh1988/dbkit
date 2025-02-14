package sync

import (
	"database/sql"
	"errors"
	"example.com/m/v2/conf"
	"example.com/m/v2/model"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
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
