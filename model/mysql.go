package model

import (
	"database/sql"
	"fmt"
)

func GetMysqlVersion(db *sql.DB) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database connection is not available")
	}
	query := "SELECT version();"
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var version string
	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", err
		}
	}
	return version, nil
}
