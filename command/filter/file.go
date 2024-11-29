package filter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// DatabaseTableStruct 用于存储数据库名和表名的映射关系
type DatabaseTableStruct struct {
	tables map[string]bool
	finish bool
	offset int64
}

// DatabaseStruct 用于存储数据库名和数据库对象的映射关系
type DatabaseStruct struct {
	databases map[string]*DatabaseTableStruct
}

type TableStruct struct {
	offset int64
	flag   bool
}

// NewDatabaseStruct 创建一个新的 DatabaseStruct 实例
func NewDatabaseStruct() *DatabaseStruct {
	return &DatabaseStruct{
		databases: make(map[string]*DatabaseTableStruct),
	}
}

// NewDatabaseStruct 创建一个新的 DatabaseStruct 实例
func NewDatabaseStructInit(tables []string) (*DatabaseStruct, error) {
	dbStruct := &DatabaseStruct{
		databases: make(map[string]*DatabaseTableStruct),
	}
	for _, dbTableStrings := range tables {
		dbTable := strings.Split(dbTableStrings, ".")
		if len(dbTable) != 2 {
			return nil, errors.New("the option schema input error")
		}

		// 添加数据库
		dbStruct.AddDatabase(dbTable[0])
		fmt.Printf("add database %s\n", dbTable[0])

		// 添加表
		dbStruct.AddTable(dbTable[0], dbTable[1])
		fmt.Printf("add table %s.%s\n", dbTable[0], dbTable[1])

	}
	return dbStruct, nil
}

// AddDatabase 添加一个新的数据库
func (ds *DatabaseStruct) AddDatabase(dbName string) {
	dbName = strings.ToUpper(dbName)
	if _, exists := ds.databases[dbName]; !exists {
		ds.databases[dbName] = &DatabaseTableStruct{tables: make(map[string]bool)}
	}
}

// AddTable 添加一个新的表到指定的数据库
func (ds *DatabaseStruct) AddTable(dbName, tableName string) {
	dbName = strings.ToUpper(dbName)
	tableName = strings.ToUpper(tableName)
	if db, exists := ds.databases[dbName]; exists {
		db.tables[tableName] = false
	} else {
		fmt.Printf("Database %s does not exist. Add the database first.\n", dbName)
	}
}

// DatabaseExists 检查数据库是否存在
func (ds *DatabaseStruct) DatabaseExists(dbName string) bool {
	dbName = strings.ToUpper(dbName)
	_, exists := ds.databases[dbName]
	return exists
}

// TableExists 检查表是否存在于指定的数据库
func (ds *DatabaseStruct) TableExists(dbName, tableName string) bool {
	dbName = strings.ToUpper(dbName)
	tableName = strings.ToUpper(tableName)
	if db, exists := ds.databases[dbName]; exists {
		_, tableExists := db.tables[tableName]
		_, allFlagExists := db.tables["*"]
		//如果用户参数里面有dbname.*时，不管这个函数调用传入的表名是否存在，都返回true
		return tableExists || allFlagExists
	}
	return false
}

func regexpLine(regString string, TargetString string) (string, error) {
	regString = strings.ToUpper(regString)
	re := regexp.MustCompile(regString)
	match := re.FindStringSubmatch(TargetString)
	if len(match) > 1 {
		// match[1] 是第一个捕获组，即数据库名（去掉反引号）
		return match[1], nil
	} else {
		return "", errors.New(fmt.Sprintln("No match found in line:", TargetString))
	}
}
