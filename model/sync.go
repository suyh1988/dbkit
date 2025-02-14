package model

type SyncOption struct {
	ConfigFile            string
	WriteEventInterval    int64
	WriteTimeInterval     int64
	PrimaryKeyColumnNames map[string][]string
	TableColumnMap        map[string][]string //保存从MySQL information_schema中查询到的表字段名
	WriteMode             string
	WriteBatchSize        int
}
