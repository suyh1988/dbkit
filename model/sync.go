package model

type SyncOption struct {
	ConfigFile            string
	WriteEventInterval    int64
	WriteTimeInterval     int64
	PrimaryKeyColumnNames map[string][]string
	TableColumnMap        map[string][]string
}
