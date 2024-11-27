package model

type SyncOption struct {
<<<<<<< HEAD
	ConfigFile            string
	WriteEventInterval    int64
	WriteTimeInterval     int64
	PrimaryKeyColumnNames map[string][]string
	TableColumnMap        map[string][]string
	RedisWriteMode        string
	RedisWriteBatchSize   int
=======
	SourceIP       string // mysql IP
	SourcePort     int    // mysql port
	SourceUser     string // mysql user
	SourcePassWord string // mysql password
	TargetType     string
	SyncMode       string
	TargetIP       string
	TargetPort     string
	TargetUser     string
	TargetPassword string
	ConfigFile     string
	BinlogPos      string

	DBName    string // mysql database name
	TableName string // mysql table name
	ServerID  int    //server id
	Mode      string // operation type
	CharSet   string
	RedisDB   int
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
}
