package model

type SyncOption struct {
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
}
