package model

type MysqlBinlogSql struct {
	IP        string // mysql IP
	Port      int    // mysql port
	User      string // mysql user
	PassWord  string // mysql password
	DBName    string // mysql database name
	TableName string // mysql table name
	ServerID  int    //server id
	Mode      string // operation type
	CharSet   string
	StartFile string
	StopFile  string
	StartPose int
	StopPose  int
	StartTime string
	StopTime  string
}
