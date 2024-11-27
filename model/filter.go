package model

type Filter struct {
	InFile             string //filter input mysqldump file
	OutFile            string //filtered mysqldump file
	TableList          string //filter databases of mysqldump file
	BufferSize         int
	LinePrefixLongByte int
}
