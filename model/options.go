package model

import (
	"context"
<<<<<<< HEAD
	"example.com/m/v2/logging"

=======
	"dbkit/logging"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	"fmt"
	"github.com/rs/zerolog/log"
)

type DaemonOptions struct {
	ConfigFile     string
	Test           bool
	Debug          bool
	Runid          uint64
	ActionType     string
	CommonDataPath string

<<<<<<< HEAD
	BinlogSql       *BinlogSql
	MysqlSync       *SyncOption
	MysqlDumpFilter *Filter
=======
	BinlogSql *MysqlBinlogSql
	MysqlSync *SyncOption
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600

	Logger *logging.Logger
	Ctx    context.Context
	Cancel context.CancelFunc
}

func NewDaemonOptions(l *logging.Logger) *DaemonOptions {
	//userHome := util.HomeDir()

	ctx, cancel := context.WithCancel(context.Background())
	return &DaemonOptions{
<<<<<<< HEAD
		Logger:          l,
		BinlogSql:       &BinlogSql{},
		MysqlSync:       &SyncOption{},
		MysqlDumpFilter: &Filter{},
		Ctx:             ctx,
		Cancel:          cancel,
=======
		Logger:    l,
		BinlogSql: &MysqlBinlogSql{},
		MysqlSync: &SyncOption{},
		Ctx:       ctx,
		Cancel:    cancel,
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	}
}

func (opts *DaemonOptions) Print() {
	log.Info().Msg(fmt.Sprintf("ConfigFile:%s", opts.ConfigFile))
	log.Info().Msg(fmt.Sprintf("Debug: %v", opts.Debug))

}
