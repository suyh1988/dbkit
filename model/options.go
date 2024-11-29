package model

import (
	"context"
	"example.com/m/v2/logging"

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

	BinlogSql       *BinlogSql
	MysqlSync       *SyncOption
	MysqlDumpFilter *Filter

	Logger *logging.Logger
	Ctx    context.Context
	Cancel context.CancelFunc
}

func NewDaemonOptions(l *logging.Logger) *DaemonOptions {
	//userHome := util.HomeDir()

	ctx, cancel := context.WithCancel(context.Background())
	return &DaemonOptions{

		Logger:          l,
		BinlogSql:       &BinlogSql{},
		MysqlSync:       &SyncOption{},
		MysqlDumpFilter: &Filter{},
		Ctx:             ctx,
		Cancel:          cancel,
	}
}

func (opts *DaemonOptions) Print() {
	log.Info().Msg(fmt.Sprintf("ConfigFile:%s", opts.ConfigFile))
	log.Info().Msg(fmt.Sprintf("Debug: %v", opts.Debug))

}
