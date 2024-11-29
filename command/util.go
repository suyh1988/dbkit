package command

import (
	"example.com/m/v2/command/binlogsql"
	"example.com/m/v2/command/filter"
	"example.com/m/v2/command/sync"
	"example.com/m/v2/model"
	"github.com/urfave/cli"
)

func NewCommands(opts *model.DaemonOptions) []cli.Command {
	return []cli.Command{
		binlogsql.NewBinlogSqlCommand(opts),
		sync.NewSyncCommand(opts),
		filter.NewFilterCommand(opts),
	}
}
