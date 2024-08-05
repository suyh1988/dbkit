package command

import (
	"dbkit/command/binlogsql"
	"dbkit/command/sync"
	"dbkit/model"
	"github.com/urfave/cli"
)

func NewCommands(opts *model.DaemonOptions) []cli.Command {
	return []cli.Command{
		binlogsql.NewBinlogSqlCommand(opts),
		sync.NewSyncCommand(opts),
	}
}
