package command

import (
<<<<<<< HEAD
	"example.com/m/v2/command/binlogsql"
	"example.com/m/v2/command/filter"
	"example.com/m/v2/command/sync"
	"example.com/m/v2/model"
=======
	"dbkit/command/binlogsql"
	"dbkit/command/sync"
	"dbkit/model"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	"github.com/urfave/cli"
)

func NewCommands(opts *model.DaemonOptions) []cli.Command {
	return []cli.Command{
		binlogsql.NewBinlogSqlCommand(opts),
		sync.NewSyncCommand(opts),
<<<<<<< HEAD
		filter.NewFilterCommand(opts),
=======
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	}
}
