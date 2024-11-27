package config

import (
<<<<<<< HEAD
	"errors"
	"example.com/m/v2/model"
=======
	"dbkit/model"
	"errors"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	DEFAULT_DATA_PATH = "/data/logs/"
)

type Config struct {
	MySQL struct {
		ServerID   int    `yaml:"server_id"`
		SourceIP   string `yaml:"source_ip"`
		SourcePort int    `yaml:"source_port"`
		SourceUser string `yaml:"source_user"`
		SourcePass string `yaml:"source_password"`
		SyncMode   string `yaml:"sync_mode"`
		TargetType string `yaml:"target_type"`
		DBName     string `yaml:"db_name"`
		TableName  string `yaml:"table_name"`
		CharSet    string `yaml:"charset"`
	} `yaml:"mysql_sync"`
	Redis struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		DB       string `yaml:"db"`
	} `yaml:"redis"`
}

func NewGlobalFlags(opts *model.DaemonOptions) []cli.Flag {
	return []cli.Flag{
		cli.Uint64Flag{
			Name:        "runid",
			Value:       0,
			Usage:       "Runid of this action.",
			Required:    false,
			Destination: &opts.Runid,
		},
		cli.StringFlag{
			Name:        "type",
			Value:       "",
			Usage:       "Action type.",
			Required:    false,
			Destination: &opts.ActionType,
		},
		cli.BoolFlag{
			Name:        "debug",
			Usage:       "run with debug mode",
			Required:    false,
			Destination: &opts.Debug,
		},
		cli.StringFlag{
			Name:        "config",
			Value:       "",
			Usage:       "Global Config File",
			Required:    false,
			Destination: &opts.ConfigFile,
		},
	}
}

func setGlobalDefault(options *model.DaemonOptions) {
	if options.CommonDataPath == "" {
		options.CommonDataPath = DEFAULT_DATA_PATH
	}

	dirs := []string{
		options.CommonDataPath,
		options.CommonDataPath + "/dbkit",
	}

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err = os.MkdirAll(dir, 0755); err != nil {
				log.Error().Err(err).Str("dir", dir).Msg("mkdir common data path failed!")
				panic(err)
			}
		}
	}

	if runtime.GOOS == "windows" {
		options.Test = true
	}

}

func CreateBeforeRun(options *model.DaemonOptions) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		setGlobalDefault(options)
		options.Print()
		if len(c.Args()) == 0 {
			return nil
		}
		cmdName := c.Args().Get(0)
		cmd := c.App.Command(cmdName)
		if cmd == nil {
			return errors.New("Unknown subcommand :" + cmdName)
		}
		action := cmd.Name

		options.Logger.ReConfigure(action)

		if options.Debug {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}
		if options.Runid == 0 {
			rand.Seed(time.Now().UnixNano())
			options.Runid = rand.Uint64()
		}
		ctx := log.Logger.With().Uint64("runid", options.Runid)
		if options.ActionType != "" {
			ctx = ctx.Str("actiontype", options.ActionType)
		} else if len(c.Args()) > 0 {
			ctx = ctx.Str("actiontype", action)
		}
		log.Logger = ctx.Logger()

		return nil
	}
}
