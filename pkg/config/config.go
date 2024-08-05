package config

import (
	"dbkit/model"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	DEFAULT_DATA_PATH = "/Users/suyonghe/Desktop/dbkit/logs/"
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

func readConfigFromFile(configPath string) (*Config, error) {
	file, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml file: %w", err)
	}

	return &config, nil
}

func setGlobalDefault(options *model.DaemonOptions) {
	if options.CommonDataPath == "" {
		options.CommonDataPath = DEFAULT_DATA_PATH
	}

	dirs := []string{
		options.CommonDataPath,
		options.CommonDataPath + "/transfer",
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

	if options.ConfigFile != "" {
		config, err := readConfigFromFile(options.ConfigFile)
		if err != nil {
			log.Error().Err(err).Msg("failed to read config file")
			return
		}

		// Set MySQL sync options from config file if not provided via command line
		if options.MysqlSync.ServerID == 0 {
			options.MysqlSync.ServerID = config.MySQL.ServerID
		}
		if options.MysqlSync.SourceIP == "" {
			options.MysqlSync.SourceIP = config.MySQL.SourceIP
		}
		if options.MysqlSync.SourcePort == 0 {
			options.MysqlSync.SourcePort = config.MySQL.SourcePort
		}
		if options.MysqlSync.SourceUser == "" {
			options.MysqlSync.SourceUser = config.MySQL.SourceUser
		}
		if options.MysqlSync.SourcePassWord == "" {
			options.MysqlSync.SourcePassWord = config.MySQL.SourcePass
		}
		if options.MysqlSync.SyncMode == "" {
			options.MysqlSync.SyncMode = config.MySQL.SyncMode
		}
		if options.MysqlSync.TargetType == "" {
			options.MysqlSync.TargetType = config.MySQL.TargetType
		}
		if options.MysqlSync.DBName == "" {
			options.MysqlSync.DBName = config.MySQL.DBName
		}
		if options.MysqlSync.TableName == "" {
			options.MysqlSync.TableName = config.MySQL.TableName
		}
		if options.MysqlSync.CharSet == "" {
			options.MysqlSync.CharSet = config.MySQL.CharSet
		}

		// Set Redis options from config file if not provided via command line
		if options.MysqlSync.TargetIP == "" && options.MysqlSync.TargetPort == "" {
			options.MysqlSync.TargetIP = config.Redis.Addr
		}
		if options.MysqlSync.TargetUser == "" {
			options.MysqlSync.TargetUser = config.Redis.Password
		}
		if options.MysqlSync.TargetPassword == "" {
			options.MysqlSync.TargetPassword = config.Redis.Password
		}
		if options.MysqlSync.RedisDB == "" {
			options.MysqlSync.RedisDB = config.Redis.DB
		}
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
