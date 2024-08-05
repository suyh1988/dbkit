package main

import (
	"dbkit/command"
	"dbkit/logging"
	"dbkit/model"
	"dbkit/pkg/config"
	"dbkit/pkg/util"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
	"os"
)

func logInit() *logging.Logger {
	/*
		// UNIX Time is faster and smaller than most timestamps
		// If you set zerolog.TimeFieldFormat to an empty string,
		// logs will write with UNIX time
		zerolog.TimeFieldFormat = ""
		// In order to always output a static time to stdout for these
		// examples to pass, we need to override zerolog.TimestampFunc
		// and log.Logger globals -- you would not normally need to do this
		zerolog.TimestampFunc = func() time.Time {
			return time.Date(2008, 1, 8, 17, 5, 05, 0, time.UTC)
		}
		log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	*/
	config := logging.Config{
		ConsoleLoggingEnabled: false,
		FileLoggingEnabled:    true,
		EncodeLogsAsJson:      true,
		Directory:             "/Users/suyonghe/Desktop/dbkit/logs/",
		Filename:              "dba.log",
		MaxSize:               100,
		MaxBackups:            10,
		MaxAge:                365,
	}
	l := logging.Configure(config)
	log.Logger = *l.Logger
	return l
}

func main() {
	l := logInit()
	options := model.NewDaemonOptions(l)

	//log.Info().Msg("dba started!!!!")
	app := cli.NewApp()
	app.Name = "Operate Tools"
	app.Usage = ``
	app.Version = "1.0.0"

	app.Flags = config.NewGlobalFlags(options)
	app.Commands = command.NewCommands(options)
	app.Before = config.CreateBeforeRun(options)

	go util.WaitSignals(options.Ctx, options.Cancel)

	err := app.Run(os.Args)
	if err != nil {
		log.Error().Msg(err.Error())
		println("有错误:" + err.Error())
		os.Exit(-1)
	}
}
