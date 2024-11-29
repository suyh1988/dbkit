package main

import (
<<<<<<< HEAD
	"example.com/m/v2/command"
	"example.com/m/v2/logging"
	"example.com/m/v2/model"
	"example.com/m/v2/pkg/config"
	"example.com/m/v2/pkg/util"
=======
	"dbkit/command"
	"dbkit/logging"
	"dbkit/model"
	"dbkit/pkg/config"
	"dbkit/pkg/util"
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
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
		Directory:             "/data/logs/",
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
<<<<<<< HEAD
	app.Name = "mysql binlog tool"
	app.Usage = `1. 支持mysql binlog文件解析出sql以及回滚sql,需要在mysql服务器上执行,并且需要连上mysql;
				2. 支持分析binlog文件,还有哪些表有写入,通常用于下线检查;
				3. 支持mysql数据变更同步到异构数据库,如redis/mongodb/es等
`
=======
	app.Name = "Operate Tools"
	app.Usage = ``
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	app.Version = "1.0.0"

	app.Flags = config.NewGlobalFlags(options)
	app.Commands = command.NewCommands(options)
	app.Before = config.CreateBeforeRun(options)

	go util.WaitSignals(options.Ctx, options.Cancel)

	err := app.Run(os.Args)
	if err != nil {
		log.Error().Msg(err.Error())
<<<<<<< HEAD
		println("err:" + err.Error())
=======
		println("有错误:" + err.Error())
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
		os.Exit(-1)
	}
}
