package logging

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path"
)

// Configuration for logging
type Config struct {
	// Enable console logging
	ConsoleLoggingEnabled bool

	// EncodeLogsAsJson makes the log framework log JSON
	EncodeLogsAsJson bool
	// FileLoggingEnabled makes the framework log to a file
	// the fields below can be skipped if this value is false!
	FileLoggingEnabled bool
	// Directory to log to to when filelogging is enabled
	Directory string
	// Filename is the name of the logfile which will be placed inside the directory
	Filename string
	// MaxSize the max size in MB of the logfile before it's rolled
	MaxSize int
	// MaxBackups the max number of rolled files to keep
	MaxBackups int
	// MaxAge the max age in days to keep a logfile
	MaxAge int
}

type Logger struct {
	w      io.Writer
	config *Config
	*zerolog.Logger
}

func (l *Logger) Close() {
	if closer, ok := l.w.(io.WriteCloser); ok {
		closer.Close()
	}
}

// Configure sets up the logging framework
//
// In production, the container logs will be collected and file logging should be disabled. However,
// during development it's nicer to see logs as text and optionally write to a file when debugging
// problems in the containerized pipeline
//
// The output log file will be located at /var/log/service-xyz/service-xyz.log and
// will be rolled according to configuration set.
func Configure(config Config) *Logger {
	//var writers []io.Writer

	//if config.ConsoleLoggingEnabled {
	//	writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr})
	//}
	//if config.FileLoggingEnabled {
	//	writers = append(writers, newRollingFile(config))
	//}
	//mw := io.MultiWriter(writers...)

	var mw io.Writer
	if config.FileLoggingEnabled {
		mw = newRollingFile(config)
	} else {
		mw = zerolog.ConsoleWriter{Out: os.Stderr}
	}
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	logger := zerolog.New(mw).With().Timestamp().Caller().Logger()

	return &Logger{
		Logger: &logger,
		config: &config,
		w:      mw,
	}
}

func (logger *Logger) ReConfigure(name string) {
	logger.Close()
	if !logger.config.FileLoggingEnabled {
		return
	}
	logger.config.Filename = name + ".log"
	mw := newRollingFile(*logger.config)
<<<<<<< HEAD
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
=======
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	l := zerolog.New(mw).With().Timestamp().Caller().Logger()
	logger.w = mw
	logger.Logger = &l
	log.Logger = l
}

func newRollingFile(config Config) io.Writer {
	if err := os.MkdirAll(config.Directory, 0744); err != nil {
		log.Error().Err(err).Str("path", config.Directory).Msg("can't create log directory")
		return nil
	}

	return &lumberjack.Logger{
		Filename:   path.Join(config.Directory, config.Filename),
		MaxBackups: config.MaxBackups, // files
		MaxSize:    config.MaxSize,    // megabytes
		MaxAge:     config.MaxAge,     // days
	}
}
