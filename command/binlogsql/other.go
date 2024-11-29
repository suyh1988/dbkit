package binlogsql

type NoOpLogger struct{}

func (l *NoOpLogger) Debug(args ...interface{})                 {}
func (l *NoOpLogger) Debugf(format string, args ...interface{}) {}
func (l *NoOpLogger) Debugln(args ...interface{})               {}

func (l *NoOpLogger) Error(args ...interface{})                 {}
func (l *NoOpLogger) Errorf(format string, args ...interface{}) {}
func (l *NoOpLogger) Errorln(args ...interface{})               {}

func (l *NoOpLogger) Info(args ...interface{})                 {}
func (l *NoOpLogger) Infof(format string, args ...interface{}) {}
func (l *NoOpLogger) Infoln(args ...interface{})               {}

func (l *NoOpLogger) Warn(args ...interface{})                 {}
func (l *NoOpLogger) Warnf(format string, args ...interface{}) {}
func (l *NoOpLogger) Warnln(args ...interface{})               {}

func (l *NoOpLogger) Fatal(args ...interface{})                 {}
func (l *NoOpLogger) Fatalf(format string, args ...interface{}) {}
func (l *NoOpLogger) Fatalln(args ...interface{})               {}

func (l *NoOpLogger) Panic(args ...interface{})                 {}
func (l *NoOpLogger) Panicf(format string, args ...interface{}) {}
func (l *NoOpLogger) Panicln(args ...interface{})               {}

func (l *NoOpLogger) Print(args ...interface{})                 {}
func (l *NoOpLogger) Printf(format string, args ...interface{}) {}
func (l *NoOpLogger) Println(args ...interface{})               {}