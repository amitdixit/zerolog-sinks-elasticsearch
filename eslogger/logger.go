package eslogger

import (
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// Logger is a custom logger that logs messages using Zerolog.
type Logger struct {
	logger zerolog.Logger
}

type LoggerProperty struct {
	Value interface{}
	Key   string
}

// var appLog Logger

// func GetLogger() *Logger {
// 	return &appLog
// }

// Log is a global instance of the Logger type.
var loggerInstance Logger

func GetLogger() *Logger {

	return &loggerInstance
}

// Init initializes the logger with the given log level and output.
// It should be called only once, in the main function.
func NewLogger(enableDebugLevel bool) *Logger {
	//zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.MessageFieldName = "message"
	zerolog.ErrorFieldName = "error.message"
	zerolog.ErrorStackMarshaler = marshallStack
	zerolog.ErrorStackFieldName = "error.stack_trace"
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.999Z"
	zerolog.TimestampFieldName = "@timestamp"
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	zerolog.LevelFieldName = "log.level"
	zerolog.CallerSkipFrameCount = 4

	logLevel := zerolog.InfoLevel
	if enableDebugLevel {
		logLevel = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(logLevel)

	hostname, _ := os.Hostname()

	options := func(r *ElasticSearchOpts) {
		r.IndexTemplate = "my-demo-app"
	}

	esWriter, err := NewEsWriter(options)

	if err != nil {
		panic(err)
	}

	multi := zerolog.MultiLevelWriter(os.Stdout, esWriter)

	const (
		ecsVersion = "1.6.0"
		originKey  = "log.origin"
	)

	loggerInstance.logger = zerolog.New(multi).With().Timestamp().Stack().
		Str("hostname", hostname).
		Int("pid", os.Getpid()).
		Str("ecs.version", ecsVersion).Logger()

	return &Logger{logger: loggerInstance.logger}
}

// Debug logs a debug message.
func (logger Logger) Debug(msg string) {
	loggerInstance.logger.Debug().Msg(msg)
}

func (logger Logger) DefaultInfo() *zerolog.Event {
	return loggerInstance.logger.Info()
}

func (logger Logger) DefaultError() *zerolog.Event {
	return loggerInstance.logger.Error()
}

// Info logs an info message.
func (logger Logger) Info(msg string) {
	loggerInstance.logger.Info().Msg(msg)

}

// Info logs an info message.
func (logger Logger) Infof(format string, v ...interface{}) {
	loggerInstance.logger.Info().Msgf(format, v...)
}

func (logger Logger) InfofWithProperties(format string, properties ...LoggerProperty) {

	event := loggerInstance.logger.Info()
	for _, element := range properties {
		event.Any(element.Key, element.Value)
	}
	event.Msg(format)
}

// Warn logs a warning message.
func (logger Logger) Warn(msg string) {
	loggerInstance.logger.Warn().Msg(msg)
}

// Error logs an error message.
func (logger Logger) Error(msg string, err error) {
	loggerInstance.logger.Error().Err(err).Stack().Msg(msg)
}

// Fatal logs a fatal message and exits the program.
func (logger Logger) Fatal(msg string, err error) {
	loggerInstance.logger.Fatal().Err(err).Msg(msg)
}

// Panic logs a panic message and panics.
func (logger Logger) Panic(msg string, err error) {
	loggerInstance.logger.Panic().Err(err).Msg(msg)
}

type stackTracer interface {
	stackTrace() errors.StackTrace
}

func marshallStack(err error) interface{} {
	if e, ok := err.(stackTracer); ok {
		st := e.stackTrace()
		return fmt.Sprintf("%+v", st)
	}

	return nil
}
