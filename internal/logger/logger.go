package logger

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Audit     *zap.Logger
	Telemetry *zap.Logger
	Error     *zap.Logger
)

func Init() {
	isFileLogging := os.Getenv("LOG_TO_FILE") == "true"
	logDir := "logs"
	if isFileLogging {
		_ = os.MkdirAll(logDir, 0755)
	}

	Audit = newZapLogger(filepath.Join(logDir, "risk-audit.log"), 100, 2555, isFileLogging)
	Telemetry = newZapLogger(filepath.Join(logDir, "risk-telemetry.log"), 100, 14, isFileLogging)
	Error = newZapLogger(filepath.Join(logDir, "risk-errors.log"), 100, 30, isFileLogging)
}

func newZapLogger(filename string, maxSize int, maxAge int, isFileLogging bool) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "ts"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.MessageKey = "msg"

	encoder := zapcore.NewJSONEncoder(encoderConfig)

	var writeSyncer zapcore.WriteSyncer

	if isFileLogging {
		lumberJackLogger := &lumberjack.Logger{
			Filename:   filename,
			MaxSize:    maxSize, // megabytes
			MaxAge:     maxAge,  // days
			MaxBackups: 0,
			Compress:   true,
		}
		writeSyncer = zapcore.AddSync(lumberJackLogger)
	} else {
		writeSyncer = zapcore.AddSync(os.Stdout)
	}

	core := zapcore.NewCore(encoder, writeSyncer, zap.DebugLevel)
	return zap.New(core)
}

func Sync() {
	if Audit != nil {
		_ = Audit.Sync()
	}
	if Telemetry != nil {
		_ = Telemetry.Sync()
	}
	if Error != nil {
		_ = Error.Sync()
	}
}
