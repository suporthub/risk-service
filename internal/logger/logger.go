package logger

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	AppLog  *slog.Logger
	RiskLog *slog.Logger
)

func Init() {
	isFileLogging := os.Getenv("LOG_TO_FILE") == "true"

	// Ensure local logs directory exists gracefully
	if isFileLogging {
		_ = os.MkdirAll("logs", 0755)
	}

	AppLog = newJSONLogger("system_telemetry", "logs/application.log", 50, 7, isFileLogging)
	RiskLog = newJSONLogger("risk_ledger", "logs/risk-events.log", 50, 14, isFileLogging)
}

func newJSONLogger(streamTag, filename string, maxSize, maxAge int, isFileLogging bool) *slog.Logger {
	var writer io.Writer = os.Stdout

	if isFileLogging {
		rollingFile := newRollingFile(filename, maxSize, maxAge)
		writer = io.MultiWriter(os.Stdout, rollingFile)
	}

	handler := slog.NewJSONHandler(writer, nil).WithAttrs([]slog.Attr{
		slog.String("stream", streamTag),
	})

	return slog.New(handler)
}

func newRollingFile(filename string, maxSize int, maxAge int) io.Writer {
	return &lumberjack.Logger{
		Filename: filename,
		MaxSize:  maxSize, // Megabytes
		MaxAge:   maxAge,  // Days
		Compress: true,
	}
}
