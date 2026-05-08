package retryqueue

import (
	"log/slog"
	"os"
)

// 默认使用 slog，使用者可以通过 WithLogger 注入自定义实现
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// defaultLogger 包装
type defaultLogger struct {
	l *slog.Logger
}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{
		l: slog.New(slog.NewJSONHandler(
			os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			})),
	}
}

func (l *defaultLogger) Info(msg string, args ...any)  { l.l.Info(msg, args...) }
func (l *defaultLogger) Debug(msg string, args ...any) { l.l.Debug(msg, args...) }
func (l *defaultLogger) Warn(msg string, args ...any)  { l.l.Warn(msg, args...) }
func (l *defaultLogger) Error(msg string, args ...any) { l.l.Error(msg, args...) }
