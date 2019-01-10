package logger

import (
	"bufio"
	"io"
	"log"
	"os"
)

// Logger is a simple wrapper around the default go Logger.
// Its intended as a placeholder to support a custom implementation at a future point.
type Logger struct {
	Prefix string
	*log.Logger
}

// NewLogger returns a new instance of the Logger class
func NewLogger(output io.Writer, prefix string) *Logger {
	logger := Logger{
		Prefix: prefix,
		Logger: log.New(output, prefix, 7),
	}
	return &logger
}

// NewStdoutLogger is a wrapper around NewLogger which defaults output to stdout.
func NewStdoutLogger(prefix string) *Logger {
	logger := Logger{
		Prefix: prefix,
		Logger: log.New(bufio.NewWriter(os.Stdout), prefix, 7),
	}
	return &logger
}

func (l *Logger) Debug(v ...interface{}) {
	l.Print(v)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.Printf(format, v)
}

func (l *Logger) Info(v ...interface{}) {
	l.Print(v)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.Printf(format, v)
}

func (l *Logger) Warning(v ...interface{}) {
	l.Print(v)
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	l.Printf(format, v)
}

func (l *Logger) Error(v ...interface{}) {
	l.Print(v)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.Printf(format, v)
}
