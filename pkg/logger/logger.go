package logger

import (
	"fmt"
	"io"
	"log"
	"os"
)

var activeLoggers = map[string]bool{
	"api":      true,
	"startup":  true,
	"store":    true,
	"easyraft": true,
}

// Logger is a simple wrapper around the default go Logger.
// Its intended as a placeholder to support a custom implementation at a future point.
type Logger struct {
	service string
	*log.Logger
}

// NewLogger returns a new instance of the Logger class
func NewLogger(output io.Writer, service string) *Logger {
	logger := Logger{
		service: service,
		Logger:  log.New(output, service, 7),
	}
	return &logger
}

// NewStdoutLogger is a wrapper around NewLogger which defaults output to stdout.
func NewStdoutLogger(service string) *Logger {
	logger := Logger{
		service: service,
		Logger:  log.New(os.Stdout, fmt.Sprintf("%s - ", service), 7),
	}
	return &logger
}

func (l *Logger) Debug(v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Print(v...)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Printf(format, v...)
}

func (l *Logger) Info(v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Print(v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Printf(format, v...)
}

func (l *Logger) Warning(v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Print(v...)
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Printf(format, v...)
}

func (l *Logger) Error(v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Print(v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	if !activeLoggers[l.service] {
		return
	}
	l.Printf(format, v...)
}
