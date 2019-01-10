package logger

import (
	"bytes"
	"testing"
)

func TestLogToCustomWriter(t *testing.T) {
	b := bytes.Buffer{}
	logger := NewLogger(&b, "PREFIX:")
	logger.Print("This is a log message")
	if b.Len() != 56 {
		t.Errorf("Log Message Length incorrect, expected 56 got %d", b.Len())
	}
}
