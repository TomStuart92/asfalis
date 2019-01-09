package utils

import (
	"os"
	"testing"
)

func TestExistingFileRead(t *testing.T) {
	file := "exist_test.txt"
	data := []byte(file)
	os.Create(file)
	defer os.Remove(file)
	err := WriteAndSyncFile(file, data, os.ModeExclusive)
	if err != nil {
		t.Error("Unexpected Err during file write")
	}
}
