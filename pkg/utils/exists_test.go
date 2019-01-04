package utils

import (
	"os"
	"testing"
)

func TestExistingFile(t *testing.T) {
	file := "exist_test.txt"
	os.Create(file)
	defer os.Remove(file)
	ok := Exist(file)
	if !ok {
		t.Error("Expected File to Exist")
	}
}

func TestNonExistingFile(t *testing.T) {
	file := "not_exist_test.txt"
	ok := Exist(file)
	if ok {
		t.Error("Expected File to Not Exist")
	}
}
