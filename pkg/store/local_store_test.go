package store

import (
	"testing"
)

const (
	key1   string = "key1"
	key2   string = "key2"
	value1 string = "value1"
	value2 string = "value2"
)

func TestNoRecord(t *testing.T) {
	s := NewLocalStore()
	_, ok := s.Get(key1)
	if ok {
		t.Error("Retrieved Invalid Data")
	}
}

func TestSetOnNewRecord(t *testing.T) {
	s := NewLocalStore()
	s.Set(key1, value1)
	value, ok := s.Get(key1)
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != value1 {
		t.Error("Incorrect Value Retrieved")
	}
}

func TestSetOnUpdatedRecord(t *testing.T) {
	s := NewLocalStore()
	s.Set(key1, value1)
	s.Set(key1, value2)
	value, ok := s.Get(key1)
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != value2 {
		t.Error("Incorrect Value Retrieved")
	}
}

func TestSetOWithMultipleRecord(t *testing.T) {
	s := NewLocalStore()
	s.Set(key1, value1)
	s.Set(key2, value2)
	value, ok := s.Get(key2)
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != value2 {
		t.Error("Incorrect Value Retrieved")
	}
}

func TestDeleteWithMultipleRecord(t *testing.T) {
	s := NewLocalStore()
	s.Set(key1, value1)
	s.Set(key2, value2)
	s.Delete(key1)
	value, ok := s.Get(key2)
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != value2 {
		t.Error("Incorrect Value Retrieved")
	}
	_, ok = s.Get(key1)
	if ok {
		t.Error("Key Not Deleted From Store")
	}
}
