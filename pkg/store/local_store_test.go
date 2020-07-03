package store

import (
	"encoding/json"
	"testing"
)

const (
	key1   string = "key1"
	key2   string = "key2"
	value1 string = "value1"
	value2 string = "value2"
)

func TestNoRecord(t *testing.T) {
	s := NewStore()
	_, ok := s.Get(key1)
	if ok {
		t.Error("Retrieved Invalid Data")
	}
}

func TestSetOnNewRecord(t *testing.T) {
	s := NewStore()
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
	s := NewStore()
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
	s := NewStore()
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
	s := NewStore()
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
	value, ok = s.Get(key1)
	if ok {
		t.Error("Key Not Deleted From Store")
	}
}

func TestGetSnapshot(t *testing.T) {
	s := NewStore()
	s.Set(key1, value1)
	s.Set(key2, value2)
	s.Delete(key1)
	_, err := s.GetSnapshot()
	if err != nil {
		t.Error("Error Getting Snapshot")
	}
}

func TestSetSnapshot(t *testing.T) {
	s1 := NewStore()
	s1.Set(key1, value1)
	snapshot, err := s1.GetSnapshot()
	if err != nil {
		t.Error("Error Getting Snapshot")
	}
	s2 := NewStore()
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		t.Error("Error Unmarshalling Snapshot")
	}
	s2.SetSnapshot(store)
	value, ok := s2.Get(key1)
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != value1 {
		t.Error("Incorrect Value Retrieved")
	}
}
