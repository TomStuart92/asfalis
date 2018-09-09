package store

import (
	"testing"

	"github.com/TomStuart92/asfalis/pkg/store"
)

func TestSetOnNewRecord(t *testing.T) {
	store := store.New()
	ok, _ := store.Set("key", "value")
	if !ok {
		t.Errorf("Error Setting Value")
	}
}

func TestSetOnExistingRecord(t *testing.T) {
	store := store.New()
	store.Set("key", "value")
	ok, _ := store.Set("key", "value")
	if !ok {
		t.Errorf("Error Setting Value")
	}
}

func TestGetOnNoRecord(t *testing.T) {
	store := store.New()
	_, err := store.Get("key")
	if err == nil {
		t.Errorf("No Error Getting Unset Value")
	}
}

func TestGetOnExistingRecord(t *testing.T) {
	store := store.New()
	store.Set("key", "value")
	value, err := store.Get("key")
	if err != nil {
		t.Errorf("Error Getting Value")
	}
	if value != "value" {
		t.Errorf("Retrieved Wrong Value")
	}
}

func TestGetOnUpdatedRecord(t *testing.T) {
	store := store.New()
	store.Set("key", "value")
	value, err := store.Get("key")
	if err != nil {
		t.Errorf("Error Getting Value")
	}
	if value != "value" {
		t.Errorf("Retrieved Wrong Value")
	}
	store.Set("key", "updatedValue")
	value, err = store.Get("key")
	if err != nil {
		t.Errorf("Error Getting Value")
	}
	if value != "updatedValue" {
		t.Errorf("Retrieved Wrong Value")
	}
}

func TestDeleteOnExistingRecord(t *testing.T) {
	store := store.New()
	store.Set("key", "value")
	value, err := store.Get("key")
	if err != nil {
		t.Errorf("Error Getting Value")
	}
	if value != "value" {
		t.Errorf("Retrieved Wrong Value")
	}
	store.Delete("key")
	value, err = store.Get("key")
	if err == nil {
		t.Errorf("Failed to Delete Value")
	}
}
