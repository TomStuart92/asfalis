package store

import (
	"errors"
	"sync"
)

// Record holds wraps a keys value
type Record struct {
	Key   string
	Value string
}

// Store holds key-record Map
type Store struct {
	*sync.Mutex
	Values map[string]*Record
}

// Set a value in the store
func (store Store) Set(key string, value string) (result Record, err error) {
	store.Lock()
	record := Record{key, value}
	store.Values[key] = &record
	store.Unlock()
	return record, nil
}

// Get a value from the store
func (store Store) Get(key string) (result Record, err error) {
	store.Lock()
	record, present := store.Values[key]
	if !present {
		return Record{}, errors.New("Key Not Found")
	}
	store.Unlock()
	return *record, nil
}

// Delete a value from the store
func (store Store) Delete(key string) (ok bool, err error) {
	store.Lock()
	delete(store.Values, key)
	store.Unlock()
	return true, nil
}

// New returns a new store instance
func New() (store Store) {
	return Store{&sync.Mutex{}, make(map[string]*Record)}
}
