package store

import (
	"encoding/json"
	"sync"
)

// Store is a simple mutex protected key-value store
type LocalStore struct {
	mu     sync.RWMutex
	values map[string]string
}

// NewStore returns a new instance of a store
func NewLocalStore() *LocalStore {
	return &LocalStore{
		values: make(map[string]string),
	}
}

// Get retrieves a value from the store
func (s *LocalStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[key]
	return v, ok
}

// Set sets a value in the store
func (s *LocalStore) Set(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}

// Delete deletes a value from the store
func (s *LocalStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
}

// GetSnapshot returns a json representation of the values in the store.
func (s *LocalStore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.values)
}

// SetSnapshot replaces the underlying keyValue mappings with a new set
func (s *LocalStore) SetSnapshot(snapshot map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = snapshot
}
