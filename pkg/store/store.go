package store

import (
	"bytes"
	"encoding/gob"
	"log"
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
	proposeChannel chan<- string
	Values         map[string]*Record
}

// Set a value in the store
func (store *Store) Set(key string, value string) (result Record, err error) {
	record := Record{key, value}
	store.propose(record)
	return record, nil
}

// Get a value from the store
func (store *Store) Get(key string) (value string, ok bool) {
	store.Lock()
	record, present := store.Values[key]
	store.Unlock()
	return record.Value, present
}

// Delete a value from the store
func (store *Store) Delete(key string) (ok bool, err error) {
	record := Record{Key: key, Value: ""}
	store.propose(record)
	return true, nil
}

func (store *Store) propose(record Record) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(record); err != nil {
		log.Fatal(err)
	}
	store.proposeChannel <- buffer.String()
}

func (store *Store) readCommits(commitChannel <-chan *string, errorChannel <-chan error) {
	for data := range commitChannel {
		if data == nil {
			log.Printf("Finished Processing Updates")
			continue
		}

		var record Record
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&record); err != nil {
			log.Fatalf("Could Not Decode Message (%v)", err)
		}
		store.Lock()
		store.Values[record.Key] = &record
		store.Unlock()
	}
	if err, ok := <-errorChannel; ok {
		log.Fatal(err)
	}
}

// New returns a new store instance
func New(proposeChannel chan<- string, commitChannel chan<- *string) (store *Store) {
	return &Store{&sync.Mutex{}, proposeChannel, make(map[string]*Record)}
}
