package store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"

	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/snap"
)

type Snapper interface {
	Load() (*raftpb.Snapshot, error)
}

// DistributedStore is a key-value store which is designed to propose changes
// and read commits from a pair of channels. These channels are usually backed
// by an implementation of the Raft algorithm.
type DistributedStore struct {
	proposeC    chan<- string
	commitC     <-chan *string
	errorC      <-chan error
	store       *Store
	snapshotter Snapper
}

// keyValue is an internal representation of a key-value pair used to send such
// pairs into and out of the stores channels.
type keyValue struct {
	Key   string
	Value string
}

// NewDistributedStore creates a new instance of a DistributedStore
func NewDistributedStore(snapshotter Snapper, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *DistributedStore {
	store := NewStore()
	s := &DistributedStore{proposeC, commitC, errorC, store, snapshotter}
	s.readCommits()
	go s.readCommits()
	return s
}

// Lookup delegates a request through to the underlying store instance
func (s *DistributedStore) Lookup(key string) (string, bool) {
	return s.store.Get(key)
}

// Propose proposed a change to the store through the proposeC channel
func (s *DistributedStore) Propose(key string, value string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(keyValue{Key: key, Value: value}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

// GetSnapshot delegates a request through to the underlying store instance
func (s *DistributedStore) GetSnapshot() ([]byte, error) {
	return s.store.GetSnapshot()
}

// readCommits loops through commits in the commitC channel, and applies them as
// appropriate
func (s *DistributedStore) readCommits() {
	for data := range s.commitC {
		if data == nil {
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var kv keyValue
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&kv); err != nil {
			log.Fatalf("Failed to decode message (%v)", err)
		}
		s.store.Set(kv.Key, kv.Value)
	}
	if err, ok := <-s.errorC; ok {
		log.Fatal(err)
	}
}

// recoverFromSnapshot allows for the recovery of data from a snapshot.
func (s *DistributedStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.store.SetSnapshot(store)
	return nil
}
