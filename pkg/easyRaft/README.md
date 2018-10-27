# Easyraft package

The easyraft package exists to abstract the underlying raft algorithm from the application. It brings together the WAL, Transport, Snap and Raft packages and provides a simple channel based interface for the user. 


### Usage

The users provides config, a function to return a snapshot of the application level data, and a proposal channel. In return they recieve a commitChannel, errorChannel and a channel for snapshots. The user is left responsible for the application level interpretation of messages from these channels. The raft algorithm will ensure consesus between nodes.

```golang
func NewRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter)
```

An example implementation of the application level logic is seen in the distributed store package:
```golang
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
		if kv.Value == "" {
			s.store.Delete(kv.Key)
		} else {
			s.store.Set(kv.Key, kv.Value)
		}
	}
	if err, ok := <-s.errorC; ok {
		log.Fatal(err)
	}
}
```