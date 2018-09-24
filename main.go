package main

import (
	"github.com/TomStuart92/asfalis/pkg/http"
	"github.com/TomStuart92/asfalis/pkg/rpc"
	"github.com/TomStuart92/asfalis/pkg/store"
	"github.com/TomStuart92/asfalis/raft"
	"github.com/TomStuart92/asfalis/raft/raftpb"
)

const (
	rpcPort  = ":50051"
	httpPort = 3000
)

func main() {

	storage := raft.NewMemoryStorage()

	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	peers := []raft.Peer{{ID: 0x01}}
	raft.StartNode(c, peers)

	proposeChannel := make(chan string)
	defer close(proposeChannel)

	commitChannel := make(chan *string)
	defer close(commitChannel)

	store := store.New(proposeChannel, commitChannel)

	go http.ServeHTTP(httpPort, store)
	go rpc.ServeRPC(rpcPort, store)

	for {
		select {
		case <-s.Ticker:
			n.Tick()
		case rd := <-s.Node.Ready():
			saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					s.Node.ApplyConfChange(cc)
				}
			}
			s.Node.Advance()
		case <-s.done:
			return
		}
	}

}
