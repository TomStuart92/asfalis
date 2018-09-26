package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TomStuart92/asfalis/pkg/http"
	"github.com/TomStuart92/asfalis/pkg/rpc"
	"github.com/TomStuart92/asfalis/pkg/store"
	"github.com/TomStuart92/asfalis/raft"
	pb "github.com/TomStuart92/asfalis/raft/raftpb"
)

const (
	rpcPort  = ":50051"
	httpPort = 3000
)

func saveToStorage(hardState pb.HardState, entries []pb.Entry, snapshot pb.Snapshot) {
	fmt.Println(hardState)
}

func send(messages []pb.Message) {
	for message := range messages {
		fmt.Println(message)
	}
}

func main() {
	proposeChannel := make(chan string)
	defer close(proposeChannel)

	commitChannel := make(chan *string)
	defer close(commitChannel)

	store := store.New(proposeChannel, commitChannel)

	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	// Set peer list to the other nodes in the cluster.
	// Note that they need to be started separately as well.
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

	go http.ServeHTTP(httpPort, store)
	go rpc.ServeRPC(rpcPort, store)

	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for {
			select {
			case proposal := <-proposeChannel:
				fmt.Println("PROPOSAL")
				// blocks until accepted by raft state machine
				err := n.Propose(context.TODO(), []byte(proposal))
				fmt.Println("PROPOSAL ACCEPTED")
				if err != nil {
					log.Fatalf("Failed to propose: %v", err)
				}
			}
		}
	}()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Tick\n")
			n.Tick()
		case rd := <-n.Ready():
			fmt.Println("Looping\n")
			// saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)

			send(rd.Messages)
			// 		// if !raft.IsEmptySnap(rd.Snapshot) {
			// 		// 	// processSnapshot(rd.Snapshot)
			// 		// }
			for _, entry := range rd.CommittedEntries {
				if entry.Type == pb.EntryConfChange {
					var cc pb.ConfChange
					cc.Unmarshal(entry.Data)
					n.ApplyConfChange(cc)
				}
			}
			n.Advance()
		}
	}

}
