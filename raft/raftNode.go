package raft

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/TomStuart92/asfalis/raft/raftpb"
)

func NewRaftNode() (chan<- string, chan<- *string) {

	proposeChannel := make(chan string)
	defer close(proposeChannel)

	commitChannel := make(chan *string)
	defer close(commitChannel)

	ticker := time.NewTicker(100 * time.Millisecond)

	storage := NewMemoryStorage()
	c := &Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	// Set peer list to the other nodes in the cluster.
	// Note that they need to be started separately as well.
	n := StartNode(c, []Peer{{ID: 0x02}, {ID: 0x03}})

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

	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Tick\n")
				n.Tick()
			case rd := <-n.Ready():
				fmt.Println("Looping\n")
				// saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)

				// send(rd.Messages)
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
	}()
	return proposeChannel, commitChannel

}
