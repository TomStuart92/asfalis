package main

import (
	"fmt"

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
	proposeChannel, commitChannel := raft.NewRaftNode()
	store := store.New(proposeChannel, commitChannel)
	go http.ServeHTTP(httpPort, store)
	go rpc.ServeRPC(rpcPort, store)
	func() {
		for {
		}
	}()
}
