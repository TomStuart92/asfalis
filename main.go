package main

import (
	"flag"
	"strings"

	"github.com/TomStuart92/asfalis/pkg/api/http"
	"github.com/TomStuart92/asfalis/pkg/api/rpc"
	"github.com/TomStuart92/asfalis/pkg/easyRaft"
	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/store"
)

const (
	rpcPort  = ":50051"
	httpPort = 3000
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var valueStore *store.Store
	getSnapshot := func() ([]byte, error) { return valueStore.GetSnapshot() }

	commitC := easyraft.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	valueStore = store.New(proposeC, commitC)

	go rpc.ServeRPC(rpcPort, valueStore)
	http.ServeHTTP(httpPort, valueStore)
}
