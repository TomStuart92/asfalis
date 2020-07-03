package main

import (
	"flag"
	"strings"

	"github.com/TomStuart92/asfalis/pkg/api"
	"github.com/TomStuart92/asfalis/pkg/easyraft"
	"github.com/TomStuart92/asfalis/pkg/store"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeChannel := make(chan string)
	defer close(proposeChannel)


	// raft provides a commit stream for the proposals from the http api
	var kvs *store.DistributedStore
	commitC, errorC, snapshotterReady := easyraft.NewRaftNode(*id, strings.Split(*cluster, ","), *join, kvs.GetSnapshot, proposeChannel)

	kvs = store.NewDistributedStore(<-snapshotterReady, proposeChannel, commitC, errorC)

	// the key-value http handler will propose updates to raft
	api.ServeHTTP(kvs, *kvport, errorC)
}
