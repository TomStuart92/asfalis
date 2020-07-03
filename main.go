package main

import (
	"flag"
	"strings"

	"github.com/TomStuart92/asfalis/pkg/api"
	"github.com/TomStuart92/asfalis/pkg/easyraft"
	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/store"
)

var log = logger.NewStdoutLogger("main.go: ")

func main() {
	cluster := flag.String("cluster", "", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeChannel := make(chan string)

	// raft provides a commit stream for the proposals from the http api
	var kvs *store.DistributedStore
	commitChannel, errorChannel, snapshotterReady := easyraft.NewRaftNode(*id, strings.Split(*cluster, ","), *join, kvs.GetSnapshot, proposeChannel)

	kvs = store.NewDistributedStore(<-snapshotterReady, proposeChannel, commitChannel)

	// the key-value http handler will propose updates to raft
	api.Serve(kvs, *kvport)

	select {
		case err := <- errorChannel:
			// api.Shutdown()
			log.Fatal(err)

	}
}
