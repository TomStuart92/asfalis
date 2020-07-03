package main

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/TomStuart92/asfalis/pkg/api"
	"github.com/TomStuart92/asfalis/pkg/easyraft"
	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/store"
)

var log = logger.NewStdoutLogger("startup")

func main() {
	cluster := flag.String("cluster", "", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	ctx := context.Background()
	proposeChannel := make(chan string)

	// raft provides a commit stream for the proposals from the http api
	commitChannel, errorChannel := easyraft.NewRaftNode(*id, strings.Split(*cluster, ","), *join, proposeChannel)
	log.Info("Started Raft Node")

	kvs := store.NewDistributedStore(proposeChannel, commitChannel)
	log.Info("Created Key-Value Store")

	// the key-value http handler will propose updates to raft
	go api.Serve(kvs, *kvport)
	log.Info("Started API Server")

	err := <-errorChannel
	if err != nil {
		log.Error(err)
		api.Shutdown(ctx)
		os.Exit(1)
	}
}
