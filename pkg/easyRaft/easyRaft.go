package easyraft

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/raft"
	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
)

var log *logger.Logger = logger.NewStdoutLogger("easyraft")

// A key-value stream backed by raft
type easyRaft struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- string            // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id        int      // client ID for raft session
	peers     []string // raft peer URLs
	join      bool     // node is joining an existing cluster
	snapdir   string   // path to snapshot directory
	lastIndex uint64   // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	transport *transport.Transport
}

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel.
func NewRaftNode(id int, peers []string, join bool, proposeC <-chan string) (<-chan string, <-chan error) {
	confChangeC := make(chan raftpb.ConfChange)
	commitC := make(chan string)
	errorC := make(chan error)

	easyraft := &easyRaft{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		snapdir:     fmt.Sprintf("/tmp/raftexample-%v-snap", time.Now()),
	}
	go easyraft.startRaft()
	return commitC, errorC
}

// startRaft sets up the various parts
func (easyRaft *easyRaft) startRaft() {
	// Create snapshot directory
	if err := os.MkdirAll(easyRaft.snapdir, 0750); err != nil {
		log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
	}

	// Init in-memory storage for raft
	easyRaft.raftStorage = raft.NewMemoryStorage()

	// Init set of peers
	peers := make([]raft.Peer, len(easyRaft.peers))
	for i := range peers {
		peers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := &raft.Config{
		ID:                        uint64(easyRaft.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   easyRaft.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	startPeers := peers
	if easyRaft.join {
		startPeers = nil
	}

	// Start raft node
	easyRaft.node = raft.StartNode(c, startPeers)

	easyRaft.transport = &transport.Transport{
		ID:        types.ID(easyRaft.id),
		ClusterID: 0x1000,
		Raft:      easyRaft,
		ErrorC:    make(chan error),
	}

	// start transport + add peers
	easyRaft.transport.Start()
	for i := range easyRaft.peers {
		if i+1 != easyRaft.id {
			easyRaft.transport.AddPeer(types.ID(i+1), []string{easyRaft.peers[i]})
		}
	}

	// serve raft + process channels.
	go easyRaft.serveRaft()
	go easyRaft.serveChannels()
}

// serveRaft starts the TCP server over which the raft nodes communicate
func (rc *easyRaft) serveRaft() {
	srv := http.Server{
		Handler: rc.transport.Handler(),
	}

	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("Failed parsing URL: %v", err)
	}

	ln, err := net.Listen("tcp", url.Host)
	if err != nil {
		log.Fatalf("Failed to create listener: %v", err)
	}

	log.Infof("Starting raft tcp server on tcp:%s", url.Host)
	if err := srv.Serve(ln); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// serveChannels is the workhorse which processes updates from different channels
func (easyRaft *easyRaft) serveChannels() {
	snap, err := easyRaft.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	easyRaft.confState = snap.Metadata.ConfState
	easyRaft.snapshotIndex = snap.Metadata.Index
	easyRaft.appliedIndex = snap.Metadata.Index

	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	// handle proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for easyRaft.proposeC != nil && easyRaft.confChangeC != nil {

			select {
			case prop, ok := <-easyRaft.proposeC:
				log.Infof("Received proposed change: %v", prop)
				if !ok {
					easyRaft.proposeC = nil
				} else {
					log.Infof("Processing proposed change: %v", prop)
					// blocks until accepted by raft state machine
					easyRaft.node.Propose(context.TODO(), []byte(prop))
					log.Infof("Processed proposed change: %v", prop)
				}

			case cc, ok := <-easyRaft.confChangeC:
				log.Infof("Received proposed config change: %v", cc)
				if !ok {
					easyRaft.confChangeC = nil
				} else {
					log.Infof("Processing proposed config change: %v", cc)
					confChangeCount++
					cc.ID = confChangeCount
					easyRaft.node.ProposeConfChange(context.TODO(), cc)
					log.Infof("Processed proposed config change: %v", cc)
				}
			}
		}
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			log.Debug("Ticking node forward")
			easyRaft.node.Tick()

		case rd := <-easyRaft.node.Ready():
			log.Infof("Received %d entries from Raft", len(rd.Entries))
			easyRaft.raftStorage.Append(rd.Entries)

			log.Infof("Received %d message(s) to send to peers", len(rd.Messages))
			for _, message := range rd.Messages {
				log.Debugf("Sending message type: %s", message.Type)
			}
			easyRaft.transport.Send(rd.Messages)

			if ok := easyRaft.publishEntries(
				easyRaft.entriesToApply(rd.CommittedEntries),
			); !ok {
				return
			}

			log.Debug("Advancing node forward")
			easyRaft.node.Advance()

		case err := <-easyRaft.transport.ErrorC:
			log.Errorf("Received error from transport: %v", err)
			close(easyRaft.commitC)
			easyRaft.errorC <- err
			close(easyRaft.errorC)
			easyRaft.node.Stop()
			return
		}
	}
}

func (easyRaft *easyRaft) entriesToApply(entries []raftpb.Entry) (entriesToApply []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}

	// check that the newest entry to apply is greater than the current applied index
	firstIdx := entries[0].Index
	if firstIdx > easyRaft.appliedIndex+1 {
		log.Fatalf("First index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, easyRaft.appliedIndex)
	}

	// only apply entries from applied index onwards to prevent repeats
	if easyRaft.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		entriesToApply = entries[easyRaft.appliedIndex-firstIdx+1:]
	}

	log.Debugf("There is %d entries to apply", len(entriesToApply))
	return entriesToApply
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (easyRaft *easyRaft) publishEntries(entries []raftpb.Entry) bool {

	for i := range entries {
		log.Debugf("Handling entry %d with type %s", i+1, entries[i].Type)

		switch entries[i].Type {

		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				log.Info("Entry was empty, ignoring...")
				break
			}

			s := string(entries[i].Data)

			log.Infof("Writing data from entry into commit channel %s", s)
			easyRaft.commitC <- s

		case raftpb.EntryConfChange:

			var cc raftpb.ConfChange
			err := cc.Unmarshal(entries[i].Data)
			if err != nil {
				log.Printf("Unmarshal failed: %v", err)
			}

			// let the node handle the config change
			easyRaft.confState = *easyRaft.node.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					log.Info("Config change message, involved new node. Informing transport.")
					easyRaft.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				} else {
					log.Info("Config change message was for me joining, ignoring...")
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(easyRaft.id) {
					log.Info("I've been removed from the cluster! Shutting down.")
					return false
				}
				log.Info("Config change message, involved removing node. Informing transport.")
				easyRaft.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		log.Infof("Updating applied index from %d => %d", easyRaft.appliedIndex, entries[i].Index)
		easyRaft.appliedIndex = entries[i].Index

		// special nil commit to signal replay has finished
		if entries[i].Index == easyRaft.lastIndex {
			easyRaft.commitC <- ""
		}
	}
	return true
}

func (rc *easyRaft) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *easyRaft) IsIDRemoved(id uint64) bool                           { return false }
func (rc *easyRaft) ReportUnreachable(id uint64)                          {}
func (rc *easyRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
