package easyraft

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/raft"
	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/transport"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
)

var log *logger.Logger = logger.NewStdoutLogger("EASYRAFT: ")

// A key-value stream backed by raft
type easyRaft struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          int                    // client ID for raft session
	peers       []string               // raft peer URLs
	join        bool                   // node is joining an existing cluster
	snapdir     string                 // path to snapshot directory
	getSnapshot func() ([]byte, error) // returns a snapshot of application layer data
	lastIndex   uint64                 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	snapCount uint64
	transport *transport.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(id int, peers []string, join bool, proposeC <-chan string) (<-chan *string, <-chan error) {

	confChangeC := make(chan raftpb.ConfChange)
	commitC := make(chan *string)
	errorC := make(chan error)

	log.Info("Setup channels")

	easyraft := &easyRaft{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		snapdir:     fmt.Sprintf("/tmp/raftexample-%v-snap", time.Now()),
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
	}
	go easyraft.startRaft()
	return commitC, errorC
}

func (easyRaft *easyRaft) startRaft() {
	if !fileutil.Exist(easyRaft.snapdir) {
		if err := os.Mkdir(easyRaft.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}

	easyRaft.raftStorage = raft.NewMemoryStorage()

	rpeers := make([]raft.Peer, len(easyRaft.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
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

	startPeers := rpeers
	if easyRaft.join {
		startPeers = nil
	}
	easyRaft.node = raft.StartNode(c, startPeers)

	easyRaft.transport = &transport.Transport{
		ID:        types.ID(easyRaft.id),
		ClusterID: 0x1000,
		Raft:      easyRaft,
		ErrorC:    make(chan error),
	}

	easyRaft.transport.Start()
	for i := range easyRaft.peers {
		if i+1 != easyRaft.id {
			easyRaft.transport.AddPeer(types.ID(i+1), []string{easyRaft.peers[i]})
		}
	}
	go easyRaft.serveRaft()
	go easyRaft.serveChannels()
}

func (rc *easyRaft) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *easyRaft) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			log.Infof("Writing data into commit channel %s", s)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			err := cc.Unmarshal(ents[i].Data)
			if err != nil {
				log.Printf("Unmarshal failed: %v", err)
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *easyRaft) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

// stop closes http, closes all channels, and stops raft.
func (rc *easyRaft) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *easyRaft) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *easyRaft) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

func (easyRaft *easyRaft) serveChannels() {
	snap, err := easyRaft.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	easyRaft.confState = snap.Metadata.ConfState
	easyRaft.snapshotIndex = snap.Metadata.Index
	easyRaft.appliedIndex = snap.Metadata.Index

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
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
				log.Infof("Received config change: %v", cc)
				if !ok {
					easyRaft.confChangeC = nil
				} else {
					log.Infof("Processing config change: %v", cc)
					confChangeCount++
					cc.ID = confChangeCount
					easyRaft.node.ProposeConfChange(context.TODO(), cc)
					log.Infof("Processed config change: %v", cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(easyRaft.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			easyRaft.node.Tick()

		case rd := <-easyRaft.node.Ready():
			easyRaft.raftStorage.Append(rd.Entries)
			easyRaft.transport.Send(rd.Messages)

			if ok := easyRaft.publishEntries(
				easyRaft.entriesToApply(rd.CommittedEntries),
			); !ok {
				easyRaft.stop()
				return
			}
			easyRaft.node.Advance()

		case err := <-easyRaft.transport.ErrorC:
			easyRaft.writeError(err)
			return

		case <-easyRaft.stopc:
			easyRaft.stop()
			return
		}
	}
}

func (rc *easyRaft) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen transport (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve transport (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *easyRaft) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *easyRaft) IsIDRemoved(id uint64) bool                           { return false }
func (rc *easyRaft) ReportUnreachable(id uint64)                          {}
func (rc *easyRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
