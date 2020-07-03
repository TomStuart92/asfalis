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
	"github.com/TomStuart92/asfalis/pkg/snap"
	"github.com/TomStuart92/asfalis/pkg/transport"
	"github.com/TomStuart92/asfalis/pkg/wal"
	"github.com/TomStuart92/asfalis/pkg/wal/walpb"
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
	waldir      string                 // path to WAL directory
	snapdir     string                 // path to snapshot directory
	getSnapshot func() ([]byte, error) // returns a snapshot of application layer data
	lastIndex   uint64                 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

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
func NewRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

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
		waldir:      fmt.Sprintf("/tmp/raftexample-%v", time.Now()),
		snapdir:     fmt.Sprintf("/tmp/raftexample-%v-snap",  time.Now()),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go easyraft.startRaft()
	return commitC, errorC, easyraft.snapshotterReady
}


func (easyRaft *easyRaft) startRaft() {
	if !fileutil.Exist(easyRaft.snapdir) {
		if err := os.Mkdir(easyRaft.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}

	// create a new snapshotter
	easyRaft.snapshotter = snap.New(easyRaft.snapdir)
	easyRaft.snapshotterReady <- easyRaft.snapshotter

	// 
	oldwal := wal.Exist(easyRaft.waldir)
	easyRaft.wal = easyRaft.replayWAL()
	
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

	
	if oldwal {
		easyRaft.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if easyRaft.join {
			startPeers = nil
		}
		easyRaft.node = raft.StartNode(c, startPeers)
	}

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



func (rc *easyRaft) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
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

func (rc *easyRaft) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *easyRaft) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *easyRaft) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		err = rc.raftStorage.ApplySnapshot(*snapshot)
		if err != nil {
			log.Printf("Failed to Apply Snaphshot: %v", err)
		}
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
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

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *easyRaft) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (easyRaft *easyRaft) serveChannels() {
	snap, err := easyRaft.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	easyRaft.confState = snap.Metadata.ConfState
	easyRaft.snapshotIndex = snap.Metadata.Index
	easyRaft.appliedIndex = snap.Metadata.Index

	defer easyRaft.wal.Close()

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
					log.Infof("Processing proposed change: %v",prop)
					// blocks until accepted by raft state machine
					easyRaft.node.Propose(context.TODO(), []byte(prop))
					log.Infof("Processed proposed change: %v",prop)
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

		// store raft entries to wal, then publish over commit channel
		case rd := <-easyRaft.node.Ready():
			log.Info("Node ready")
			easyRaft.wal.Save(rd.HardState, rd.Entries)

			log.Infof("Saved %d entries to WAL", len(rd.Entries))
			if !raft.IsEmptySnap(rd.Snapshot) {
				easyRaft.saveSnap(rd.Snapshot)
				easyRaft.raftStorage.ApplySnapshot(rd.Snapshot)
				easyRaft.publishSnapshot(rd.Snapshot)
			}
			easyRaft.raftStorage.Append(rd.Entries)
			easyRaft.transport.Send(rd.Messages)


			if ok := easyRaft.publishEntries(easyRaft.entriesToApply(rd.CommittedEntries)); !ok {
				easyRaft.stop()
				return
			}
			easyRaft.maybeTriggerSnapshot()
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
