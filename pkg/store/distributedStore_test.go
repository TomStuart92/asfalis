package store

import (
	"testing"
	"time"

	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
)

type snapStub struct{}

func (s snapStub) Load() (*raftpb.Snapshot, error) {
	return nil, nil
}

func replayProposals(proposeC <-chan string, commitC chan<- *string) {
	for data := range proposeC {
		commitC <- &data
	}
}

func TestNoRecordOnLookup(t *testing.T) {
	snapshotter := snapStub{}
	proposeC := make(chan string, 1)
	commitC := make(chan *string, 1)
	errorC := make(chan error, 1)
	d := NewDistributedStore(snapshotter, proposeC, commitC, errorC)
	_, ok := d.Lookup("key")
	if ok {
		t.Error("Retrieved Invalid Data")
	}
}

func TestPropose(t *testing.T) {
	snapshotter := snapStub{}
	proposeC := make(chan string, 1)
	commitC := make(chan *string, 1)
	errorC := make(chan error, 1)
	d := NewDistributedStore(snapshotter, proposeC, commitC, errorC)
	d.Propose("key", "value")
	select {
	case _ = <-proposeC:
		return
	default:
		t.Error("No Proposal Recieved")
	}
}

func TestCommit(t *testing.T) {
	snapshotter := snapStub{}
	proposeC := make(chan string)
	commitC := make(chan *string)
	errorC := make(chan error)
	d := NewDistributedStore(snapshotter, proposeC, commitC, errorC)

	// replay proposals directly into commits
	go replayProposals(proposeC, commitC)

	d.Propose("key", "value")

	// allow store time to set key. (eventually consistent)
	time.Sleep(2 * time.Second)
	value, ok := d.Lookup("key")
	if !ok {
		t.Error("Could Not Retrieve Key From Store")
	}
	if value != "value" {
		t.Error("Incorrect Value Retrieved")
	}
}

func TestDistributedStoreGetSnapshot(t *testing.T) {
	snapshotter := snapStub{}
	proposeC := make(chan string)
	commitC := make(chan *string)
	errorC := make(chan error)
	d := NewDistributedStore(snapshotter, proposeC, commitC, errorC)

	// replay proposals directly into commits
	go replayProposals(proposeC, commitC)

	d.Propose("key", "value")
	// allow store time to set key. (eventually consistent)
	time.Sleep(2 * time.Second)
	_, err := d.GetSnapshot()
	if err != nil {
		t.Error("Error Getting Snapshot")
	}
}
