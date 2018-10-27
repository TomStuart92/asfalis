package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/TomStuart92/asfalis/pkg/raft"
	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/snap/snappb"
	"github.com/TomStuart92/asfalis/pkg/utils"
)

const snapSuffix = ".snap"

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

// Snapshotter is responsible for persisting the current state of data to drive.
type Snapshotter struct {
	dir string
}

// New returns a new snapshotter with the given dir
func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

// SaveSnap checks if snapshot is empty, and if not writes it to file
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	b := utils.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)
	snap := snappb.Snapshot{Crc: crc, Data: b}
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	spath := filepath.Join(s.dir, fname)

	err = utils.WriteAndSyncFile(spath, d, 0666)
	if err != nil {
		fmt.Printf("failed to write a snap file")
		rerr := os.Remove(spath)
		if rerr != nil {
			fmt.Printf("failed to remove broken snapshot file %s", spath)
		}
		return err
	}
	return nil
}

func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := Read(fpath)
	if err != nil {
		brokenPath := fpath + ".broken"
		fmt.Printf("failed to read a snap file")

		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			fmt.Printf("cannot rename broken snapshot file %v to %v: %v", fpath, brokenPath, rerr)
		} else {
			fmt.Printf("renamed to a broken snap file")
		}
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(snapname string) (*raftpb.Snapshot, error) {
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		fmt.Printf("cannot read file %v: %v", snapname, err)
		return nil, err
	}

	if len(b) == 0 {
		fmt.Printf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		fmt.Printf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		fmt.Printf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		fmt.Printf("snap file is corrupt")
		return nil, ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		fmt.Printf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				fmt.Printf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}
