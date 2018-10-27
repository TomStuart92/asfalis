package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
)

var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
func Exist(dir string) bool {
	names, err := ReadDir(dir, WithExt(".wal"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
			} else {
				plog.Panicf("parse correct name should never fail: %v", err)
			}
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(lg *zap.Logger, names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWALName(name)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
			} else {
				plog.Panicf("parse correct name should never fail: %v", err)
			}
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	names, err := ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkWalNames(lg, names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

func checkWalNames(lg *zap.Logger, names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				if lg != nil {
					lg.Warn(
						"ignored file in WAL directory",
						zap.String("path", name),
					)
				} else {
					plog.Warningf("ignored file %v in wal", name)
				}
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

// ReadDirOp represents an read-directory operation.
type ReadDirOp struct {
	ext string
}

// ReadDirOption configures archiver operations.
type ReadDirOption func(*ReadDirOp)

// WithExt filters file names by their extensions.
// (e.g. WithExt(".wal") to list only WAL files)
func WithExt(ext string) ReadDirOption {
	return func(op *ReadDirOp) { op.ext = ext }
}

func (op *ReadDirOp) applyOpts(opts []ReadDirOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// ReadDir returns the filenames in the given directory in sorted order.
func ReadDir(d string, opts ...ReadDirOption) ([]string, error) {
	op := &ReadDirOp{}
	op.applyOpts(opts)

	dir, err := os.Open(d)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	if op.ext != "" {
		tss := make([]string, 0)
		for _, v := range names {
			if filepath.Ext(v) == op.ext {
				tss = append(tss, v)
			}
		}
		names = tss
	}
	return names, nil
}
