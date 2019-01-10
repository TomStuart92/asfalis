package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/TomStuart92/asfalis/pkg/utils"
)

// filePipeline pipelines allocating disk space
type filePipeline struct {
	// dir to put files
	dir string
	// size of files to make, in bytes
	size int64
	// count number of files generated
	count int

	filec chan *utils.LockedFile
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		dir:   dir,
		size:  fileSize,
		filec: make(chan *utils.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *utils.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *utils.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	if f, err = utils.LockFile(fpath, os.O_CREATE|os.O_WRONLY, utils.PrivateFileMode); err != nil {
		return nil, err
	}
	if err = utils.Preallocate(f.File, fp.size, true); err != nil {
		log.Errorf("failed to allocate space when creating new wal file (%v)", err)
		f.Close()
		return nil, err
	}
	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
