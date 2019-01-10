package wal

import (
	"io"
	"os"
	"path/filepath"

	"github.com/TomStuart92/asfalis/pkg/wal/walpb"
	"go.etcd.io/etcd/pkg/fileutil"
)

// Repair tries to repair ErrUnexpectedEOF in the
// last wal file by truncating.
func Repair(dirpath string) bool {
	f, err := openLast(dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	log.Infof("repairing %v", f.Name())

	rec := &walpb.Record{}
	decoder := newDecoder(f)
	for {
		lastOffset := decoder.lastOffset()
		err := decoder.decode(rec)
		switch err {
		case nil:
			// update crc of the decoder when necessary
			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				// current crc of decoder must match the crc of the record.
				// do no need to match 0 crc, since the decoder is a new one at this case.
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}
				decoder.updateCRC(rec.Crc)
			}
			continue

		case io.EOF:
			log.Info("repaired %s", f.Name())
			return true

		case io.ErrUnexpectedEOF:
			bf, bferr := os.Create(f.Name() + ".broken")
			if bferr != nil {
				log.Errorf("could not repair %v, failed to create backup file", f.Name())
				return false
			}
			defer bf.Close()

			if _, err = f.Seek(0, io.SeekStart); err != nil {
				log.Errorf("could not repair %v, failed to read file", f.Name())
				return false
			}

			if _, err = io.Copy(bf, f); err != nil {
				log.Errorf("could not repair %v, failed to copy file", f.Name())
				return false
			}

			if err = f.Truncate(lastOffset); err != nil {
				log.Errorf("could not repair %v, failed to truncate file", f.Name())
				return false
			}

			if err = f.Sync(); err != nil {
				log.Errorf("could not repair %v, failed to sync file", f.Name())
				return false
			}

			log.Infof("repaired %s", f.Name())

			return true

		default:
			log.Errorf("could not repair error (%v)", err)
			return false
		}
	}
}

// openLast opens the last wal file for read and write.
func openLast(dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWALNames(dirpath)
	if err != nil {
		return nil, err
	}
	last := filepath.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
