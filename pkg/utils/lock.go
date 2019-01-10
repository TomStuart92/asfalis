package utils

import (
	"errors"
	"os"
	"syscall"
)

var (
	// ErrLocked is returned if a file is locked
	ErrLocked = errors.New("fileutil: file already locked")
)

type LockedFile struct{ *os.File }

func flockTryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			err = ErrLocked
		}
		return nil, err
	}
	return &LockedFile{f}, nil
}

func flockLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{f}, err
}

func TryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return flockTryLockFile(path, flag, perm)
}

func LockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return flockLockFile(path, flag, perm)
}
