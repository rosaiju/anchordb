//go:build !windows

package lock

import (
	"errors"
	"os"
	"syscall"
)

type handle = *os.File

// acquire uses flock(LOCK_EX|LOCK_NB). flock locks belong to the open file
// description, so a second Acquire in the same process also fails.
func acquire(path string) (handle, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}

func release(f handle) error { return f.Close() }
