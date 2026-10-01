// Package lock provides an exclusive, process-level lock on a file. The OS
// releases it automatically when the process exits, so a crash never leaves a
// stale lock behind.
package lock

import "errors"

// ErrLocked is returned when another holder already owns the lock.
var ErrLocked = errors.New("lock: already locked by another process or handle")

// Lock is a held lock. Release it with Unlock.
type Lock struct{ h handle }

// Acquire takes the lock on path without blocking, creating the file if needed.
func Acquire(path string) (*Lock, error) {
	h, err := acquire(path)
	if err != nil {
		return nil, err
	}
	return &Lock{h: h}, nil
}

// Unlock releases the lock.
func (l *Lock) Unlock() error { return release(l.h) }
