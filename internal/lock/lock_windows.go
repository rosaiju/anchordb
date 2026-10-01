//go:build windows

package lock

import (
	"errors"
	"syscall"
)

type handle = syscall.Handle

const errorSharingViolation syscall.Errno = 32

// acquire opens the file with share mode 0: while the handle is open, every
// other CreateFile on it (from any process) fails with a sharing violation.
func acquire(path string) (handle, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil,
		syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		if errors.Is(err, errorSharingViolation) {
			return 0, ErrLocked
		}
		return 0, err
	}
	return h, nil
}

func release(h handle) error { return syscall.CloseHandle(h) }
