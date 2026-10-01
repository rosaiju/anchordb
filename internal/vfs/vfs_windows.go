//go:build windows

package vfs

import (
	"os"
	"syscall"
	"unsafe"
)

var procMoveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

const (
	movefileReplaceExisting = 0x1
	movefileWriteThrough    = 0x8
)

// rename uses MoveFileExW with MOVEFILE_WRITE_THROUGH so the call does not
// return until the rename has been flushed to disk. os.Rename omits that flag.
func rename(oldpath, newpath string) error {
	from, err := syscall.UTF16PtrFromString(oldpath)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(newpath)
	if err != nil {
		return err
	}
	r, _, e := procMoveFileExW.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)),
		movefileReplaceExisting|movefileWriteThrough)
	if r == 0 {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: e}
	}
	return nil
}

// syncDir is a no-op on Windows: Go cannot open a directory handle that
// supports FlushFileBuffers. AnchorDB relies on NTFS metadata journaling plus
// write-through renames instead (docs/architecture.md §8).
func syncDir(string) error { return nil }
