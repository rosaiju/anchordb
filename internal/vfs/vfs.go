// Package vfs is the small file-system interface the engine performs all
// persistent I/O through (except the directory lock). The production
// implementation is OS; tests wrap it to inject write, sync, and rename
// failures.
package vfs

import (
	"io"
	"os"
)

// File is the subset of *os.File the engine uses.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.Closer
	Sync() error
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	Name() string
}

// FS is the subset of the os package the engine uses.
type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Remove(name string) error
	// Rename atomically replaces newpath with oldpath.
	Rename(oldpath, newpath string) error
	ReadDir(dir string) ([]os.DirEntry, error)
	MkdirAll(dir string, perm os.FileMode) error
	Stat(name string) (os.FileInfo, error)
	// SyncDir makes earlier creates, renames, and removes in dir durable.
	SyncDir(dir string) error
}

// OS is the real file system.
var OS FS = osFS{}

type osFS struct{}

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err // avoid returning a typed-nil *os.File in the interface
	}
	return f, nil
}
func (osFS) Remove(name string) error                    { return os.Remove(name) }
func (osFS) Rename(oldpath, newpath string) error        { return rename(oldpath, newpath) }
func (osFS) ReadDir(dir string) ([]os.DirEntry, error)   { return os.ReadDir(dir) }
func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (osFS) Stat(name string) (os.FileInfo, error)       { return os.Stat(name) }
func (osFS) SyncDir(dir string) error                    { return syncDir(dir) }
