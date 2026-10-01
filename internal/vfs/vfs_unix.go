//go:build !windows

package vfs

import "os"

func rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

// syncDir fsyncs the directory so that entries created, renamed, or removed in
// it survive a crash (POSIX does not guarantee this from a file fsync alone).
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
