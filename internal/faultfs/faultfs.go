// Package faultfs wraps a vfs.FS and injects failures into chosen calls.
//
// A Rule selects calls by operation kind, by a filepath.Match pattern applied
// to the file's base name, and by call count (the n-th matching call fails,
// optionally for several calls in a row or forever). Write failures can be
// short writes: the first k bytes really reach the underlying file before the
// error is returned, which is exactly what an interrupted write looks like.
//
// LoseUnsynced is an APPROXIMATION of power loss: it truncates every file the
// wrapper has seen written to the length it had at its last successful Sync
// (or when it was first opened). It does not model lost directory operations
// (creates/renames/deletes), reordering, or partially persisted pages, so it
// can only show that the engine relies on fsync for file data; it does not
// prove behaviour under real power loss.
package faultfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rosaiju/anchordb/internal/vfs"
)

// Op names an FS or File operation.
type Op string

const (
	OpOpen     Op = "open"
	OpWrite    Op = "write"
	OpSync     Op = "sync"
	OpRename   Op = "rename"
	OpRemove   Op = "remove"
	OpSyncDir  Op = "syncdir"
	OpTruncate Op = "truncate"
	OpReadDir  Op = "readdir"
	OpRead     Op = "read"
	OpClose    Op = "close"
)

// ErrInjected is the default injected error.
var ErrInjected = errors.New("faultfs: injected fault")

// Rule describes one fault.
type Rule struct {
	Op      Op
	Pattern string // filepath.Match against the base name; "" matches all
	After   int    // fail the After-th matching call (1-based; 0 means 1)
	Times   int    // number of consecutive failing calls (0 means 1; <0 forever)
	Err     error  // nil means ErrInjected
	// Partial applies to OpWrite: this many bytes (clamped to the buffer) are
	// written to the underlying file before the error is returned.
	Partial int

	seen  int
	fired int
}

// Event records one intercepted call.
type Event struct {
	Op       Op
	Name     string // base name (for rename: new name)
	Injected bool
}

// FS is the fault-injecting wrapper.
type FS struct {
	inner vfs.FS

	mu     sync.Mutex
	rules  []*Rule
	events []Event
	synced map[string]int64 // full path → durable length (power-loss approximation)
}

// New wraps inner (vfs.OS if nil).
func New(inner vfs.FS) *FS {
	if inner == nil {
		inner = vfs.OS
	}
	return &FS{inner: inner, synced: map[string]int64{}}
}

// Inject adds a rule and returns it.
func (f *FS) Inject(r Rule) *Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	rr := r
	f.rules = append(f.rules, &rr)
	return &rr
}

// Clear removes all rules (the event log is kept).
func (f *FS) Clear() {
	f.mu.Lock()
	f.rules = nil
	f.mu.Unlock()
}

// Fired reports how many times r injected a fault.
func (f *FS) Fired(r *Rule) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return r.fired
}

// Events returns a copy of the event log.
func (f *FS) Events() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Event(nil), f.events...)
}

// Count returns how many calls of op matched pattern so far.
func (f *FS) Count(op Op, pattern string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.events {
		if e.Op == op && match(pattern, e.Name) {
			n++
		}
	}
	return n
}

func match(pattern, base string) bool {
	if pattern == "" {
		return true
	}
	ok, err := filepath.Match(pattern, base)
	return err == nil && ok
}

// check records the call and returns the rule that fires, if any.
func (f *FS) check(op Op, names ...string) *Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	base := ""
	if len(names) > 0 {
		base = filepath.Base(names[len(names)-1])
	}
	var fire *Rule
	for _, r := range f.rules {
		if r.Op != op {
			continue
		}
		m := false
		for _, n := range names {
			if match(r.Pattern, filepath.Base(n)) {
				m = true
			}
		}
		if !m {
			continue
		}
		r.seen++
		after := r.After
		if after <= 0 {
			after = 1
		}
		times := r.Times
		if times == 0 {
			times = 1
		}
		if fire == nil && r.seen >= after && (times < 0 || r.seen < after+times) {
			r.fired++
			fire = r
		}
	}
	f.events = append(f.events, Event{Op: op, Name: base, Injected: fire != nil})
	return fire
}

func errOf(r *Rule, op Op, name string) error {
	e := r.Err
	if e == nil {
		e = ErrInjected
	}
	return fmt.Errorf("faultfs %s %s: %w", op, filepath.Base(name), e)
}

// OpenFile implements vfs.FS.
func (f *FS) OpenFile(name string, flag int, perm os.FileMode) (vfs.File, error) {
	if r := f.check(OpOpen, name); r != nil {
		return nil, errOf(r, OpOpen, name)
	}
	inner, err := f.inner.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if _, ok := f.synced[name]; !ok || flag&os.O_TRUNC != 0 {
		var sz int64
		if st, err := inner.Stat(); err == nil {
			sz = st.Size()
		}
		if flag&os.O_TRUNC != 0 {
			sz = 0
		}
		f.synced[name] = sz
	}
	f.mu.Unlock()
	return &file{fs: f, inner: inner, path: name}, nil
}

// Remove implements vfs.FS.
func (f *FS) Remove(name string) error {
	if r := f.check(OpRemove, name); r != nil {
		return errOf(r, OpRemove, name)
	}
	err := f.inner.Remove(name)
	if err == nil {
		f.mu.Lock()
		delete(f.synced, name)
		f.mu.Unlock()
	}
	return err
}

// Rename implements vfs.FS. Rules match either the old or the new base name.
func (f *FS) Rename(oldpath, newpath string) error {
	if r := f.check(OpRename, oldpath, newpath); r != nil {
		return errOf(r, OpRename, newpath)
	}
	err := f.inner.Rename(oldpath, newpath)
	if err == nil {
		f.mu.Lock()
		if v, ok := f.synced[oldpath]; ok {
			f.synced[newpath] = v
			delete(f.synced, oldpath)
		}
		f.mu.Unlock()
	}
	return err
}

// ReadDir implements vfs.FS.
func (f *FS) ReadDir(dir string) ([]os.DirEntry, error) {
	if r := f.check(OpReadDir, dir); r != nil {
		return nil, errOf(r, OpReadDir, dir)
	}
	return f.inner.ReadDir(dir)
}

// MkdirAll implements vfs.FS (never faulted).
func (f *FS) MkdirAll(dir string, perm os.FileMode) error { return f.inner.MkdirAll(dir, perm) }

// Stat implements vfs.FS (never faulted).
func (f *FS) Stat(name string) (os.FileInfo, error) { return f.inner.Stat(name) }

// SyncDir implements vfs.FS.
func (f *FS) SyncDir(dir string) error {
	if r := f.check(OpSyncDir, dir); r != nil {
		return errOf(r, OpSyncDir, dir)
	}
	return f.inner.SyncDir(dir)
}

// LoseUnsynced truncates every tracked file to its last-synced length.
// All files must be closed (or the DB closed) first. See the package comment
// for why this is only an approximation of power loss.
func (f *FS) LoseUnsynced() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for path, n := range f.synced {
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if st.Size() > n {
			if err := os.Truncate(path, n); err != nil {
				return err
			}
		}
	}
	return nil
}

// SyncedLengths returns, for every file path seen, the length it had at its
// last successful Sync (or when first opened). A test can copy a live
// directory and cut each copy to these lengths to approximate power loss
// without closing the DB (Close would fsync).
func (f *FS) SyncedLengths() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64, len(f.synced))
	for k, v := range f.synced {
		out[k] = v
	}
	return out
}

type file struct {
	fs    *FS
	inner vfs.File
	path  string
}

func (w *file) Read(p []byte) (int, error) {
	if r := w.fs.check(OpRead, w.path); r != nil {
		return 0, errOf(r, OpRead, w.path)
	}
	return w.inner.Read(p)
}

func (w *file) ReadAt(p []byte, off int64) (int, error) {
	if r := w.fs.check(OpRead, w.path); r != nil {
		return 0, errOf(r, OpRead, w.path)
	}
	return w.inner.ReadAt(p, off)
}

func (w *file) Write(p []byte) (int, error) {
	if r := w.fs.check(OpWrite, w.path); r != nil {
		k := r.Partial
		if k < 0 {
			k = 0
		}
		if k > len(p) {
			k = len(p)
		}
		n := 0
		if k > 0 {
			n, _ = w.inner.Write(p[:k])
		}
		return n, errOf(r, OpWrite, w.path)
	}
	return w.inner.Write(p)
}

func (w *file) Sync() error {
	if r := w.fs.check(OpSync, w.path); r != nil {
		return errOf(r, OpSync, w.path)
	}
	if err := w.inner.Sync(); err != nil {
		return err
	}
	if st, err := w.inner.Stat(); err == nil {
		w.fs.mu.Lock()
		w.fs.synced[w.path] = st.Size()
		w.fs.mu.Unlock()
	}
	return nil
}

func (w *file) Truncate(size int64) error {
	if r := w.fs.check(OpTruncate, w.path); r != nil {
		return errOf(r, OpTruncate, w.path)
	}
	if err := w.inner.Truncate(size); err != nil {
		return err
	}
	w.fs.mu.Lock()
	if w.fs.synced[w.path] > size {
		w.fs.synced[w.path] = size
	}
	w.fs.mu.Unlock()
	return nil
}

func (w *file) Close() error {
	w.fs.check(OpClose, w.path) // recorded, never faulted
	return w.inner.Close()
}

func (w *file) Stat() (os.FileInfo, error) { return w.inner.Stat() }
func (w *file) Name() string               { return w.inner.Name() }
