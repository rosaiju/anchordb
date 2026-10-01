package faultfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rosaiju/anchordb/internal/faultfs"
)

func TestShortWriteAndCounting(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	r := fs.Inject(faultfs.Rule{Op: faultfs.OpWrite, Pattern: "a*.log", After: 2, Partial: 3})
	f, err := fs.OpenFile(filepath.Join(dir, "a1.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("first write: %d %v", n, err)
	}
	n, err := f.Write([]byte("world"))
	if n != 3 || !errors.Is(err, faultfs.ErrInjected) {
		t.Fatalf("second write: %d %v", n, err)
	}
	if n, err := f.Write([]byte("!")); n != 1 || err != nil {
		t.Fatalf("third write: %d %v", n, err)
	}
	f.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "a1.log"))
	if string(b) != "hellowor!" {
		t.Fatalf("content %q", b)
	}
	if fs.Fired(r) != 1 || fs.Count(faultfs.OpWrite, "a*.log") != 3 {
		t.Fatalf("fired=%d count=%d", fs.Fired(r), fs.Count(faultfs.OpWrite, "a*.log"))
	}
}

func TestForeverAndPatternMismatch(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	sentinel := errors.New("boom")
	fs.Inject(faultfs.Rule{Op: faultfs.OpSync, Pattern: "x", Times: -1, Err: sentinel})
	f, _ := fs.OpenFile(filepath.Join(dir, "y"), os.O_CREATE|os.O_RDWR, 0o644)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	g, _ := fs.OpenFile(filepath.Join(dir, "x"), os.O_CREATE|os.O_RDWR, 0o644)
	for i := 0; i < 3; i++ {
		if err := g.Sync(); !errors.Is(err, sentinel) {
			t.Fatalf("sync %d: %v", i, err)
		}
	}
	g.Close()
	if err := fs.Rename(filepath.Join(dir, "y"), filepath.Join(dir, "z")); err != nil {
		t.Fatal(err)
	}
	fs.Inject(faultfs.Rule{Op: faultfs.OpRename, Pattern: "z"})
	if err := fs.Rename(filepath.Join(dir, "z"), filepath.Join(dir, "w")); err == nil {
		t.Fatal("rename matching old name should fail")
	}
}

func TestLoseUnsynced(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	p := filepath.Join(dir, "f")
	f, _ := fs.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	f.Write([]byte("durable"))
	f.Sync()
	f.Write([]byte("-volatile"))
	f.Close()
	if err := fs.LoseUnsynced(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if string(b) != "durable" {
		t.Fatalf("got %q", b)
	}
}
