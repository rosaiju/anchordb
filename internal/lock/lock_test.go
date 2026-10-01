package lock_test

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/rosaiju/anchordb/internal/lock"
)

func TestExclusiveInProcess(t *testing.T) {
	p := filepath.Join(t.TempDir(), "LOCK")
	l, err := lock.Acquire(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Acquire(p); !errors.Is(err, lock.ErrLocked) {
		t.Fatalf("second acquire: %v", err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatal(err)
	}
	l2, err := lock.Acquire(p)
	if err != nil {
		t.Fatalf("after unlock: %v", err)
	}
	l2.Unlock()
}

// A lock held by another process blocks us, and is released when that process dies.
func TestExclusiveAcrossProcesses(t *testing.T) {
	if p := os.Getenv("LOCK_CHILD"); p != "" {
		if _, err := lock.Acquire(p); err != nil {
			os.Stdout.WriteString("fail\n")
			os.Exit(1)
		}
		os.Stdout.WriteString("held\n")
		bufio.NewReader(os.Stdin).ReadString('\n') // hold until parent says so
		os.Exit(0)
	}
	p := filepath.Join(t.TempDir(), "LOCK")
	cmd := exec.Command(os.Args[0], "-test.run=^TestExclusiveAcrossProcesses$")
	cmd.Env = append(os.Environ(), "LOCK_CHILD="+p)
	in, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(out).ReadString('\n')
	if line != "held\n" {
		t.Fatalf("child: %q", line)
	}
	if _, err := lock.Acquire(p); !errors.Is(err, lock.ErrLocked) {
		t.Fatalf("acquire while child holds: %v", err)
	}
	cmd.Process.Kill() // crash: OS must release the lock
	in.Close()
	cmd.Wait()
	l, err := lock.Acquire(p)
	if err != nil {
		t.Fatalf("lock not released after child died: %v", err)
	}
	l.Unlock()
}
