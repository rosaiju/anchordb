package anchordb_test

// Regression test for spec §2.2 rule 11 / §3: Close must wait for an
// in-progress Checkpoint (it takes ckptMu before mu). Added by the main agent
// during integration to close the gap listed in docs/correctness.md.
//
// The test is deterministic: it uses channels to pause the checkpoint at a
// chosen file write, and it inspects goroutine stacks to confirm that Close is
// parked on a lock (not merely "hasn't run yet") before releasing the
// checkpoint. Timeouts are only deadlock watchdogs, never synchronization.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/vfs"
)

// gateFS pauses the first Write, after arm is set, to a file whose base name
// matches pattern: it closes entered, then waits until release is closed.
// Arming after Open matters because Open itself writes CURRENT.tmp when it
// initializes a new database.
type gateFS struct {
	vfs.FS
	pattern string
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newGateFS(pattern string) *gateFS {
	return &gateFS{FS: vfs.OS, pattern: pattern, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gateFS) OpenFile(name string, flag int, perm os.FileMode) (vfs.File, error) {
	f, err := g.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if ok, _ := filepath.Match(g.pattern, filepath.Base(name)); ok {
		return &gateFile{File: f, g: g}, nil
	}
	return f, nil
}

type gateFile struct {
	vfs.File
	g *gateFS
}

func (f *gateFile) Write(p []byte) (int, error) {
	if !f.g.armed.Load() {
		return f.File.Write(p)
	}
	f.g.once.Do(func() {
		close(f.g.entered)
		<-f.g.release
	})
	return f.File.Write(p)
}

const watchdog = 30 * time.Second

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(watchdog):
		t.Fatalf("deadlock watchdog: %s did not happen within %v", what, watchdog)
	}
}

// closeIsParked reports whether some goroutine is blocked acquiring a lock
// inside (*DB).Close.
func closeIsParked() bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		header, _, _ := strings.Cut(g, "\n")
		if strings.Contains(g, "anchordb.(*DB).Close(") &&
			(strings.Contains(header, "Lock") || strings.Contains(header, "semacquire")) {
			return true
		}
	}
	return false
}

func TestCloseWaitsForRunningCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pattern string // file whose first write pauses the checkpoint
		// writeDuringPause commits a transaction while the checkpoint is
		// paused; only possible once the checkpoint has released mu (§5.5 step 5).
		writeDuringPause bool
	}{
		{"paused-writing-snapshot", "checkpoint-*.ckpt.tmp", false},
		{"paused-publishing-CURRENT", "CURRENT.tmp", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			gate := newGateFS(tc.pattern)
			// SyncNone makes Close perform I/O (a WAL fsync), so a Close that
			// failed to wait would race visibly with the checkpoint.
			db, err := anchordb.Open(dir, &anchordb.Options{FS: gate, SyncMode: anchordb.SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]string{}
			for i := 0; i < 50; i++ {
				k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)
				if err := db.Put([]byte(k), []byte(v)); err != nil {
					t.Fatal(err)
				}
				want[k] = v
			}
			ckptTxID := uint64(50)

			gate.armed.Store(true)
			ckptErr := make(chan error, 1)
			go func() { ckptErr <- db.Checkpoint() }()
			waitFor(t, gate.entered, "checkpoint reaching the paused write")

			if tc.writeDuringPause {
				if err := db.Put([]byte("during"), []byte("publish")); err != nil {
					t.Fatalf("writer blocked or failed during checkpoint publication: %v", err)
				}
				want["during"] = "publish"
			}

			closeErr := make(chan error, 1)
			closeReturned := make(chan struct{})
			go func() { closeErr <- db.Close(); close(closeReturned) }()

			// Wait until Close is provably parked on a lock. If Close does not
			// wait for the checkpoint it returns instead, which fails the test.
			deadline := time.Now().Add(watchdog)
			for !closeIsParked() {
				select {
				case <-closeReturned:
					t.Fatalf("Close returned (%v) while a checkpoint was still running", <-closeErr)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("deadlock watchdog: Close never reached its lock")
				}
				runtime.Gosched()
			}
			select {
			case <-closeReturned:
				t.Fatal("Close returned while a checkpoint was still running")
			default:
			}

			close(gate.release)
			select {
			case err := <-ckptErr:
				if err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
			case <-time.After(watchdog):
				t.Fatal("deadlock watchdog: Checkpoint did not finish")
			}
			waitFor(t, closeReturned, "Close returning")
			if err := <-closeErr; err != nil {
				t.Fatalf("Close: %v", err)
			}
			if _, err := db.Begin(false); !errors.Is(err, anchordb.ErrClosed) {
				t.Fatalf("Begin after Close: %v, want ErrClosed", err)
			}

			// Reopen with the real file system: committed data survives and
			// the checkpoint was fully published and reclaimed.
			db2, err := anchordb.Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db2.Close()
			st, err := db2.Stats()
			if err != nil {
				t.Fatal(err)
			}
			if st.CheckpointTxID != ckptTxID {
				t.Fatalf("CheckpointTxID=%d, want %d (checkpoint not published)", st.CheckpointTxID, ckptTxID)
			}
			if st.Recovery.RemovedFiles != 0 {
				t.Fatalf("RemovedFiles=%d: the checkpoint left garbage behind", st.Recovery.RemovedFiles)
			}
			if st.Keys != len(want) {
				t.Fatalf("Keys=%d, want %d", st.Keys, len(want))
			}
			for k, v := range want {
				got, err := db2.Get([]byte(k))
				if err != nil || string(got) != v {
					t.Fatalf("Get(%s) = %q, %v; want %q", k, got, err, v)
				}
			}
		})
	}
}
