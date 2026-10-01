package anchordb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/faultfs"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

// ioEvents counts mutating I/O calls seen by fs.
func ioEvents(fs *faultfs.FS) int {
	n := 0
	for _, e := range fs.Events() {
		switch e.Op {
		case faultfs.OpWrite, faultfs.OpSync, faultfs.OpTruncate, faultfs.OpRename, faultfs.OpRemove, faultfs.OpSyncDir:
			n++
		}
	}
	return n
}

// assertFailedDB checks the §2.1.1 table for new operations.
func assertFailedDB(t *testing.T, db *anchordb.DB) {
	t.Helper()
	_, err := db.Get([]byte("a"))
	wantErr(t, err, anchordb.ErrDBFailed, "Get on failed DB")
	wantErr(t, db.Put([]byte("a"), nil), anchordb.ErrDBFailed, "Put on failed DB")
	wantErr(t, db.Delete([]byte("a")), anchordb.ErrDBFailed, "Delete on failed DB")
	wantErr(t, db.Scan(nil, nil, func(k, v []byte) bool { return true }), anchordb.ErrDBFailed, "Scan on failed DB")
	_, err = db.Begin(false)
	wantErr(t, err, anchordb.ErrDBFailed, "Begin(false) on failed DB")
	_, err = db.Begin(true)
	wantErr(t, err, anchordb.ErrDBFailed, "Begin(true) on failed DB")
	wantErr(t, db.View(func(*anchordb.Tx) error { return nil }), anchordb.ErrDBFailed, "View on failed DB")
	wantErr(t, db.Update(func(*anchordb.Tx) error { return nil }), anchordb.ErrDBFailed, "Update on failed DB")
	wantErr(t, db.Checkpoint(), anchordb.ErrDBFailed, "Checkpoint on failed DB")
	_, err = db.Stats()
	wantErr(t, err, anchordb.ErrDBFailed, "Stats on failed DB")
}

// closeFailed closes a failed DB and checks Close returns nil and performs no
// write, fsync, truncate, rename, remove or directory sync.
func closeFailed(t *testing.T, db *anchordb.DB, fs *faultfs.FS) {
	t.Helper()
	before := ioEvents(fs)
	if err := db.Close(); err != nil {
		t.Fatalf("Close on failed DB: %v (§2.1.1: must return nil)", err)
	}
	if after := ioEvents(fs); after != before {
		var extra []string
		for _, e := range fs.Events()[len(fs.Events())-(after-before):] {
			extra = append(extra, string(e.Op)+":"+e.Name)
		}
		t.Fatalf("Close on a failed DB performed I/O: %v", extra)
	}
	wantErr(t, db.Close(), anchordb.ErrClosed, "second Close")
}

// §5.2/§6.4: a commit whose WAL write lets through k bytes (or whose fsync
// fails after a full write) returns ErrCommitUncertain wrapping the cause,
// fails the DB, and after reopen the outcome is decided by the bytes on disk.
func TestCommitWriteFailures(t *testing.T) {
	val := []byte("value-of-c-0123456789")
	frameLen := len(refmodel.Frame(1, refmodel.CommitPayload(3, []refmodel.Op{{Key: []byte("c"), Value: val}})))
	type tc struct {
		name    string
		rule    faultfs.Rule
		through int // bytes expected to reach the file
	}
	cases := []tc{
		{"write 0 bytes", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log"}, 0},
		{"write 1 byte", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: 1}, 1},
		{"write 15 bytes", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: 15}, 15},
		{"write header only", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: 16}, 16},
		{"write half", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: frameLen / 2}, frameLen / 2},
		{"write all but 1", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: frameLen - 1}, frameLen - 1},
		{"full write, error", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: frameLen}, frameLen},
		{"fsync fails", faultfs.Rule{Op: faultfs.OpSync, Pattern: "wal-*.log"}, frameLen},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := faultfs.New(nil)
			db := openT(t, dir, &anchordb.Options{FS: fs})
			putT(t, db, "a", "1")
			putT(t, db, "b", "2")
			seg := lastSegment(t, dir)
			sizeBefore := fileSize(dir, filepath.Base(seg))
			r := fs.Inject(c.rule)
			err := db.Put([]byte("c"), val)
			if fs.Fired(r) != 1 {
				t.Fatalf("fault did not fire (fired=%d)", fs.Fired(r))
			}
			wantErr(t, err, anchordb.ErrCommitUncertain, "commit")
			wantErr(t, err, faultfs.ErrInjected, "commit error must wrap the cause")
			if errors.Is(err, anchordb.ErrDBFailed) {
				t.Fatal("ErrCommitUncertain must not satisfy ErrDBFailed (§2.1)")
			}
			assertFailedDB(t, db)
			closeFailed(t, db, fs)
			through := int(fileSize(dir, filepath.Base(seg)) - sizeBefore)
			if through != c.through {
				t.Fatalf("%d bytes reached the segment, expected %d (whole frame in one write, §5.2 step 4)", through, c.through)
			}
			db = openT(t, dir, nil)
			md := refmodel.New()
			md.Apply([]refmodel.Op{{Key: []byte("a"), Value: []byte("1")}})
			md.Apply([]refmodel.Op{{Key: []byte("b"), Value: []byte("2")}})
			s := statsT(t, db)
			if through == frameLen {
				md.Apply([]refmodel.Op{{Key: []byte("c"), Value: val}})
				if s.Recovery.TruncatedBytes != 0 {
					t.Fatalf("TruncatedBytes=%d", s.Recovery.TruncatedBytes)
				}
			} else if s.Recovery.TruncatedBytes != int64(through) {
				t.Fatalf("TruncatedBytes=%d want %d", s.Recovery.TruncatedBytes, through)
			}
			assertModel(t, db, md)
			if s.LastTxID != md.LastTxID {
				t.Fatalf("LastTxID=%d want %d", s.LastTxID, md.LastTxID)
			}
			// The DB works normally after recovery.
			putT(t, db, "d", "4")
			closeT(t, db)
		})
	}
}

func TestCommitFailureSyncNone(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	db := openT(t, dir, &anchordb.Options{FS: fs, SyncMode: anchordb.SyncNone})
	putT(t, db, "a", "1")
	fs.Inject(faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log", Partial: 5})
	wantErr(t, db.Put([]byte("b"), []byte("2")), anchordb.ErrCommitUncertain, "SyncNone write failure")
	closeFailed(t, db, fs)
	db = openT(t, dir, nil)
	md := refmodel.New()
	md.Put([]byte("a"), []byte("1"))
	assertModel(t, db, md)
	closeT(t, db)
}

// §2.1.1: a transaction already open when the DB fails keeps working for
// reads; its Commit returns ErrDBFailed and closes it; Rollback works.
func TestOpenTxSurvivesDBFailure(t *testing.T) {
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			dir := t.TempDir()
			fs := faultfs.New(nil)
			db := openT(t, dir, &anchordb.Options{FS: fs})
			putT(t, db, "a", "1")
			// Fail the DB via an ambiguous CURRENT publication, which happens
			// outside mu (§5.5 step 5–6) so a reader can stay open.
			fs.Inject(faultfs.Rule{Op: faultfs.OpRename, Pattern: "CURRENT.tmp"})
			ro := beginT(t, db, false)
			var wg sync.WaitGroup
			var ckErr error
			wg.Add(1)
			go func() { defer wg.Done(); ckErr = db.Checkpoint() }()
			wg.Wait()
			if ckErr == nil {
				t.Fatal("Checkpoint succeeded despite CURRENT rename failure")
			}
			if v, err := ro.Get([]byte("a")); err != nil || string(v) != "1" {
				t.Fatalf("open tx read after failure: %q %v", v, err)
			}
			n := 0
			if err := ro.Scan(nil, nil, func(k, v []byte) bool { n++; return true }); err != nil || n != 1 {
				t.Fatalf("open tx scan after failure: %v n=%d", err, n)
			}
			if end == "commit" {
				wantErr(t, ro.Commit(), anchordb.ErrDBFailed, "Commit of open ro tx on failed DB")
			} else if err := ro.Rollback(); err != nil {
				t.Fatalf("Rollback on failed DB: %v", err)
			}
			wantErr(t, ro.Rollback(), anchordb.ErrTxClosed, "tx closed afterwards")
			assertFailedDB(t, db)
			closeFailed(t, db, fs)
			db = openT(t, dir, nil)
			md := refmodel.New()
			md.Put([]byte("a"), []byte("1"))
			assertModel(t, db, md)
			closeT(t, db)
		})
	}
}

// --- rotation failures (§5.4) ---

type rotCase struct {
	name    string
	rule    faultfs.Rule
	failsDB bool
	newSeg  bool // whether wal-2 exists after reopen
}

func TestRotationFailures(t *testing.T) {
	cases := []rotCase{
		{"seal fsync", faultfs.Rule{Op: faultfs.OpSync, Pattern: "wal-*.log"}, true, false},
		{"tmp create", faultfs.Rule{Op: faultfs.OpOpen, Pattern: "wal-*.log.tmp"}, false, false},
		{"tmp header write", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "wal-*.log.tmp", Partial: 10}, false, false},
		{"tmp fsync", faultfs.Rule{Op: faultfs.OpSync, Pattern: "wal-*.log.tmp"}, false, false},
		{"rename", faultfs.Rule{Op: faultfs.OpRename, Pattern: "wal-*.log.tmp"}, true, false},
		{"dir sync", faultfs.Rule{Op: faultfs.OpSyncDir}, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := faultfs.New(nil)
			db := openT(t, dir, &anchordb.Options{FS: fs, SegmentSize: 64})
			putT(t, db, "a", "1") // wal-1 now >= 64 bytes with one record
			r := fs.Inject(c.rule)
			err := db.Put([]byte("b"), []byte("2")) // triggers rotation first
			if fs.Fired(r) != 1 {
				t.Fatalf("fault did not fire")
			}
			if err == nil {
				t.Fatal("commit succeeded despite rotation failure")
			}
			if errors.Is(err, anchordb.ErrCommitUncertain) {
				t.Fatalf("rotation failure must not be ErrCommitUncertain (§5.2 step 2): %v", err)
			}
			wantErr(t, err, faultfs.ErrInjected, "cause")
			md := refmodel.New()
			md.Put([]byte("a"), []byte("1"))
			if c.failsDB {
				wantErr(t, err, anchordb.ErrDBFailed, "rotation failure that fails the DB")
				assertFailedDB(t, db)
				closeFailed(t, db, fs)
			} else {
				if errors.Is(err, anchordb.ErrDBFailed) {
					t.Fatalf("DB failed for a recoverable rotation error: %v", err)
				}
				for _, n := range listDir(t, dir) {
					if strings.HasSuffix(n, ".tmp") {
						t.Fatalf("temp file %s left after failed rotation", n)
					}
				}
				// DB still usable; rotation succeeds now.
				putT(t, db, "c", "3")
				md.Put([]byte("c"), []byte("3"))
				if s := statsT(t, db); s.ActiveSeq != 2 {
					t.Fatalf("ActiveSeq=%d after successful retry", s.ActiveSeq)
				}
				closeT(t, db)
			}
			db = openT(t, dir, nil)
			assertModel(t, db, md)
			_, err2 := os.Stat(filepath.Join(dir, refmodel.SegmentName(2)))
			if c.failsDB && (err2 == nil) != c.newSeg {
				t.Fatalf("wal-2 exists=%v, want %v", err2 == nil, c.newSeg)
			}
			putT(t, db, "z", "26")
			closeT(t, db)
		})
	}
}

// --- checkpoint failures (§5.5) ---

type ckCase struct {
	name      string
	rule      faultfs.Rule
	failsDB   bool
	published bool // new CURRENT is authoritative afterwards (known in memory)
}

func TestCheckpointFailures(t *testing.T) {
	cases := []ckCase{
		{"rotate tmp create", faultfs.Rule{Op: faultfs.OpOpen, Pattern: "wal-*.log.tmp"}, false, false},
		{"rotate seal fsync", faultfs.Rule{Op: faultfs.OpSync, Pattern: "wal-*.log"}, true, false},
		{"rotate rename", faultfs.Rule{Op: faultfs.OpRename, Pattern: "wal-*.log.tmp"}, true, false},
		{"rotate dir sync", faultfs.Rule{Op: faultfs.OpSyncDir, After: 1}, true, false},
		{"ckpt tmp create", faultfs.Rule{Op: faultfs.OpOpen, Pattern: "checkpoint-*.ckpt.tmp"}, false, false},
		{"ckpt short write", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "checkpoint-*.ckpt.tmp", Partial: 40}, false, false},
		{"ckpt fsync", faultfs.Rule{Op: faultfs.OpSync, Pattern: "checkpoint-*.ckpt.tmp"}, false, false},
		{"ckpt rename", faultfs.Rule{Op: faultfs.OpRename, Pattern: "checkpoint-*.ckpt.tmp"}, false, false},
		{"ckpt dir sync", faultfs.Rule{Op: faultfs.OpSyncDir, After: 2}, false, false},
		{"CURRENT.tmp create", faultfs.Rule{Op: faultfs.OpOpen, Pattern: "CURRENT.tmp"}, false, false},
		{"CURRENT.tmp write", faultfs.Rule{Op: faultfs.OpWrite, Pattern: "CURRENT.tmp", Partial: 7}, false, false},
		{"CURRENT.tmp fsync", faultfs.Rule{Op: faultfs.OpSync, Pattern: "CURRENT.tmp"}, false, false},
		{"CURRENT rename", faultfs.Rule{Op: faultfs.OpRename, Pattern: "CURRENT.tmp"}, true, false},
		{"CURRENT dir sync", faultfs.Rule{Op: faultfs.OpSyncDir, After: 3}, true, false},
		{"reclaim remove", faultfs.Rule{Op: faultfs.OpRemove, Pattern: "wal-*.log"}, false, true},
		{"reclaim dir sync", faultfs.Rule{Op: faultfs.OpSyncDir, After: 4}, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := faultfs.New(nil)
			opts := &anchordb.Options{FS: fs}
			db := openT(t, dir, opts)
			md := refmodel.New()
			// A first successful checkpoint so reclamation has an old
			// checkpoint and old segments to delete.
			for i := 0; i < 10; i++ {
				putT(t, db, string(k(i)), "v1")
				md.Apply([]refmodel.Op{{Key: k(i), Value: []byte("v1")}})
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for i := 5; i < 15; i++ {
				putT(t, db, string(k(i)), "v2")
				md.Apply([]refmodel.Op{{Key: k(i), Value: []byte("v2")}})
			}
			oldCk := statsT(t, db).CheckpointTxID
			r := fs.Inject(c.rule)
			err := db.Checkpoint()
			if fs.Fired(r) != 1 {
				t.Fatalf("fault did not fire (fired=%d)", fs.Fired(r))
			}
			if err == nil {
				t.Fatal("Checkpoint returned nil despite an injected failure")
			}
			wantErr(t, err, faultfs.ErrInjected, "cause")
			if c.failsDB {
				assertFailedDB(t, db)
				closeFailed(t, db, fs)
			} else {
				if errors.Is(err, anchordb.ErrDBFailed) {
					t.Fatalf("DB failed for a non-fatal checkpoint error: %v", err)
				}
				s := statsT(t, db)
				wantCk := oldCk
				if c.published {
					wantCk = md.LastTxID
				}
				if s.CheckpointTxID != wantCk {
					t.Fatalf("CheckpointTxID=%d want %d", s.CheckpointTxID, wantCk)
				}
				for _, n := range listDir(t, dir) {
					if strings.HasSuffix(n, ".tmp") {
						t.Fatalf("temp file %s left after failed checkpoint", n)
					}
				}
				assertModel(t, db, md)
				// Keep going: more writes and a successful checkpoint, which
				// must also clean up any leftovers of the failed one.
				fs.Clear()
				putT(t, db, "after", "x")
				md.Apply([]refmodel.Op{{Key: []byte("after"), Value: []byte("x")}})
				if err := db.Checkpoint(); err != nil {
					t.Fatalf("checkpoint after recovery from failure: %v", err)
				}
				s = statsT(t, db)
				if cks := checkpoints(t, dir); len(cks) != 1 || cks[0] != refmodel.CheckpointName(s.CheckpointTxID) {
					t.Fatalf("checkpoint files after successful reclaim: %v", cks)
				}
				for _, n := range segments(t, dir) {
					var seq uint64
					fmt.Sscanf(n, "wal-%d.log", &seq)
					if seq < s.WALStartSeq {
						t.Fatalf("segment %s below wal_start %d survived reclamation", n, s.WALStartSeq)
					}
				}
				closeT(t, db)
			}
			db = openT(t, dir, nil)
			assertModel(t, db, md)
			s := statsT(t, db)
			if s.LastTxID != md.LastTxID {
				t.Fatalf("LastTxID=%d want %d", s.LastTxID, md.LastTxID)
			}
			for _, n := range listDir(t, dir) {
				if strings.HasSuffix(n, ".tmp") {
					t.Fatalf("temp file %s survived Open", n)
				}
			}
			if cks := checkpoints(t, dir); len(cks) > 1 {
				t.Fatalf("unreferenced checkpoints survived Open: %v", cks)
			}
			closeT(t, db)
		})
	}
}

// --- power-loss approximation (faultfs.SyncedLengths) ---

// copyCut copies dir (minus LOCK) to a new directory, cutting every file the
// FS tracked to its last-synced length. This approximates losing the page
// cache (it does not model lost directory operations).
func copyCut(t *testing.T, dir string, fs *faultfs.FS) string {
	t.Helper()
	dst := t.TempDir()
	synced := fs.SyncedLengths()
	for _, n := range listDir(t, dir) {
		if n == "LOCK" {
			continue
		}
		b := readFile(t, filepath.Join(dir, n))
		if l, ok := synced[filepath.Join(dir, n)]; ok && int64(len(b)) > l {
			b = b[:l]
		}
		writeFile(t, filepath.Join(dst, n), b)
	}
	return dst
}

func TestPowerLossApproxSyncAlways(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	db := openT(t, dir, &anchordb.Options{FS: fs, SegmentSize: 300})
	defer db.Close()
	md := refmodel.New()
	for i := 0; i < 60; i++ {
		putT(t, db, string(k(i%17)), fmt.Sprint(i))
		md.Apply([]refmodel.Op{{Key: k(i % 17), Value: []byte(fmt.Sprint(i))}})
		if i == 30 {
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		if i%10 == 9 {
			cp := copyCut(t, dir, fs)
			db2 := openT(t, cp, nil)
			assertModel(t, db2, md) // every acknowledged commit was fsynced (I5)
			closeT(t, db2)
		}
	}
}

func TestPowerLossApproxSyncNoneIsPrefix(t *testing.T) {
	dir := t.TempDir()
	fs := faultfs.New(nil)
	db := openT(t, dir, &anchordb.Options{FS: fs, SyncMode: anchordb.SyncNone, SegmentSize: 300})
	defer db.Close()
	md := refmodel.New()
	history := []*refmodel.Model{md.Clone()}
	for i := 0; i < 40; i++ {
		err := db.Update(func(tx *anchordb.Tx) error {
			tx.Put([]byte("x"), []byte(fmt.Sprint(i)))
			return tx.Put([]byte("y"), []byte(fmt.Sprint(i)))
		})
		if err != nil {
			t.Fatal(err)
		}
		mtx := md.Begin()
		mtx.Put([]byte("x"), []byte(fmt.Sprint(i)))
		mtx.Put([]byte("y"), []byte(fmt.Sprint(i)))
		mtx.Commit()
		history = append(history, md.Clone())
	}
	cp := copyCut(t, dir, fs)
	db2 := openT(t, cp, nil)
	defer db2.Close()
	got := dump(t, db2)
	n := statsT(t, db2).LastTxID
	if n > uint64(len(history)-1) || !history[n].Equal(got) {
		t.Fatalf("SyncNone power-loss state is not the prefix of %d txs: %s", n, kvString(got))
	}
	if n == uint64(len(history)-1) {
		t.Log("no suffix lost (rotation fsyncs happened to cover everything)")
	}
}
