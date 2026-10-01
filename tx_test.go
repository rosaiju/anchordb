package anchordb_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func beginT(t testing.TB, db *anchordb.DB, w bool) *anchordb.Tx {
	t.Helper()
	tx, err := db.Begin(w)
	if err != nil {
		t.Fatalf("Begin(%v): %v", w, err)
	}
	return tx
}

func TestMultiKeyCommitAndRollback(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	tx := beginT(t, db, true)
	if !tx.Writable() {
		t.Fatal("Writable")
	}
	for i := 0; i < 10; i++ {
		if err := tx.Put(k(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s := statsT(t, db); s.LastTxID != 1 || s.Keys != 10 {
		t.Fatalf("one tx should be one txid: %+v", s)
	}
	tx = beginT(t, db, true)
	tx.Put([]byte("rolled"), []byte("x"))
	tx.Delete(k(0))
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok := getT(t, db, "rolled"); ok {
		t.Fatal("rolled-back put visible")
	}
	if _, ok := getT(t, db, string(k(0))); !ok {
		t.Fatal("rolled-back delete applied")
	}
	if statsT(t, db).LastTxID != 1 {
		t.Fatal("rollback consumed a txid")
	}
	db = reopenT(t, db, dir, nil)
	if s := statsT(t, db); s.Keys != 10 || s.LastTxID != 1 {
		t.Fatalf("after reopen %+v", s)
	}
	closeT(t, db)
}

func TestReadYourWrites(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	md := refmodel.New()
	for _, s := range []string{"a", "c", "e", "g"} {
		putT(t, db, s, "old"+s)
		md.Put([]byte(s), []byte("old"+s))
	}
	tx := beginT(t, db, true)
	mtx := md.Begin()
	do := func(del bool, key, val string) {
		if del {
			if err := tx.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			mtx.Delete([]byte(key))
		} else {
			if err := tx.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			mtx.Put([]byte(key), []byte(val))
		}
	}
	do(false, "b", "newb")
	do(false, "c", "newc")
	do(true, "e", "")
	do(true, "zz", "")
	do(false, "a", "x")
	do(true, "a", "")
	do(false, "a", "final")
	do(false, "h", "")
	for _, key := range []string{"a", "b", "c", "e", "g", "h", "zz", "q"} {
		v, err := tx.Get([]byte(key))
		mv, ok := mtx.Get([]byte(key))
		if ok != (err == nil) || (ok && !bytes.Equal(v, mv)) {
			t.Fatalf("tx.Get(%s) = %q,%v; model %q,%v", key, v, err, mv, ok)
		}
		if !ok && !errors.Is(err, anchordb.ErrNotFound) {
			t.Fatalf("tx.Get(%s) err %v", key, err)
		}
	}
	for _, r := range [][2][]byte{{nil, nil}, {[]byte("b"), []byte("f")}, {[]byte("d"), nil}, {nil, []byte("c")}} {
		var got []refmodel.KV
		tx.Scan(r[0], r[1], func(k, v []byte) bool {
			got = append(got, refmodel.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
			return true
		})
		want := mtx.Scan(r[0], r[1])
		if kvString(got) != kvString(want) {
			t.Fatalf("tx.Scan(%q,%q):\n got %s\nwant %s", r[0], r[1], kvString(got), kvString(want))
		}
	}
	// Buffer is private until commit (checked after commit by state).
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mtx.Commit()
	assertModel(t, db, md)
}

func TestLifecycleClosedTx(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	for _, end := range []string{"commit", "rollback", "commit-empty"} {
		tx := beginT(t, db, true)
		if end != "commit-empty" {
			tx.Put([]byte("a"), []byte("b"))
		}
		if end == "rollback" {
			tx.Rollback()
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		_, err := tx.Get([]byte("a"))
		wantErr(t, err, anchordb.ErrTxClosed, end+": Get")
		wantErr(t, tx.Put([]byte("a"), nil), anchordb.ErrTxClosed, end+": Put")
		wantErr(t, tx.Delete([]byte("a")), anchordb.ErrTxClosed, end+": Delete")
		wantErr(t, tx.Scan(nil, nil, func(k, v []byte) bool { return true }), anchordb.ErrTxClosed, end+": Scan")
		wantErr(t, tx.Commit(), anchordb.ErrTxClosed, end+": Commit")
		wantErr(t, tx.Rollback(), anchordb.ErrTxClosed, end+": Rollback")
		// Precedence: ErrTxClosed beats validation.
		wantErr(t, tx.Put(nil, nil), anchordb.ErrTxClosed, end+": Put(empty) on closed")
	}
	// Lock was released: another writer can begin.
	tx := beginT(t, db, true)
	tx.Rollback()
}

func TestReadOnlyTx(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	putT(t, db, "a", "1")
	tx := beginT(t, db, false)
	if tx.Writable() {
		t.Fatal("read-only tx reports writable")
	}
	wantErr(t, tx.Put([]byte("a"), []byte("2")), anchordb.ErrTxReadOnly, "Put")
	wantErr(t, tx.Delete([]byte("a")), anchordb.ErrTxReadOnly, "Delete")
	wantErr(t, tx.Put(nil, nil), anchordb.ErrTxReadOnly, "ReadOnly beats validation")
	if v, err := tx.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatal("read in ro tx")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit of ro tx: %v", err)
	}
	if statsT(t, db).LastTxID != 1 {
		t.Fatal("ro commit consumed txid")
	}
	// Two read-only transactions may be open concurrently.
	r1 := beginT(t, db, false)
	r2 := beginT(t, db, false)
	r1.Rollback()
	r2.Rollback()
}

func TestEmptyCommitConsumesNoTxid(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	putT(t, db, "a", "1")
	before, _ := segBytes(dir)
	tx := beginT(t, db, true)
	tx.Get([]byte("a"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *anchordb.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// A validation-failed Put leaves the buffer empty.
	tx = beginT(t, db, true)
	wantErr(t, tx.Put(nil, []byte("x")), anchordb.ErrEmptyKey, "Put empty")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, _ := segBytes(dir)
	if statsT(t, db).LastTxID != 1 || after != before {
		t.Fatalf("empty commits wrote %d bytes / consumed txids", after-before)
	}
	// Put+Delete of the same new key: net no-op but still logged (rule 9).
	tx = beginT(t, db, true)
	tx.Put([]byte("tmp"), []byte("x"))
	tx.Delete([]byte("tmp"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if statsT(t, db).LastTxID != 2 {
		t.Fatal("net-no-op tx should consume a txid")
	}
	if _, ok := getT(t, db, "tmp"); ok {
		t.Fatal("tmp visible")
	}
	db = reopenT(t, db, dir, nil)
	if s := statsT(t, db); s.LastTxID != 2 || s.Keys != 1 {
		t.Fatalf("after reopen %+v", s)
	}
	closeT(t, db)
}

func segBytes(dir string) (int64, error) {
	var n int64
	ents, err := readDirNames(dir)
	for _, e := range ents {
		if len(e) > 4 && e[:4] == "wal-" {
			n += fileSize(dir, e)
		}
	}
	return n, err
}

func TestValidationErrorsDoNotCloseTx(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	tx := beginT(t, db, true)
	tx.Put([]byte("a"), []byte("1"))
	wantErr(t, tx.Put(nil, []byte("x")), anchordb.ErrEmptyKey, "empty key")
	wantErr(t, tx.Put(make([]byte, anchordb.MaxKeySize+1), nil), anchordb.ErrKeyTooLarge, "big key")
	wantErr(t, tx.Delete(nil), anchordb.ErrEmptyKey, "delete empty key")
	_, err := tx.Get(nil)
	wantErr(t, err, anchordb.ErrEmptyKey, "tx.Get empty key")
	if !testing.Short() {
		wantErr(t, tx.Put([]byte("b"), make([]byte, anchordb.MaxValueSize+1)), anchordb.ErrValueTooLarge, "big value")
	}
	if err := tx.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("tx unusable after validation error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	md := refmodel.New()
	md.Put([]byte("a"), []byte("1"))
	md.Put([]byte("c"), []byte("3"))
	assertModel(t, db, md)
}

func TestScanInProgress(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	putT(t, db, "a", "1")
	putT(t, db, "b", "2")
	tx := beginT(t, db, true)
	tx.Put([]byte("c"), []byte("3"))
	visited := 0
	err := tx.Scan(nil, nil, func(key, v []byte) bool {
		visited++
		wantErr(t, tx.Put([]byte("x"), nil), anchordb.ErrScanInProgress, "Put in scan")
		wantErr(t, tx.Delete([]byte("a")), anchordb.ErrScanInProgress, "Delete in scan")
		wantErr(t, tx.Commit(), anchordb.ErrScanInProgress, "Commit in scan")
		wantErr(t, tx.Rollback(), anchordb.ErrScanInProgress, "Rollback in scan")
		wantErr(t, tx.Put(nil, nil), anchordb.ErrScanInProgress, "ScanInProgress beats validation")
		if _, err := tx.Get([]byte("a")); err != nil {
			t.Errorf("Get in scan: %v", err)
		}
		// Nested scan allowed (rule 16).
		n := 0
		if err := tx.Scan(nil, nil, func(k, v []byte) bool { n++; return true }); err != nil || n != 3 {
			t.Errorf("nested scan: %v n=%d", err, n)
		}
		return true
	})
	if err != nil || visited != 3 {
		t.Fatalf("scan: %v visited=%d", err, visited)
	}
	// After the scan, the tx is usable and the buffer was not changed by the rejected calls.
	if err := tx.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	md := refmodel.New()
	for _, s := range []string{"a", "b", "c", "d"} {
		md.Put([]byte(s), []byte(map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}[s]))
	}
	assertModel(t, db, md)
	// Read-only tx: ErrTxReadOnly beats ErrScanInProgress.
	ro := beginT(t, db, false)
	ro.Scan(nil, nil, func(k, v []byte) bool {
		wantErr(t, ro.Put([]byte("x"), nil), anchordb.ErrTxReadOnly, "ro Put in scan")
		wantErr(t, ro.Rollback(), anchordb.ErrScanInProgress, "ro Rollback in scan")
		return false
	})
	ro.Rollback()
}

func TestScanFlagClearedAfterPanic(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	putT(t, db, "a", "1")
	tx := beginT(t, db, true)
	func() {
		defer func() { recover() }()
		tx.Scan(nil, nil, func(k, v []byte) bool { panic("boom") })
	}()
	if err := tx.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("scan flag not cleared after panic: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateViewSemantics(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	sentinel := errors.New("app error")
	err := db.Update(func(tx *anchordb.Tx) error {
		tx.Put([]byte("x"), []byte("1"))
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("Update should return fn's error unchanged, got %v", err)
	}
	if _, ok := getT(t, db, "x"); ok {
		t.Fatal("failed Update committed")
	}
	if err := db.Update(func(tx *anchordb.Tx) error { return tx.Put([]byte("y"), []byte("2")) }); err != nil {
		t.Fatal(err)
	}
	if v, _ := getT(t, db, "y"); v != "2" {
		t.Fatal("Update did not commit")
	}
	// fn commits itself → Update's Commit returns ErrTxClosed.
	err = db.Update(func(tx *anchordb.Tx) error {
		tx.Put([]byte("z"), []byte("3"))
		return tx.Commit()
	})
	wantErr(t, err, anchordb.ErrTxClosed, "Update after fn committed")
	if v, _ := getT(t, db, "z"); v != "3" {
		t.Fatal("fn's own commit lost")
	}
	// View is read-only and returns fn's error.
	err = db.View(func(tx *anchordb.Tx) error {
		if tx.Writable() {
			t.Error("View tx writable")
		}
		wantErr(t, tx.Put([]byte("q"), nil), anchordb.ErrTxReadOnly, "Put in View")
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("View err %v", err)
	}
	// Panics roll back and re-panic; the lock is released.
	for _, f := range []func(func(*anchordb.Tx) error) error{db.Update, db.View} {
		func() {
			defer func() {
				if r := recover(); r != "boom" {
					t.Fatalf("recovered %v", r)
				}
			}()
			f(func(tx *anchordb.Tx) error {
				if tx.Writable() {
					tx.Put([]byte("panic"), []byte("1"))
				}
				panic("boom")
			})
		}()
	}
	if _, ok := getT(t, db, "panic"); ok {
		t.Fatal("panicking Update committed")
	}
	done := make(chan struct{})
	go func() { db.Put([]byte("after-panic"), nil); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lock not released after panic")
	}
}

func TestTxTooLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("uses ~64 MiB")
	}
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	tx := beginT(t, db, true)
	defer tx.Rollback()
	val := make([]byte, anchordb.MaxValueSize)
	// payload = 12 + Σ(9 + len(k) + len(v)). Four 16 MiB values with 1-byte
	// keys: 12 + 4*(10 + 16Mi) = 64Mi + 52 > 64 MiB → the 4th Put fails.
	for i := 0; i < 3; i++ {
		if err := tx.Put([]byte{byte('a' + i)}, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	used := 12 + 3*(10+anchordb.MaxValueSize)
	room := anchordb.MaxTxBytes - used // bytes left for the 4th op
	// An op of exactly `room` bytes fits (inclusive limit).
	fit := room - 10
	wantErr(t, tx.Put([]byte("d"), make([]byte, fit+1)), anchordb.ErrTxTooLarge, "one byte over")
	if err := tx.Put([]byte("d"), make([]byte, fit)); err != nil {
		t.Fatalf("exactly MaxTxBytes rejected: %v", err)
	}
	// Now full: even a tiny delete overflows; overwriting is measured on the
	// collapsed buffer, so replacing d with a smaller value fits again.
	wantErr(t, tx.Delete([]byte("e")), anchordb.ErrTxTooLarge, "delete when full")
	if err := tx.Put([]byte("d"), make([]byte, fit-100)); err != nil {
		t.Fatalf("shrinking overwrite rejected: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit of max-size tx: %v", err)
	}
}
