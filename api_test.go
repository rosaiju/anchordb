package anchordb_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func TestOpenCreatesDirAndInitialLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	db := openT(t, dir, nil)
	defer db.Close()
	names := listDir(t, dir)
	want := []string{"CURRENT", "LOCK", "wal-0000000000000001.log"}
	if len(names) != len(want) {
		t.Fatalf("files %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("files %v, want %v", names, want)
		}
	}
	cur, _ := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if !bytes.Equal(cur, refmodel.CurrentFile("", 0, 1)) {
		t.Fatalf("CURRENT = %q", cur)
	}
	seg, _ := os.ReadFile(filepath.Join(dir, "wal-0000000000000001.log"))
	if !bytes.Equal(seg, refmodel.SegmentHeader(1)) {
		t.Fatal("initial segment is not exactly a spec header")
	}
	s := statsT(t, db)
	if s.Keys != 0 || s.LastTxID != 0 || s.CheckpointTxID != 0 || s.WALStartSeq != 1 || s.ActiveSeq != 1 || s.WALBytes != 32 {
		t.Fatalf("initial stats %+v", s)
	}
}

func TestCRUD(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	defer db.Close()
	if _, ok := getT(t, db, "missing"); ok {
		t.Fatal("missing key found")
	}
	putT(t, db, "a", "1")
	putT(t, db, "b", "2")
	putT(t, db, "a", "3") // overwrite
	if v, _ := getT(t, db, "a"); v != "3" {
		t.Fatalf("a=%q", v)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, ok := getT(t, db, "b"); ok {
		t.Fatal("deleted key found")
	}
	// Deleting a missing key is not an error (§2.2 rule 13) and is logged (rule 9).
	before := statsT(t, db).LastTxID
	if err := db.Delete([]byte("never")); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if got := statsT(t, db).LastTxID; got != before+1 {
		t.Fatalf("delete of missing key consumed %d txids, want 1", got-before)
	}
	if s := statsT(t, db); s.Keys != 1 || s.LastTxID != 5 {
		t.Fatalf("stats %+v", s)
	}
}

func TestEmptyValueAndBinaryKeys(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	if err := db.Put([]byte("e"), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("e2"), []byte{}); err != nil {
		t.Fatal(err)
	}
	bin := []byte{0, 0xFF, 0, 1}
	if err := db.Put(bin, []byte{0}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		for _, key := range []string{"e", "e2"} {
			v, err := db.Get([]byte(key))
			if err != nil || v == nil || len(v) != 0 {
				t.Fatalf("Get(%s) = %#v, %v; want non-nil empty slice (§2.2 rule 12)", key, v, err)
			}
		}
		if v, err := db.Get(bin); err != nil || !bytes.Equal(v, []byte{0}) {
			t.Fatal("binary key")
		}
	}
	check()
	db = reopenT(t, db, dir, nil)
	check()
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	db = reopenT(t, db, dir, nil)
	check()
	closeT(t, db)
}

func TestKeyValueValidation(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	defer db.Close()
	wantErr(t, db.Put(nil, []byte("v")), anchordb.ErrEmptyKey, "Put nil key")
	wantErr(t, db.Put([]byte{}, []byte("v")), anchordb.ErrEmptyKey, "Put empty key")
	_, err := db.Get(nil)
	wantErr(t, err, anchordb.ErrEmptyKey, "Get empty key (§2.2 rule 14)")
	wantErr(t, db.Delete(nil), anchordb.ErrEmptyKey, "Delete empty key")
	big := make([]byte, anchordb.MaxKeySize+1)
	wantErr(t, db.Put(big, nil), anchordb.ErrKeyTooLarge, "Put big key")
	_, err = db.Get(big)
	wantErr(t, err, anchordb.ErrKeyTooLarge, "Get big key")
	wantErr(t, db.Delete(big), anchordb.ErrKeyTooLarge, "Delete big key")
	if anchordb.MaxKeySize != 65536 || anchordb.MaxValueSize != 16<<20 || anchordb.MaxTxBytes != 64<<20 {
		t.Fatal("exported limits differ from spec")
	}
	// Inclusive limits.
	maxKey := bytes.Repeat([]byte{'k'}, anchordb.MaxKeySize)
	if err := db.Put(maxKey, []byte("v")); err != nil {
		t.Fatalf("max-size key rejected: %v", err)
	}
	if testing.Short() {
		return
	}
	maxVal := bytes.Repeat([]byte{'v'}, anchordb.MaxValueSize)
	if err := db.Put([]byte("bigval"), maxVal); err != nil {
		t.Fatalf("max-size value rejected: %v", err)
	}
	wantErr(t, db.Put([]byte("x"), make([]byte, anchordb.MaxValueSize+1)), anchordb.ErrValueTooLarge, "Put big value")
	if s := statsT(t, db); s.LastTxID != 2 {
		t.Fatalf("rejected puts consumed txids: LastTxID=%d", s.LastTxID)
	}
}

func TestReturnedSlicesAreCopies(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	key, val := []byte("key"), []byte("value")
	if err := db.Put(key, val); err != nil {
		t.Fatal(err)
	}
	key[0], val[0] = 'X', 'X' // caller mutates its buffers after Put
	v, _ := db.Get([]byte("key"))
	if string(v) != "value" {
		t.Fatalf("Put did not copy: %q", v)
	}
	v[0] = 'Z'
	v2, _ := db.Get([]byte("key"))
	if string(v2) != "value" {
		t.Fatal("Get returned an alias of internal state")
	}
	db.Scan(nil, nil, func(k, v []byte) bool { k[0], v[0] = 'Q', 'Q'; return true })
	if v3, err := db.Get([]byte("key")); err != nil || string(v3) != "value" {
		t.Fatal("Scan passed aliases of internal state")
	}
}

func scanKeys(t *testing.T, scan func(start, end []byte, fn func(k, v []byte) bool) error, start, end []byte, limit int) []string {
	t.Helper()
	var out []string
	err := scan(start, end, func(k, v []byte) bool {
		out = append(out, string(k))
		return limit <= 0 || len(out) < limit
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestScanBoundaries(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	md := refmodel.New()
	for _, s := range []string{"a", "b", "ba", "bb", "c", "d", "\x00", "\xff"} {
		putT(t, db, s, "v"+s)
		md.Put([]byte(s), []byte("v"+s))
	}
	cases := []struct{ start, end []byte }{
		{nil, nil}, {[]byte{}, []byte{}}, {[]byte("b"), nil}, {nil, []byte("b")},
		{[]byte("b"), []byte("c")}, {[]byte("b"), []byte("b")}, {[]byte("c"), []byte("b")},
		{[]byte("bA"), []byte("bz")}, {[]byte("zzz"), nil}, {nil, []byte("\x00")},
		{[]byte("\x00"), []byte("\x00\x00")}, {[]byte("a"), []byte("a\x00")},
	}
	for _, c := range cases {
		var want []string
		for _, kv := range md.Scan(c.start, c.end) {
			want = append(want, string(kv.Key))
		}
		got := scanKeys(t, db.Scan, c.start, c.end, 0)
		if !eqStrings(got, want) {
			t.Errorf("Scan(%q,%q) = %q, want %q", c.start, c.end, got, want)
		}
	}
	// Early stop.
	if got := scanKeys(t, db.Scan, []byte("b"), nil, 2); !eqStrings(got, []string{"b", "ba"}) {
		t.Errorf("early stop: %q", got)
	}
	// Values are delivered correctly.
	db.Scan([]byte("c"), []byte("d"), func(k, v []byte) bool {
		if string(v) != "vc" {
			t.Errorf("value %q", v)
		}
		return true
	})
}

func TestReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	md := refmodel.New()
	db := openT(t, dir, nil)
	for i := 0; i < 200; i++ {
		putT(t, db, string(k(i)), "v")
		md.Put(k(i), []byte("v"))
	}
	for i := 0; i < 200; i += 3 {
		db.Delete(k(i))
		md.Delete(k(i))
	}
	db = reopenT(t, db, dir, nil)
	assertModel(t, db, md)
	s := statsT(t, db)
	if s.LastTxID != 267 || s.Recovery.ReplayedTxs != 267 || s.Keys != md.Len() {
		t.Fatalf("stats after reopen %+v", s)
	}
	// Txids continue after reopen.
	putT(t, db, "after", "x")
	if statsT(t, db).LastTxID != 268 {
		t.Fatal("txid did not continue")
	}
	closeT(t, db)
}

func TestClosedDB(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	closeT(t, db)
	wantErr(t, db.Close(), anchordb.ErrClosed, "second Close")
	wantErr(t, db.Put([]byte("a"), nil), anchordb.ErrClosed, "Put")
	_, err := db.Get([]byte("a"))
	wantErr(t, err, anchordb.ErrClosed, "Get")
	wantErr(t, db.Delete([]byte("a")), anchordb.ErrClosed, "Delete")
	wantErr(t, db.Scan(nil, nil, func(k, v []byte) bool { return true }), anchordb.ErrClosed, "Scan")
	_, err = db.Begin(true)
	wantErr(t, err, anchordb.ErrClosed, "Begin")
	wantErr(t, db.Update(func(*anchordb.Tx) error { return nil }), anchordb.ErrClosed, "Update")
	wantErr(t, db.View(func(*anchordb.Tx) error { return nil }), anchordb.ErrClosed, "View")
	wantErr(t, db.Checkpoint(), anchordb.ErrClosed, "Checkpoint")
	_, err = db.Stats()
	wantErr(t, err, anchordb.ErrClosed, "Stats")
	// Close released the lock.
	closeT(t, openT(t, dir, nil))
}

func TestInvalidOptions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	if db, err := anchordb.Open(dir, &anchordb.Options{SyncMode: anchordb.SyncMode(7)}); err == nil {
		db.Close()
		t.Fatal("invalid SyncMode accepted (§2.1)")
	}
	// "opens nothing": no data files may have been created.
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err == nil {
		t.Fatal("invalid-options Open created CURRENT")
	}
	for _, sz := range []int64{0, -5} {
		d := t.TempDir()
		db := openT(t, d, &anchordb.Options{SegmentSize: sz})
		for i := 0; i < 50; i++ {
			putT(t, db, string(k(i)), "v")
		}
		if s := statsT(t, db); s.ActiveSeq != 1 {
			t.Fatalf("SegmentSize %d should mean the 8 MiB default; ActiveSeq=%d", sz, s.ActiveSeq)
		}
		closeT(t, db)
	}
}

func TestSecondOpenInProcessIsLocked(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	_, err := anchordb.Open(dir, nil)
	wantErr(t, err, anchordb.ErrLocked, "second Open")
	closeT(t, db)
	closeT(t, openT(t, dir, nil))
}

func TestStatsWALBytes(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	defer db.Close()
	putT(t, db, "a", "1")
	ops := []refmodel.Op{{Key: []byte("a"), Value: []byte("1")}}
	want := int64(32 + 16 + refmodel.CommitPayloadSize(ops))
	if s := statsT(t, db); s.WALBytes != want {
		t.Fatalf("WALBytes=%d want %d", s.WALBytes, want)
	}
	st, _ := os.Stat(lastSegment(t, dir))
	if st.Size() != want {
		t.Fatalf("segment size %d want %d", st.Size(), want)
	}
}

var _ = errors.Is
