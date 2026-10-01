package anchordb_test

// Shared helpers for the black-box test suite. Expected behaviour comes from
// docs/architecture.md; expected state comes from internal/refmodel.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func openT(t testing.TB, dir string, opts *anchordb.Options) *anchordb.DB {
	t.Helper()
	db, err := anchordb.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	// Release the directory lock if the test fails before closing (Windows
	// cannot remove a TempDir whose LOCK is held). Closing twice is harmless.
	t.Cleanup(func() { db.Close() })
	return db
}

func closeT(t testing.TB, db *anchordb.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func reopenT(t testing.TB, db *anchordb.DB, dir string, opts *anchordb.Options) *anchordb.DB {
	t.Helper()
	closeT(t, db)
	return openT(t, dir, opts)
}

func putT(t testing.TB, db *anchordb.DB, k, v string) {
	t.Helper()
	if err := db.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func getT(t testing.TB, db *anchordb.DB, k string) (string, bool) {
	t.Helper()
	v, err := db.Get([]byte(k))
	if errors.Is(err, anchordb.ErrNotFound) {
		if v != nil {
			t.Fatalf("Get(%q): ErrNotFound with non-nil value", k)
		}
		return "", false
	}
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	return string(v), true
}

// dump returns the whole DB contents in order.
func dump(t testing.TB, db *anchordb.DB) []refmodel.KV {
	t.Helper()
	var out []refmodel.KV
	err := db.Scan(nil, nil, func(k, v []byte) bool {
		out = append(out, refmodel.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out
}

func kvString(kvs []refmodel.KV) string {
	var sb strings.Builder
	for i, kv := range kvs {
		if i > 20 {
			fmt.Fprintf(&sb, "... (%d total)", len(kvs))
			break
		}
		fmt.Fprintf(&sb, "%q=%q ", kv.Key, trunc(kv.Value))
	}
	return sb.String()
}

func trunc(b []byte) []byte {
	if len(b) > 24 {
		return b[:24]
	}
	return b
}

// assertModel checks that the DB's full contents (via Scan and via Get for
// every model key) equal the model.
func assertModel(t testing.TB, db *anchordb.DB, md *refmodel.Model) {
	t.Helper()
	got := dump(t, db)
	if !md.Equal(got) {
		t.Fatalf("state mismatch:\n got: %s\nwant: %s", kvString(got), kvString(md.All()))
	}
	for _, kv := range md.All() {
		v, err := db.Get(kv.Key)
		if err != nil || !bytes.Equal(v, kv.Value) {
			t.Fatalf("Get(%q) = %q, %v; want %q", kv.Key, trunc(v), err, trunc(kv.Value))
		}
	}
}

func statsT(t testing.TB, db *anchordb.DB) anchordb.Stats {
	t.Helper()
	s, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	return s
}

func wantErr(t testing.TB, err, target error, what string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("%s: got %v, want %v", what, err, target)
	}
}

// listDir returns sorted file names in dir.
func listDir(t testing.TB, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func segments(t testing.TB, dir string) []string {
	var out []string
	for _, n := range listDir(t, dir) {
		if strings.HasPrefix(n, "wal-") && strings.HasSuffix(n, ".log") {
			out = append(out, n)
		}
	}
	return out
}

func checkpoints(t testing.TB, dir string) []string {
	var out []string
	for _, n := range listDir(t, dir) {
		if strings.HasPrefix(n, "checkpoint-") && strings.HasSuffix(n, ".ckpt") {
			out = append(out, n)
		}
	}
	return out
}

func lastSegment(t testing.TB, dir string) string {
	t.Helper()
	s := segments(t, dir)
	if len(s) == 0 {
		t.Fatal("no segments")
	}
	return filepath.Join(dir, s[len(s)-1])
}

// snapshot captures every file's bytes except LOCK.
func snapshot(t testing.TB, dir string) map[string][]byte {
	t.Helper()
	m := map[string][]byte{}
	for _, n := range listDir(t, dir) {
		if n == "LOCK" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		m[n] = b
	}
	return m
}

func assertUnchanged(t testing.TB, dir string, before map[string][]byte) {
	t.Helper()
	after := snapshot(t, dir)
	for n, b := range before {
		a, ok := after[n]
		if !ok {
			t.Fatalf("file %s was deleted by a failed Open", n)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("file %s was modified by a failed Open (%d → %d bytes)", n, len(b), len(a))
		}
	}
	for n := range after {
		if _, ok := before[n]; !ok {
			t.Fatalf("file %s was created by a failed Open", n)
		}
	}
}

// wantCorrupt asserts an Open failed with *CorruptionError naming file
// (empty = any) at offset (-1 = any).
func wantCorrupt(t testing.TB, err error, file string, offset int64) {
	t.Helper()
	if !errors.Is(err, anchordb.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	var ce *anchordb.CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CorruptionError, got %T %v", err, err)
	}
	if file != "" && ce.File != file {
		t.Fatalf("CorruptionError.File = %q, want %q (%v)", ce.File, file, err)
	}
	if offset >= 0 && ce.Offset != offset {
		t.Fatalf("CorruptionError.Offset = %d, want %d (%v)", ce.Offset, offset, err)
	}
}

// openCorrupt opens a damaged dir and checks the corruption contract:
// *CorruptionError, files unchanged, and the lock released afterwards.
func openCorrupt(t testing.TB, dir string, file string, offset int64) {
	t.Helper()
	before := snapshot(t, dir)
	db, err := anchordb.Open(dir, nil)
	if err == nil {
		db.Close()
		t.Fatalf("Open succeeded on corrupt directory (expected corruption in %s)", file)
	}
	wantCorrupt(t, err, file, offset)
	assertUnchanged(t, dir, before)
	// Failed Open must release the lock (§2.1.1): a second attempt fails the
	// same way rather than with ErrLocked.
	if _, err2 := anchordb.Open(dir, nil); errors.Is(err2, anchordb.ErrLocked) {
		t.Fatal("failed Open did not release the directory lock")
	}
}

func k(i int) []byte { return []byte(fmt.Sprintf("k%05d", i)) }

func readDirNames(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out, err
}

func fileSize(dir, name string) int64 {
	st, err := os.Stat(filepath.Join(dir, name))
	if err != nil {
		return 0
	}
	return st.Size()
}
