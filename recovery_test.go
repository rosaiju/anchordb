package anchordb_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

// buildDB writes n single-key transactions (txid i writes k(i)=val(i)) and
// closes the DB. Returns the model.
func buildDB(t *testing.T, dir string, n int, opts *anchordb.Options) *refmodel.Model {
	t.Helper()
	db := openT(t, dir, opts)
	md := refmodel.New()
	for i := 1; i <= n; i++ {
		v := []byte(fmt.Sprintf("value-%d-%s", i, bytes.Repeat([]byte("x"), i%7)))
		if err := db.Put(k(i), v); err != nil {
			t.Fatal(err)
		}
		md.Apply([]refmodel.Op{{Key: k(i), Value: v}})
	}
	closeT(t, db)
	return md
}

// prefixModel returns the model after the first n transactions of buildDB.
func prefixModel(n int) *refmodel.Model {
	md := refmodel.New()
	for i := 1; i <= n; i++ {
		md.Apply([]refmodel.Op{{Key: k(i), Value: []byte(fmt.Sprintf("value-%d-%s", i, bytes.Repeat([]byte("x"), i%7)))}})
	}
	return md
}

func readFile(t testing.TB, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeFile(t testing.TB, p string, b []byte) {
	t.Helper()
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func parseSeg(t testing.TB, p string) []refmodel.SegRecord {
	t.Helper()
	recs, _, err := refmodel.ParseSegment(readFile(t, p))
	if err != nil {
		t.Fatalf("reference parser rejects engine segment %s: %v", filepath.Base(p), err)
	}
	return recs
}

// §6.2: truncating the final segment at every byte offset inside the last
// record is a torn tail: Open succeeds, drops exactly that record, truncates
// the file, reports TruncatedBytes, and the DB remains writable.
func TestTornTailEveryOffset(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, 3, nil)
	seg := lastSegment(t, dir)
	orig := readFile(t, seg)
	recs := parseSeg(t, seg)
	last := recs[len(recs)-1]
	if last.Offset+last.Len != len(orig) {
		t.Fatal("segment has bytes after the last record")
	}
	for cut := last.Offset; cut < len(orig); cut++ {
		writeFile(t, seg, orig[:cut])
		db, err := anchordb.Open(dir, nil)
		if err != nil {
			t.Fatalf("cut at %d (record %d..%d): Open: %v", cut, last.Offset, len(orig), err)
		}
		assertModel(t, db, prefixModel(2))
		s := statsT(t, db)
		if s.LastTxID != 2 || s.Recovery.TruncatedBytes != int64(cut-last.Offset) || s.Recovery.ReplayedTxs != 2 {
			t.Fatalf("cut %d: stats %+v", cut, s)
		}
		if sz := fileSize(dir, filepath.Base(seg)); sz != int64(last.Offset) {
			t.Fatalf("cut %d: file not truncated to %d (is %d)", cut, last.Offset, sz)
		}
		closeT(t, db)
	}
}

// §5.3 step 9: after truncating a torn tail, new commits must land at the
// new end of file (O_APPEND), so the log stays readable.
func TestAppendAfterTornTailTruncation(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, 3, nil)
	seg := lastSegment(t, dir)
	orig := readFile(t, seg)
	writeFile(t, seg, orig[:len(orig)-5])
	db := openT(t, dir, nil)
	putT(t, db, "new", "value")
	db = reopenT(t, db, dir, nil)
	md := prefixModel(2)
	md.Put([]byte("new"), []byte("value"))
	assertModel(t, db, md)
	if s := statsT(t, db); s.LastTxID != 3 || s.Recovery.TruncatedBytes != 0 {
		t.Fatalf("stats %+v", s)
	}
	closeT(t, db)
	if _, _, err := refmodel.ParseSegment(readFile(t, seg)); err != nil {
		t.Fatalf("segment unreadable after truncate+append: %v", err)
	}
}

func TestZeroFilledTail(t *testing.T) {
	for _, z := range []int{1, 15, 16, 17, 512, 4096} {
		dir := t.TempDir()
		buildDB(t, dir, 3, nil)
		seg := lastSegment(t, dir)
		orig := readFile(t, seg)
		writeFile(t, seg, append(append([]byte{}, orig...), make([]byte, z)...))
		db := openT(t, dir, nil)
		assertModel(t, db, prefixModel(3))
		if s := statsT(t, db); s.Recovery.TruncatedBytes != int64(z) {
			t.Fatalf("zeros %d: TruncatedBytes=%d", z, s.Recovery.TruncatedBytes)
		}
		closeT(t, db)
		if !bytes.Equal(readFile(t, seg), orig) {
			t.Fatalf("zeros %d: segment not restored to valid end", z)
		}
	}
}

func TestShortGarbageTailIsTorn(t *testing.T) {
	for n := 1; n < 16; n++ {
		dir := t.TempDir()
		buildDB(t, dir, 2, nil)
		seg := lastSegment(t, dir)
		orig := readFile(t, seg)
		writeFile(t, seg, append(append([]byte{}, orig...), bytes.Repeat([]byte{0xC3}, n)...))
		db := openT(t, dir, nil)
		assertModel(t, db, prefixModel(2))
		closeT(t, db)
	}
}

// §6.2: a bit flip in any byte of a complete record is corruption, even in
// the last record of the final segment. Open must fail with a
// *CorruptionError naming the file and frame offset, and modify nothing.
func TestBitFlipIsCorruption(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, 3, nil)
	seg := lastSegment(t, dir)
	orig := readFile(t, seg)
	recs := parseSeg(t, seg)
	for _, ri := range []int{1, 2} { // a middle record and the last record
		r := recs[ri]
		step := 1
		if testing.Short() {
			step = 5
		}
		for i := r.Offset; i < r.Offset+r.Len; i += step {
			b := append([]byte{}, orig...)
			b[i] ^= 0x01
			writeFile(t, seg, b)
			openCorrupt(t, dir, filepath.Base(seg), int64(r.Offset))
		}
	}
	writeFile(t, seg, orig)
	closeT(t, openT(t, dir, nil))
}

func TestZerosInsideFrameIsCorruption(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, 3, nil)
	seg := lastSegment(t, dir)
	orig := readFile(t, seg)
	recs := parseSeg(t, seg)
	last := recs[len(recs)-1]
	half := last.Offset + last.Len/2
	writeFile(t, seg, append(append([]byte{}, orig[:half]...), make([]byte, last.Len)...))
	openCorrupt(t, dir, filepath.Base(seg), int64(last.Offset))
}

func TestBadSegmentHeader(t *testing.T) {
	cases := map[string]func([]byte) []byte{
		"short":  func(b []byte) []byte { return b[:20] },
		"empty":  func(b []byte) []byte { return nil },
		"magic":  func(b []byte) []byte { b[1] ^= 1; return b },
		"crc":    func(b []byte) []byte { b[24] ^= 1; return b },
		"seq":    func(b []byte) []byte { return append(refmodel.SegmentHeader(99), b[32:]...) },
		"zeroed": func(b []byte) []byte { return make([]byte, len(b)) },
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			buildDB(t, dir, 2, nil)
			seg := lastSegment(t, dir)
			writeFile(t, seg, f(readFile(t, seg)))
			openCorrupt(t, dir, filepath.Base(seg), 0)
		})
	}
}

// multiSegDB builds a DB whose log spans several segments.
func multiSegDB(t *testing.T, dir string, n int) ([]string, *refmodel.Model) {
	md := buildDB(t, dir, n, &anchordb.Options{SegmentSize: 200})
	segs := segments(t, dir)
	if len(segs) < 3 {
		t.Fatalf("expected several segments, got %v", segs)
	}
	return segs, md
}

func TestMultiSegmentReplay(t *testing.T) {
	dir := t.TempDir()
	segs, md := multiSegDB(t, dir, 30)
	db := openT(t, dir, nil)
	assertModel(t, db, md)
	s := statsT(t, db)
	if s.WALStartSeq != 1 || s.ActiveSeq != uint64(len(segs)) || s.LastTxID != 30 || s.Recovery.ReplayedTxs != 30 {
		t.Fatalf("stats %+v (segments %v)", s, segs)
	}
	closeT(t, db)
	// Each non-final segment must hold at least one record and segment sizes
	// must respect the rotation rule (§5.2 step 2): a record is appended only
	// if the segment was below SegmentSize before it.
	for _, n := range segs[:len(segs)-1] {
		recs := parseSeg(t, filepath.Join(dir, n))
		if len(recs) == 0 {
			t.Fatalf("%s: commit-triggered rotation left an empty segment", n)
		}
		lastRec := recs[len(recs)-1]
		if lastRec.Offset >= 200 {
			t.Fatalf("%s: record appended at offset %d although segment was already >= SegmentSize", n, lastRec.Offset)
		}
	}
}

func TestNonFinalSegmentDamageIsCorruption(t *testing.T) {
	type mut func(t *testing.T, p string) int64 // returns expected offset
	cases := map[string]mut{
		"truncated": func(t *testing.T, p string) int64 {
			b := readFile(t, p)
			recs := parseSeg(t, p)
			l := recs[len(recs)-1]
			writeFile(t, p, b[:l.Offset+3])
			return int64(l.Offset)
		},
		"zero tail": func(t *testing.T, p string) int64 {
			b := readFile(t, p)
			writeFile(t, p, append(b, make([]byte, 40)...))
			return int64(len(b))
		},
		"bitflip": func(t *testing.T, p string) int64 {
			b := readFile(t, p)
			recs := parseSeg(t, p)
			b[recs[0].Offset+20] ^= 0x80
			writeFile(t, p, b)
			return int64(recs[0].Offset)
		},
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			segs, _ := multiSegDB(t, dir, 30)
			victim := segs[1]
			off := f(t, filepath.Join(dir, victim))
			openCorrupt(t, dir, victim, off)
		})
	}
}

func TestMissingSegment(t *testing.T) {
	for _, which := range []string{"first", "middle", "all"} {
		t.Run(which, func(t *testing.T) {
			dir := t.TempDir()
			segs, _ := multiSegDB(t, dir, 30)
			switch which {
			case "first":
				os.Remove(filepath.Join(dir, segs[0]))
			case "middle":
				os.Remove(filepath.Join(dir, segs[1]))
			case "all":
				for _, s := range segs {
					os.Remove(filepath.Join(dir, s))
				}
			}
			openCorrupt(t, dir, "", -1)
		})
	}
}

// Documented limitation (§6.2, §11): losing the whole final segment is not
// detectable. This test pins the documented behaviour (Open succeeds with a
// prefix of the history) so a change in it is noticed.
func TestMissingFinalSegmentIsUndetectable(t *testing.T) {
	dir := t.TempDir()
	segs, _ := multiSegDB(t, dir, 30)
	recs := parseSeg(t, filepath.Join(dir, segs[len(segs)-2]))
	lastTx := recs[len(recs)-1].TxID
	os.Remove(filepath.Join(dir, segs[len(segs)-1]))
	db := openT(t, dir, nil)
	assertModel(t, db, prefixModel(int(lastTx)))
	closeT(t, db)
}

func TestTxidDiscontinuityIsCorruption(t *testing.T) {
	for _, delta := range []int{0, 2, -1} {
		dir := t.TempDir()
		buildDB(t, dir, 3, nil)
		seg := lastSegment(t, dir)
		b := readFile(t, seg)
		off := len(b)
		bad := uint64(int(3) + 1 + delta)
		if delta == -1 {
			bad = 3 // duplicate
		}
		b = append(b, refmodel.Frame(1, refmodel.CommitPayload(bad, []refmodel.Op{{Key: []byte("z"), Value: []byte("1")}}))...)
		writeFile(t, seg, b)
		if delta == 0 {
			db := openT(t, dir, nil) // correct txid 4: accepted
			if v, _ := getT(t, db, "z"); v != "1" || statsT(t, db).LastTxID != 4 {
				t.Fatal("hand-written valid record not replayed")
			}
			closeT(t, db)
			continue
		}
		openCorrupt(t, dir, filepath.Base(seg), int64(off))
	}
}

func TestWrongRecordTypeIsCorruption(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, 2, nil)
	seg := lastSegment(t, dir)
	b := readFile(t, seg)
	off := len(b)
	writeFile(t, seg, append(b, refmodel.Frame(3, refmodel.CommitPayload(3, []refmodel.Op{{Key: []byte("z")}}))...))
	openCorrupt(t, dir, filepath.Base(seg), int64(off))
}

func TestBadCURRENT(t *testing.T) {
	cases := map[string]func([]byte) []byte{
		"bitflip":   func(b []byte) []byte { b[5] ^= 1; return b },
		"truncated": func(b []byte) []byte { return b[:len(b)-3] },
		"empty":     func(b []byte) []byte { return nil },
		"garbage":   func(b []byte) []byte { return []byte("hello\n") },
		"crlf":      func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\n"), []byte("\r\n")) },
		"missing ckpt": func(b []byte) []byte {
			return refmodel.CurrentFile(refmodel.CheckpointName(2), 2, 1)
		},
		"wal_start beyond log": func(b []byte) []byte { return refmodel.CurrentFile("", 0, 5) },
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			buildDB(t, dir, 3, nil)
			p := filepath.Join(dir, "CURRENT")
			writeFile(t, p, f(readFile(t, p)))
			file := "CURRENT"
			if name == "missing ckpt" || name == "wal_start beyond log" {
				file = ""
			}
			openCorrupt(t, dir, file, -1)
		})
	}
}

func TestCURRENTMissing(t *testing.T) {
	t.Run("with data", func(t *testing.T) {
		dir := t.TempDir()
		buildDB(t, dir, 3, nil)
		os.Remove(filepath.Join(dir, "CURRENT"))
		openCorrupt(t, dir, "", -1)
	})
	t.Run("header-only wal-1 (crash during init)", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, refmodel.SegmentName(1)), refmodel.SegmentHeader(1))
		db := openT(t, dir, nil)
		if s := statsT(t, db); s.Keys != 0 || s.LastTxID != 0 {
			t.Fatalf("stats %+v", s)
		}
		putT(t, db, "a", "1")
		closeT(t, db)
		if !bytes.Equal(readFile(t, filepath.Join(dir, "CURRENT")), refmodel.CurrentFile("", 0, 1)) {
			t.Fatal("CURRENT not published as spec'd")
		}
	})
	t.Run("wal-1 with a record", func(t *testing.T) {
		dir := t.TempDir()
		img := append(refmodel.SegmentHeader(1), refmodel.Frame(1, refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("a"), Value: []byte("1")}}))...)
		writeFile(t, filepath.Join(dir, refmodel.SegmentName(1)), img)
		openCorrupt(t, dir, "", -1)
	})
	t.Run("only wal-2", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, refmodel.SegmentName(2)), refmodel.SegmentHeader(2))
		openCorrupt(t, dir, "", -1)
	})
	t.Run("only a checkpoint", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, refmodel.CheckpointName(1)), refmodel.CheckpointFile(1, nil, 0))
		openCorrupt(t, dir, "", -1)
	})
	t.Run("unrelated files only → init", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "README.txt"), []byte("hi"))
		writeFile(t, filepath.Join(dir, "CURRENT.tmp"), []byte("partial"))
		db := openT(t, dir, nil)
		closeT(t, db)
		names := listDir(t, dir)
		want := []string{"CURRENT", "LOCK", "README.txt", "wal-0000000000000001.log"}
		if fmt.Sprint(names) != fmt.Sprint(want) {
			t.Fatalf("files %v want %v", names, want)
		}
	})
}

func TestCheckpointFileCorruption(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	for i := 0; i < 20; i++ {
		putT(t, db, string(k(i)), "v")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closeT(t, db)
	cks := checkpoints(t, dir)
	if len(cks) != 1 {
		t.Fatalf("checkpoints %v", cks)
	}
	p := filepath.Join(dir, cks[0])
	orig := readFile(t, p)
	for _, i := range []int{0, 17, 40, len(orig) / 2, len(orig) - 1} {
		b := append([]byte{}, orig...)
		b[i] ^= 0x02
		writeFile(t, p, b)
		openCorrupt(t, dir, cks[0], -1)
	}
	writeFile(t, p, orig[:len(orig)-1])
	openCorrupt(t, dir, cks[0], -1)
	writeFile(t, p, orig)
	closeT(t, openT(t, dir, nil))
}

// §5.3 step 8 and §4 name patterns: engine temp files, segments below
// wal_start and unreferenced checkpoints are removed; anything else is left.
func TestOpenRemovesGarbageOnly(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	for i := 0; i < 5; i++ {
		putT(t, db, string(k(i)), "v")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	putT(t, db, "after", "x")
	s := statsT(t, db)
	closeT(t, db)
	garbage := []string{
		refmodel.SegmentName(s.ActiveSeq+1) + ".tmp",
		refmodel.CheckpointName(99) + ".tmp",
		"CURRENT.tmp",
		refmodel.SegmentName(s.WALStartSeq - 1), // below wal_start (stale)
		refmodel.CheckpointName(3),              // unreferenced checkpoint
	}
	keep := []string{"notes.txt", "foo.tmp", "wal-7.log", "CURRENT.bak", "checkpoint-3.ckpt", "wal-0000000000000001.log.bak"}
	// The stale segment must not already exist for the test to be meaningful.
	for _, g := range garbage {
		writeFile(t, filepath.Join(dir, g), []byte("garbage"))
	}
	for _, g := range keep {
		writeFile(t, filepath.Join(dir, g), []byte("user file"))
	}
	db = openT(t, dir, nil)
	rs := statsT(t, db)
	closeT(t, db)
	names := map[string]bool{}
	for _, n := range listDir(t, dir) {
		names[n] = true
	}
	for _, g := range garbage {
		if names[g] {
			t.Errorf("garbage %s not removed", g)
		}
	}
	for _, g := range keep {
		if !names[g] {
			t.Errorf("non-engine file %s was deleted (§4: ignored and never deleted)", g)
		}
	}
	if rs.Recovery.RemovedFiles != len(garbage) {
		t.Errorf("RemovedFiles=%d want %d", rs.Recovery.RemovedFiles, len(garbage))
	}
}

// Validate first, modify later (§5.3): if any file is corrupt, the torn
// tail elsewhere is not truncated and garbage is not deleted.
func TestFailedOpenModifiesNothing(t *testing.T) {
	dir := t.TempDir()
	segs, _ := multiSegDB(t, dir, 30)
	last := filepath.Join(dir, segs[len(segs)-1])
	b := readFile(t, last)
	writeFile(t, last, b[:len(b)-4]) // torn tail in final segment
	writeFile(t, filepath.Join(dir, "CURRENT.tmp"), []byte("x"))
	writeFile(t, filepath.Join(dir, refmodel.CheckpointName(77)), []byte("orphan"))
	mid := filepath.Join(dir, segs[1])
	m := readFile(t, mid)
	m[len(m)-1] ^= 0xFF // corrupt the last record of a non-final segment
	writeFile(t, mid, m)
	openCorrupt(t, dir, segs[1], -1)
}

// The engine must open a directory written entirely by the spec-derived
// reference encoders (format compatibility in the read direction).
func TestOpenHandBuiltDirectory(t *testing.T) {
	dir := t.TempDir()
	base := []refmodel.KV{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte{}}, {Key: []byte("c"), Value: []byte("3")}}
	writeFile(t, filepath.Join(dir, refmodel.CheckpointName(5)), refmodel.CheckpointFile(5, base, 2))
	s3 := append(refmodel.SegmentHeader(3),
		refmodel.Frame(1, refmodel.CommitPayload(6, []refmodel.Op{{Key: []byte("a"), Delete: true}, {Key: []byte("d"), Value: []byte("4")}}))...)
	s4 := refmodel.SegmentHeader(4) // empty middle segment is legal
	s5 := append(refmodel.SegmentHeader(5), refmodel.Frame(1, refmodel.CommitPayload(7, []refmodel.Op{{Key: []byte("b"), Value: []byte("2")}}))...)
	writeFile(t, filepath.Join(dir, refmodel.SegmentName(3)), s3)
	writeFile(t, filepath.Join(dir, refmodel.SegmentName(4)), s4)
	writeFile(t, filepath.Join(dir, refmodel.SegmentName(5)), s5)
	writeFile(t, filepath.Join(dir, "CURRENT"), refmodel.CurrentFile(refmodel.CheckpointName(5), 5, 3))
	db := openT(t, dir, nil)
	md := refmodel.New()
	md.Put([]byte("b"), []byte("2"))
	md.Put([]byte("c"), []byte("3"))
	md.Put([]byte("d"), []byte("4"))
	assertModel(t, db, md)
	s := statsT(t, db)
	if s.LastTxID != 7 || s.CheckpointTxID != 5 || s.WALStartSeq != 3 || s.ActiveSeq != 5 || s.Recovery.ReplayedTxs != 2 {
		t.Fatalf("stats %+v", s)
	}
	putT(t, db, "e", "5")
	closeT(t, db)
	recs := parseSeg(t, filepath.Join(dir, refmodel.SegmentName(5)))
	if len(recs) != 2 || recs[1].TxID != 8 {
		t.Fatal("engine did not append txid 8 to the last hand-built segment")
	}
}

// Engine-written files must be readable by the reference decoders and
// describe exactly the engine's state (format compatibility, write direction).
func TestEngineFilesMatchReferenceFormat(t *testing.T) {
	dir := t.TempDir()
	opts := &anchordb.Options{SegmentSize: 300}
	db := openT(t, dir, opts)
	md := refmodel.New()
	for i := 0; i < 40; i++ {
		ops := []refmodel.Op{{Key: k(i % 13), Value: []byte(fmt.Sprint(i))}}
		if i%5 == 0 {
			ops = append(ops, refmodel.Op{Key: k(100 + i), Delete: true})
		}
		err := db.Update(func(tx *anchordb.Tx) error {
			for _, op := range ops {
				if op.Delete {
					tx.Delete(op.Key)
				} else {
					tx.Put(op.Key, op.Value)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		mtx := md.Begin()
		for _, op := range ops {
			if op.Delete {
				mtx.Delete(op.Key)
			} else {
				mtx.Put(op.Key, op.Value)
			}
		}
		mtx.Commit()
		if i == 20 {
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
	}
	closeT(t, db)
	// Rebuild state purely from files with reference decoders.
	cur := string(readFile(t, filepath.Join(dir, "CURRENT")))
	var ckName string
	var ckTx, walStart uint64
	if _, err := fmt.Sscanf(cur, "ANCHORDB-CURRENT 1\ncheckpoint %s\ncheckpoint_txid %d\nwal_start %d\n", &ckName, &ckTx, &walStart); err != nil {
		t.Fatalf("CURRENT %q: %v", cur, err)
	}
	if !bytes.Equal([]byte(cur), refmodel.CurrentFile(ckName, ckTx, walStart)) {
		t.Fatal("CURRENT does not match the reference encoding")
	}
	state := map[string][]byte{}
	img := readFile(t, filepath.Join(dir, ckName))
	if !bytes.Equal(img[:32], refmodel.CheckpointHeader(ckTx)) {
		t.Fatal("checkpoint header")
	}
	for off := 32; off < len(img); {
		typ, p, n, err := refmodel.DecodeFrame(img[off:])
		if err != nil {
			t.Fatal(err)
		}
		off += n
		if typ == 2 {
			es, err := refmodel.ParseEntries(p)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range es {
				state[string(e.Key)] = e.Value
			}
		}
	}
	next := ckTx + 1
	for seq := walStart; ; seq++ {
		p := filepath.Join(dir, refmodel.SegmentName(seq))
		if _, err := os.Stat(p); err != nil {
			break
		}
		for _, r := range parseSeg(t, p) {
			if r.TxID != next {
				t.Fatalf("txid %d, want %d", r.TxID, next)
			}
			next++
			for _, op := range r.Ops {
				if op.Delete {
					delete(state, string(op.Key))
				} else {
					state[string(op.Key)] = op.Value
				}
			}
		}
	}
	if next-1 != md.LastTxID {
		t.Fatalf("files contain %d txs, model %d", next-1, md.LastTxID)
	}
	if !md.Equal(refmodel.ScanMap(state, nil, nil)) {
		t.Fatal("state rebuilt from files differs from model")
	}
}
