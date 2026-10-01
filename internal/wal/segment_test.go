package wal_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rosaiju/anchordb/internal/refmodel"
	"github.com/rosaiju/anchordb/internal/wal"
)

func TestSegmentNames(t *testing.T) {
	if wal.SegmentName(7) != "wal-0000000000000007.log" {
		t.Fatal(wal.SegmentName(7))
	}
	good := map[string]uint64{"wal-0000000000000007.log": 7, "wal-0000000000000000.log": 0}
	for n, want := range good {
		if got, ok := wal.ParseSegmentName(n); !ok || got != want {
			t.Errorf("%s: %d %v", n, got, ok)
		}
	}
	for _, n := range []string{"wal-7.log", "wal-0000000000000007.log.tmp", "WAL-0000000000000007.log",
		"wal-000000000000000x.log", "wal-+000000000000007.log", "wal-0000000000000007.lo", "xwal-0000000000000007.log"} {
		if _, ok := wal.ParseSegmentName(n); ok {
			t.Errorf("%s: accepted (spec §4 pattern ^wal-[0-9]{16}\\.log$)", n)
		}
	}
}

func TestSegmentHeaderMatchesReference(t *testing.T) {
	if !bytes.Equal(wal.EncodeSegmentHeader(9), refmodel.SegmentHeader(9)) || wal.SegmentHeaderSize != 32 {
		t.Fatal("segment header differs from spec §4.2")
	}
}

func commitFrame(txid uint64, key string) []byte {
	return refmodel.Frame(1, refmodel.CommitPayload(txid, []refmodel.Op{{Key: []byte(key), Value: []byte("value-" + key)}}))
}

// seg builds a segment image with n records (txids 1..n) and returns the
// image and each record's start offset.
func seg(seq uint64, n int) ([]byte, []int) {
	img := refmodel.SegmentHeader(seq)
	var offs []int
	for i := 1; i <= n; i++ {
		offs = append(offs, len(img))
		img = append(img, commitFrame(uint64(i), string(rune('a'+i)))...)
	}
	return img, offs
}

func scan(img []byte, seq uint64, final bool) (wal.ScanResult, []uint64, error) {
	var ids []uint64
	r, err := wal.ScanSegment(img, seq, final, func(c wal.Commit) error { ids = append(ids, c.TxID); return nil })
	return r, ids, err
}

func wantFormatErr(t *testing.T, err error, off int64, what string) {
	t.Helper()
	var fe *wal.FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("%s: want *FormatError, got %v", what, err)
	}
	if fe.Offset != off {
		t.Fatalf("%s: offset %d, want %d", what, fe.Offset, off)
	}
}

func TestScanCleanAndHeaderOnly(t *testing.T) {
	img, _ := seg(3, 3)
	for _, final := range []bool{true, false} {
		r, ids, err := scan(img, 3, final)
		if err != nil || r.Torn || r.ValidEnd != int64(len(img)) || len(ids) != 3 {
			t.Fatalf("final=%v: %+v %v %v", final, r, ids, err)
		}
		r, ids, err = scan(refmodel.SegmentHeader(3), 3, final)
		if err != nil || r.Torn || r.ValidEnd != 32 || len(ids) != 0 {
			t.Fatalf("header-only final=%v: %+v %v", final, r, err)
		}
	}
}

func TestScanBadSegmentHeader(t *testing.T) {
	img, _ := seg(3, 1)
	cases := map[string][]byte{
		"short":     img[:31],
		"empty":     nil,
		"magic":     mutate(img, func(b []byte) []byte { b[0] = 'X'; return b }),
		"version":   append(refmodel.SegmentHeader(3)[:0:0], img...),
		"crc":       mutate(img, func(b []byte) []byte { b[25] ^= 1; return b }),
		"seq field": append(refmodel.SegmentHeader(4), img[32:]...),
	}
	// Build a version-2 header with a valid CRC.
	v := refmodel.SegmentHeader(3)
	v[8] = 2
	copy(v[24:28], le32(refmodel.CRC(v[:24])))
	cases["version"] = append(v, img[32:]...)
	for name, b := range cases {
		for _, final := range []bool{true, false} {
			_, _, err := scan(b, 3, final)
			wantFormatErr(t, err, 0, name)
		}
	}
}

func le32(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }

// §6.2: truncating the final segment anywhere inside the last record is a
// torn tail ending at that record's start; in a non-final segment it is
// corruption at that offset.
func TestScanTornTailEveryOffset(t *testing.T) {
	img, offs := seg(1, 3)
	last := offs[2]
	for cut := last + 1; cut < len(img); cut++ {
		r, ids, err := scan(img[:cut], 1, true)
		if err != nil || !r.Torn || r.ValidEnd != int64(last) || len(ids) != 2 {
			t.Fatalf("final cut %d: %+v ids=%v err=%v", cut, r, ids, err)
		}
		_, _, err = scan(img[:cut], 1, false)
		wantFormatErr(t, err, int64(last), "non-final torn")
	}
}

func TestScanZeroTail(t *testing.T) {
	img, _ := seg(1, 2)
	for _, z := range []int{1, 15, 16, 17, 4096} {
		b := append(append([]byte{}, img...), make([]byte, z)...)
		r, ids, err := scan(b, 1, true)
		if err != nil || !r.Torn || r.ValidEnd != int64(len(img)) || len(ids) != 2 {
			t.Fatalf("zeros %d final: %+v %v", z, r, err)
		}
		_, _, err = scan(b, 1, false)
		wantFormatErr(t, err, int64(len(img)), "zeros non-final")
	}
}

// §6.2: 1..15 trailing bytes of anything are a torn tail in the final segment.
func TestScanShortGarbageTail(t *testing.T) {
	img, _ := seg(1, 1)
	for n := 1; n < 16; n++ {
		b := append(append([]byte{}, img...), bytes.Repeat([]byte{0xA5}, n)...)
		r, _, err := scan(b, 1, true)
		if err != nil || !r.Torn || r.ValidEnd != int64(len(img)) {
			t.Fatalf("garbage %d: %+v %v", n, r, err)
		}
	}
}

// §6.2: header OK, partial payload, then zeros past the declared end → corrupt.
func TestScanZerosInsideFrameIsCorrupt(t *testing.T) {
	img, offs := seg(1, 2)
	last := offs[1]
	half := last + (len(img)-last)/2
	b := append(append([]byte{}, img[:half]...), make([]byte, 64)...)
	_, _, err := scan(b, 1, true)
	wantFormatErr(t, err, int64(last), "zeros inside frame")
}

func TestScanNonZeroGarbageAfterHeaderIsCorrupt(t *testing.T) {
	img, _ := seg(1, 1)
	b := append(append([]byte{}, img...), bytes.Repeat([]byte{0xA5}, 40)...)
	_, _, err := scan(b, 1, true)
	wantFormatErr(t, err, int64(len(img)), "garbage tail")
	// All zeros except one byte far away → not all-zero → corrupt.
	z := make([]byte, 100)
	z[99] = 1
	_, _, err = scan(append(append([]byte{}, img...), z...), 1, true)
	wantFormatErr(t, err, int64(len(img)), "mostly zero tail")
}

// Every bit flip inside any complete record is corruption reported at that
// record's offset, even in the final segment.
func TestScanBitFlipsAreCorruption(t *testing.T) {
	img, offs := seg(1, 3)
	ends := append(offs[1:], len(img))
	for r := range offs {
		for i := offs[r]; i < ends[r]; i++ {
			b := append([]byte{}, img...)
			b[i] ^= 0x10
			_, _, err := scan(b, 1, true)
			wantFormatErr(t, err, int64(offs[r]), "bit flip")
		}
	}
}

func TestScanWrongTypeAndBadPayload(t *testing.T) {
	img := refmodel.SegmentHeader(1)
	b := append(append([]byte{}, img...), refmodel.Frame(2, refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("a")}}))...)
	_, _, err := scan(b, 1, true)
	wantFormatErr(t, err, 32, "wrong type")
	b = append(append([]byte{}, img...), refmodel.Frame(1, []byte("not a commit"))...)
	_, _, err = scan(b, 1, true)
	wantFormatErr(t, err, 32, "bad payload")
	b = append(append([]byte{}, img...), refmodel.Frame(1, refmodel.CommitPayload(1, nil))...)
	_, _, err = scan(b, 1, true)
	wantFormatErr(t, err, 32, "zero ops")
}

func TestScanCallbackErrorPropagates(t *testing.T) {
	img, _ := seg(1, 3)
	sentinel := errors.New("stop")
	n := 0
	_, err := wal.ScanSegment(img, 1, true, func(wal.Commit) error {
		n++
		if n == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || n != 2 {
		t.Fatalf("err=%v n=%d", err, n)
	}
}

func FuzzScanSegment(f *testing.F) {
	img, _ := seg(1, 2)
	f.Add(img, true)
	f.Add(img[:len(img)-3], true)
	f.Add(append(img, make([]byte, 20)...), false)
	f.Fuzz(func(t *testing.T, b []byte, final bool) {
		r, err := wal.ScanSegment(b, 1, final, func(wal.Commit) error { return nil })
		recs, stop, rerr := refmodel.ParseSegment(b)
		_ = recs
		if err != nil {
			var fe *wal.FormatError
			if !errors.As(err, &fe) {
				t.Fatalf("non-FormatError %v", err)
			}
			return
		}
		if r.ValidEnd < 32 || r.ValidEnd > int64(len(b)) {
			t.Fatalf("ValidEnd %d out of range", r.ValidEnd)
		}
		if r.Torn && !final {
			t.Fatal("torn tail reported in non-final segment")
		}
		if !r.Torn && (rerr != nil || r.ValidEnd != int64(len(b))) {
			t.Fatalf("clean end but reference stopped at %d: %v", stop, rerr)
		}
		if r.Torn && int64(stop) != r.ValidEnd {
			t.Fatalf("torn at %d, reference stopped at %d", r.ValidEnd, stop)
		}
	})
}
