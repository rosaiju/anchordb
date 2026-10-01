package wal_test

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/rosaiju/anchordb/internal/refmodel"
	"github.com/rosaiju/anchordb/internal/wal"
)

func toRef(ops []wal.Op) []refmodel.Op {
	out := make([]refmodel.Op, len(ops))
	for i, o := range ops {
		out[i] = refmodel.Op{Delete: o.Delete, Key: o.Key, Value: o.Value}
	}
	return out
}

func sampleCommit() wal.Commit {
	return wal.Commit{TxID: 42, Ops: []wal.Op{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Delete: true},
		{Key: []byte("c"), Value: []byte{}},
		{Key: []byte("d\x00e"), Value: bytes.Repeat([]byte{9}, 300)},
	}}
}

func TestLimitsMatchSpec(t *testing.T) {
	if wal.MaxKeySize != 65536 || wal.MaxValueSize != 16777216 || wal.MaxTxBytes != 64<<20 {
		t.Fatal("limits differ from spec §2.1")
	}
	if wal.CommitHeaderSize != 12 || wal.OpSize(true, 3, 0) != 8 || wal.OpSize(false, 3, 4) != 16 {
		t.Fatal("OpSize differs from spec §2.1 (put = 9+k+v, delete = 5+k)")
	}
}

func TestEncodeCommitMatchesReference(t *testing.T) {
	c := sampleCommit()
	got := wal.EncodeCommit(c)
	want := refmodel.CommitPayload(c.TxID, toRef(c.Ops))
	if !bytes.Equal(got, want) {
		t.Fatal("EncodeCommit differs from the spec layout")
	}
	if len(got) != refmodel.CommitPayloadSize(toRef(c.Ops)) {
		t.Fatal("size formula mismatch")
	}
}

func TestDecodeCommitRoundTrip(t *testing.T) {
	c := sampleCommit()
	p := wal.EncodeCommit(c)
	d, err := wal.DecodeCommit(p)
	if err != nil {
		t.Fatal(err)
	}
	if d.TxID != c.TxID || len(d.Ops) != len(c.Ops) {
		t.Fatalf("got %+v", d)
	}
	for i := range c.Ops {
		if d.Ops[i].Delete != c.Ops[i].Delete || !bytes.Equal(d.Ops[i].Key, c.Ops[i].Key) || !bytes.Equal(d.Ops[i].Value, c.Ops[i].Value) {
			t.Fatalf("op %d differs", i)
		}
	}
	// Spec §4.2: decoded keys/values alias the payload.
	p[13+4] ^= 0xFF // first key byte
	if d.Ops[0].Key[0] != p[17] {
		t.Fatal("decoded key does not alias payload")
	}
}

func mutate(p []byte, f func([]byte) []byte) []byte { return f(append([]byte{}, p...)) }

func TestDecodeCommitRejects(t *testing.T) {
	good := refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("a"), Value: []byte("v")}, {Key: []byte("b"), Delete: true}})
	if _, err := wal.DecodeCommit(good); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty":        nil,
		"short header": good[:11],
		"zero ops":     refmodel.CommitPayload(1, nil),
		"bad opcode":   mutate(good, func(b []byte) []byte { b[12] = 3; return b }),
		"opcode zero":  mutate(good, func(b []byte) []byte { b[12] = 0; return b }),
		"trailing":     append(append([]byte{}, good...), 0),
		"truncated":    good[:len(good)-1],
		"count too big": mutate(good, func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[8:], 3)
			return b
		}),
		"count smaller (trailing ops)": mutate(good, func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[8:], 1)
			return b
		}),
		"zero key":       refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte{}, Value: []byte("v")}}),
		"key too large":  refmodel.CommitPayload(1, []refmodel.Op{{Key: make([]byte, 65537), Value: nil}}),
		"duplicate keys": refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("a")}, {Key: []byte("a"), Delete: true}}),
		"descending":     refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("b")}, {Key: []byte("a")}}),
		"keyLen past end": mutate(good, func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[13:], 0xFFFFFFF0)
			return b
		}),
		"valLen past end": mutate(good, func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[19:], 1000)
			return b
		}),
	}
	for name, p := range cases {
		if _, err := wal.DecodeCommit(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDecodeCommitLimitsInclusive(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 32 MiB")
	}
	maxKey := bytes.Repeat([]byte{'k'}, 65536)
	maxVal := make([]byte, 16<<20)
	p := refmodel.CommitPayload(1, []refmodel.Op{{Key: maxKey, Value: maxVal}})
	if _, err := wal.DecodeCommit(p); err != nil {
		t.Fatalf("max key/value rejected: %v", err)
	}
	p = refmodel.CommitPayload(1, []refmodel.Op{{Key: []byte("k"), Value: make([]byte, 16<<20+1)}})
	if _, err := wal.DecodeCommit(p); err == nil {
		t.Fatal("value of MaxValueSize+1 accepted")
	}
}

// allocBytes returns the bytes allocated by f. Other goroutines (GC, the fuzz
// worker) can only add noise, so the minimum over a few runs is used.
func allocBytes(f func()) uint64 {
	best := ^uint64(0)
	for i := 0; i < 3; i++ {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		f()
		runtime.ReadMemStats(&b)
		if d := b.TotalAlloc - a.TotalAlloc; d < best {
			best = d
		}
	}
	return best
}

func TestDecodeCommitHugeCountDoesNotAllocate(t *testing.T) {
	p := make([]byte, 40)
	binary.LittleEndian.PutUint32(p[8:], 0xFFFFFFFF)
	n := allocBytes(func() { wal.DecodeCommit(p) })
	if n > 16*uint64(len(p))+4096 {
		t.Fatalf("allocated %d bytes for a %d-byte payload", n, len(p))
	}
}

func FuzzDecodeCommit(f *testing.F) {
	f.Add(wal.EncodeCommit(sampleCommit()))
	f.Add(refmodel.CommitPayload(7, []refmodel.Op{{Key: []byte("x"), Delete: true}}))
	f.Add(make([]byte, 12))
	hc := make([]byte, 30)
	binary.LittleEndian.PutUint32(hc[8:], 0xFFFFFFFF)
	f.Add(hc)
	f.Fuzz(func(t *testing.T, p []byte) {
		var c wal.Commit
		var err error
		n := allocBytes(func() { c, err = wal.DecodeCommit(p) })
		if n > 16*uint64(len(p))+4096 {
			t.Fatalf("allocated %d bytes for %d input bytes", n, len(p))
		}
		rtx, rops, rerr := refmodel.ParseCommit(p)
		if (err == nil) != (rerr == nil) {
			t.Fatalf("engine err=%v reference err=%v", err, rerr)
		}
		if err != nil {
			return
		}
		if c.TxID != rtx || len(c.Ops) != len(rops) {
			t.Fatal("decoded content differs from reference")
		}
		if !bytes.Equal(wal.EncodeCommit(c), p) {
			t.Fatal("accepted payload is not canonical (re-encode differs)")
		}
	})
}
