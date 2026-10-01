package checkpoint_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rosaiju/anchordb/internal/checkpoint"
	"github.com/rosaiju/anchordb/internal/refmodel"
	"github.com/rosaiju/anchordb/internal/vfs"
)

func kvs(n int) []refmodel.KV {
	out := make([]refmodel.KV, n)
	for i := range out {
		out[i] = refmodel.KV{Key: []byte(fmt.Sprintf("key-%05d", i)), Value: []byte(fmt.Sprintf("val-%d", i*i))}
	}
	return out
}

func toEntries(in []refmodel.KV) []checkpoint.Entry {
	out := make([]checkpoint.Entry, len(in))
	for i, kv := range in {
		out[i] = checkpoint.Entry{Key: kv.Key, Value: kv.Value}
	}
	return out
}

func TestNames(t *testing.T) {
	if checkpoint.Name(42) != "checkpoint-00000000000000000042.ckpt" {
		t.Fatal(checkpoint.Name(42))
	}
	if v, ok := checkpoint.ParseName("checkpoint-00000000000000000042.ckpt"); !ok || v != 42 {
		t.Fatal("parse")
	}
	for _, n := range []string{"checkpoint-42.ckpt", "checkpoint-00000000000000000042.ckpt.tmp",
		"checkpoint-0000000000000000004x.ckpt", "checkpoint-+0000000000000000042.ckpt", "Checkpoint-00000000000000000042.ckpt"} {
		if _, ok := checkpoint.ParseName(n); ok {
			t.Errorf("%s accepted", n)
		}
	}
}

func TestEncodeEntriesMatchesReference(t *testing.T) {
	in := append(kvs(5), refmodel.KV{Key: []byte("zz"), Value: []byte{}})
	if !bytes.Equal(checkpoint.EncodeEntries(toEntries(in)), refmodel.EntriesPayload(in)) {
		t.Fatal("EncodeEntries differs from spec §4.3")
	}
	got, err := checkpoint.DecodeEntries(refmodel.EntriesPayload(in))
	if err != nil || len(got) != len(in) {
		t.Fatal(err)
	}
	for i := range in {
		if !bytes.Equal(got[i].Key, in[i].Key) || !bytes.Equal(got[i].Value, in[i].Value) {
			t.Fatalf("entry %d", i)
		}
	}
}

func TestDecodeEntriesAliases(t *testing.T) {
	p := refmodel.EntriesPayload(kvs(1))
	es, err := checkpoint.DecodeEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	p[8] ^= 0xFF
	if es[0].Key[0] != p[8] {
		t.Fatal("DecodeEntries does not alias input (spec §9.1)")
	}
}

func TestDecodeEntriesRejects(t *testing.T) {
	good := refmodel.EntriesPayload(kvs(2))
	m := func(f func([]byte) []byte) []byte { return f(append([]byte{}, good...)) }
	cases := map[string][]byte{
		"empty":      nil,
		"count zero": refmodel.EntriesPayload(nil),
		"trailing":   append(append([]byte{}, good...), 0),
		"truncated":  good[:len(good)-1],
		"zero key":   refmodel.EntriesPayload([]refmodel.KV{{Key: []byte{}, Value: []byte("v")}}),
		"big key":    refmodel.EntriesPayload([]refmodel.KV{{Key: make([]byte, 65537)}}),
		"dup":        refmodel.EntriesPayload([]refmodel.KV{{Key: []byte("a")}, {Key: []byte("a")}}),
		"desc":       refmodel.EntriesPayload([]refmodel.KV{{Key: []byte("b")}, {Key: []byte("a")}}),
		"count big":  m(func(b []byte) []byte { binary.LittleEndian.PutUint32(b, 3); return b }),
		"huge count": m(func(b []byte) []byte { binary.LittleEndian.PutUint32(b, 0xFFFFFFFF); return b }),
		"huge klen":  m(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[4:], 0xFFFFFFFF); return b }),
	}
	for name, p := range cases {
		if _, err := checkpoint.DecodeEntries(p); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if testing.Short() {
		return
	}
	big := refmodel.EntriesPayload([]refmodel.KV{{Key: []byte("k"), Value: make([]byte, 16<<20+1)}})
	if _, err := checkpoint.DecodeEntries(big); err == nil {
		t.Error("value > MaxValueSize accepted")
	}
	okMax := refmodel.EntriesPayload([]refmodel.KV{{Key: make([]byte, 65536), Value: make([]byte, 16<<20)}})
	if _, err := checkpoint.DecodeEntries(okMax); err != nil {
		t.Errorf("max key/value rejected: %v", err)
	}
}

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

func FuzzDecodeEntries(f *testing.F) {
	f.Add(refmodel.EntriesPayload(kvs(3)))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 1, 0, 0, 0, 'a'})
	f.Fuzz(func(t *testing.T, p []byte) {
		var es []checkpoint.Entry
		var err error
		if n := allocBytes(func() { es, err = checkpoint.DecodeEntries(p) }); n > 16*uint64(len(p))+4096 {
			t.Fatalf("allocated %d bytes for %d input bytes", n, len(p))
		}
		ref, rerr := refmodel.ParseEntries(p)
		if (err == nil) != (rerr == nil) {
			t.Fatalf("engine %v reference %v", err, rerr)
		}
		if err != nil {
			return
		}
		if len(es) != len(ref) || !bytes.Equal(checkpoint.EncodeEntries(es), p) {
			t.Fatal("round trip failed")
		}
	})
}

// --- whole-file validation (spec §4.3) ---

func writeFile(t *testing.T, dir string, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func readAll(path string, want uint64) ([]refmodel.KV, error) {
	var out []refmodel.KV
	err := checkpoint.Read(vfs.OS, path, want, func(k, v []byte) {
		out = append(out, refmodel.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
	})
	return out, err
}

func TestReadValidFiles(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		n, per int
	}{{0, 0}, {1, 0}, {10, 3}, {10, 1}, {100, 0}} {
		in := kvs(tc.n)
		p := writeFile(t, dir, checkpoint.Name(9), refmodel.CheckpointFile(9, in, tc.per))
		got, err := readAll(p, 9)
		if err != nil {
			t.Fatalf("n=%d per=%d: %v", tc.n, tc.per, err)
		}
		md := refmodel.New()
		for _, kv := range in {
			md.Put(kv.Key, kv.Value)
		}
		if !md.Equal(got) {
			t.Fatalf("n=%d: content mismatch", tc.n)
		}
	}
}

func TestReadRejects(t *testing.T) {
	dir := t.TempDir()
	in := kvs(6)
	good := refmodel.CheckpointFile(9, in, 2)
	hdr := refmodel.CheckpointHeader(9)
	ent := func(k ...refmodel.KV) []byte { return refmodel.Frame(2, refmodel.EntriesPayload(k)) }
	tr := func(txid, n uint64) []byte { return refmodel.Frame(3, refmodel.TrailerPayload(txid, n)) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	a, b := refmodel.KV{Key: []byte("a"), Value: []byte("1")}, refmodel.KV{Key: []byte("b"), Value: []byte("2")}
	cases := map[string][]byte{
		"truncated header":      good[:20],
		"bad magic":             append([]byte("XNCHCKP1"), good[8:]...),
		"header txid differs":   cat(refmodel.CheckpointHeader(8), good[32:]),
		"no trailer":            cat(hdr, ent(a, b)),
		"trailer not last":      cat(hdr, tr(9, 1), ent(a)),
		"two trailers":          cat(hdr, ent(a), tr(9, 1), tr(9, 1)),
		"bytes after trailer":   append(append([]byte{}, good...), 0),
		"zeros after trailer":   append(append([]byte{}, good...), make([]byte, 32)...),
		"count mismatch":        cat(hdr, ent(a, b), tr(9, 3)),
		"trailer txid mismatch": cat(hdr, ent(a, b), tr(10, 2)),
		"cross-frame order":     cat(hdr, ent(b), ent(a), tr(9, 2)),
		"cross-frame duplicate": cat(hdr, ent(a), ent(a), tr(9, 2)),
		"wrong frame type":      cat(hdr, refmodel.Frame(1, refmodel.EntriesPayload([]refmodel.KV{a})), tr(9, 1)),
		"empty entries frame":   cat(hdr, refmodel.Frame(2, refmodel.EntriesPayload(nil)), tr(9, 0)),
		"truncated last frame":  good[:len(good)-1],
		"truncated mid":         good[:len(good)-30],
		"header only":           hdr,
		"empty file":            nil,
		"trailer payload short": cat(hdr, refmodel.Frame(3, make([]byte, 8))),
		"trailer payload long":  cat(hdr, refmodel.Frame(3, make([]byte, 17))),
	}
	for name, img := range cases {
		p := writeFile(t, dir, checkpoint.Name(9), img)
		if _, err := readAll(p, 9); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// wantTxID / file name disagreement.
	p := writeFile(t, dir, checkpoint.Name(9), good)
	if _, err := readAll(p, 10); err == nil {
		t.Error("wantTxID mismatch accepted")
	}
	// Every bit flip anywhere in a valid file is rejected.
	for i := 0; i < len(good); i++ {
		if i >= 28 && i < 32 { // header reserved bytes are not covered by headerCRC
			continue
		}
		img := append([]byte{}, good...)
		img[i] ^= 0x04
		p := writeFile(t, dir, checkpoint.Name(9), img)
		if _, err := readAll(p, 9); err == nil {
			t.Fatalf("bit flip at %d accepted", i)
		}
	}
}

func TestWriteThenRead(t *testing.T) {
	dir := t.TempDir()
	in := kvs(3000)
	in = append(in, refmodel.KV{Key: []byte("zzz-big"), Value: bytes.Repeat([]byte{7}, 1<<20+5)})
	each := func(yield func(k, v []byte) bool) {
		for _, kv := range in {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
	var stages []checkpoint.Stage
	if err := checkpoint.Write(vfs.OS, dir, 77, each, func(s checkpoint.Stage) { stages = append(stages, s) }); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 || ents[0].Name() != checkpoint.Name(77) {
		t.Fatalf("directory after Write: %v", ents)
	}
	got, err := readAll(filepath.Join(dir, checkpoint.Name(77)), 77)
	if err != nil || len(got) != len(in) {
		t.Fatalf("%v %d", err, len(got))
	}
	// Independent parse of the file: frames decode, payloads ≤ ~1 MiB unless single entry.
	img, _ := os.ReadFile(filepath.Join(dir, checkpoint.Name(77)))
	if !bytes.Equal(img[:32], refmodel.CheckpointHeader(77)) {
		t.Fatal("header differs from spec")
	}
	off, total := 32, 0
	for off < len(img) {
		typ, p, n, err := refmodel.DecodeFrame(img[off:])
		if err != nil {
			t.Fatalf("frame at %d: %v", off, err)
		}
		off += n
		if typ == 3 {
			if off != len(img) || !bytes.Equal(p, refmodel.TrailerPayload(77, uint64(len(in)))) {
				t.Fatal("bad trailer")
			}
			break
		}
		es, err := refmodel.ParseEntries(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(es) > 1 && len(p) > 2<<20 {
			t.Fatalf("multi-entry frame of %d bytes (spec: batched up to ~1 MiB)", len(p))
		}
		total += len(es)
	}
	if total != len(in) {
		t.Fatalf("entries %d", total)
	}
	// Empty checkpoint.
	if err := checkpoint.Write(vfs.OS, dir, 78, func(func(k, v []byte) bool) {}, nil); err != nil {
		t.Fatal(err)
	}
	img, _ = os.ReadFile(filepath.Join(dir, checkpoint.Name(78)))
	if !bytes.Equal(img, refmodel.CheckpointFile(78, nil, 0)) {
		t.Fatal("empty checkpoint differs from spec layout")
	}
}
