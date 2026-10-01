package record_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/rosaiju/anchordb/internal/record"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func TestConstantsMatchSpec(t *testing.T) {
	if record.HeaderSize != 16 || record.MaxPayload != 64<<20+1<<10 {
		t.Fatalf("HeaderSize=%d MaxPayload=%d", record.HeaderSize, record.MaxPayload)
	}
}

// The engine's encoder must be byte-identical to the spec-derived one.
func TestAppendMatchesReference(t *testing.T) {
	for _, p := range [][]byte{nil, {}, {0}, []byte("hello"), bytes.Repeat([]byte{0xAB}, 4097)} {
		for _, typ := range []byte{0, 1, 2, 3, 255} {
			got := record.Append(nil, typ, p)
			want := refmodel.Frame(typ, p)
			if !bytes.Equal(got, want) {
				t.Fatalf("typ %d len %d: encoding differs from spec", typ, len(p))
			}
			// Append must append, not overwrite.
			pre := []byte("prefix")
			got2 := record.Append(append([]byte{}, pre...), typ, p)
			if !bytes.Equal(got2, append(pre, want...)) {
				t.Fatal("Append does not preserve dst")
			}
		}
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	p := []byte("payload-bytes")
	buf := append(refmodel.Frame(7, p), []byte("trailing garbage")...)
	f, n, err := record.Decode(buf)
	if err != nil || n != 16+len(p) || f.Type != 7 || !bytes.Equal(f.Payload, p) {
		t.Fatalf("decode: %+v %d %v", f, n, err)
	}
	// Payload aliases buf (spec §4.1).
	buf[16] ^= 0xFF
	if f.Payload[0] != buf[16] {
		t.Fatal("payload does not alias the input buffer")
	}
}

func TestDecodeEmptyPayload(t *testing.T) {
	f, n, err := record.Decode(refmodel.Frame(1, nil))
	if err != nil || n != 16 || len(f.Payload) != 0 {
		t.Fatalf("%v %d %v", f, n, err)
	}
}

func TestDecodeEveryTruncationIsIncomplete(t *testing.T) {
	full := refmodel.Frame(1, []byte("0123456789abcdef0123"))
	for i := 0; i < len(full); i++ {
		if _, _, err := record.Decode(full[:i]); !errors.Is(err, record.ErrIncomplete) {
			t.Fatalf("prefix len %d: want ErrIncomplete, got %v", i, err)
		}
	}
}

// Every single-bit flip in a complete frame is detected, with the error kind
// the spec's check order implies.
func TestDecodeEveryBitFlip(t *testing.T) {
	full := refmodel.Frame(1, []byte("some payload of moderate size"))
	for i := 0; i < len(full); i++ {
		for bit := 0; bit < 8; bit++ {
			b := append([]byte{}, full...)
			b[i] ^= 1 << bit
			_, _, err := record.Decode(b)
			var want error
			switch {
			case i < 8 || i >= 13 && i < 16:
				want = record.ErrBadHeader
			default:
				want = record.ErrBadChecksum
			}
			if !errors.Is(err, want) {
				t.Fatalf("flip byte %d bit %d: want %v got %v", i, bit, want, err)
			}
		}
	}
}

func hdr(n uint32) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], n)
	binary.LittleEndian.PutUint32(b[4:8], refmodel.CRC(b[0:4]))
	return b
}

func TestDecodeCheckOrder(t *testing.T) {
	// TooLarge is reported before Incomplete.
	if _, _, err := record.Decode(hdr(record.MaxPayload + 1)); !errors.Is(err, record.ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	if _, _, err := record.Decode(hdr(0xFFFFFFFF)); !errors.Is(err, record.ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	// MaxPayload itself is allowed: short buffer → Incomplete.
	if _, _, err := record.Decode(hdr(record.MaxPayload)); !errors.Is(err, record.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	// Bad header beats too-large.
	b := hdr(record.MaxPayload + 1)
	b[14] = 1
	if _, _, err := record.Decode(b); !errors.Is(err, record.ErrBadHeader) {
		t.Fatalf("want ErrBadHeader, got %v", err)
	}
	// Incomplete payload beats checksum.
	b = hdr(10)
	if _, _, err := record.Decode(append(b, 1, 2, 3)); !errors.Is(err, record.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	// All-zero 16+ bytes: lenCRC of zero length is not zero → BadHeader.
	if _, _, err := record.Decode(make([]byte, 64)); !errors.Is(err, record.ErrBadHeader) {
		t.Fatalf("zeros: want ErrBadHeader, got %v", err)
	}
}

func TestDecodeDoesNotAllocate(t *testing.T) {
	buf := refmodel.Frame(1, bytes.Repeat([]byte{1}, 1000))
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, err := record.Decode(buf); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Decode allocated %v times", allocs)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(refmodel.Frame(1, []byte("x")))
	f.Add(refmodel.Frame(3, nil))
	f.Add(make([]byte, 20))
	f.Add(hdr(5))
	f.Add(hdr(record.MaxPayload + 1))
	f.Fuzz(func(t *testing.T, buf []byte) {
		fr, n, err := record.Decode(buf)
		rtyp, rp, rn, rerr := refmodel.DecodeFrame(buf)
		// Same classification as the independent decoder.
		pairs := []struct{ a, b error }{
			{record.ErrIncomplete, refmodel.ErrIncomplete},
			{record.ErrBadHeader, refmodel.ErrBadHeader},
			{record.ErrTooLarge, refmodel.ErrTooLarge},
			{record.ErrBadChecksum, refmodel.ErrBadChecksum},
		}
		if (err == nil) != (rerr == nil) {
			t.Fatalf("engine err %v, reference err %v", err, rerr)
		}
		for _, p := range pairs {
			if errors.Is(err, p.a) != errors.Is(rerr, p.b) {
				t.Fatalf("engine err %v, reference err %v", err, rerr)
			}
		}
		if err != nil {
			return
		}
		if n != rn || fr.Type != rtyp || !bytes.Equal(fr.Payload, rp) || n > len(buf) {
			t.Fatalf("mismatch: n=%d rn=%d", n, rn)
		}
		if !bytes.Equal(record.Append(nil, fr.Type, fr.Payload), buf[:n]) {
			t.Fatal("re-encoding a decoded frame does not reproduce it")
		}
	})
}
