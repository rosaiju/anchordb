package manifest_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rosaiju/anchordb/internal/manifest"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func TestEncodeMatchesSpec(t *testing.T) {
	cases := []manifest.Current{
		{Checkpoint: "", CheckpointTxID: 0, WALStart: 1},
		{Checkpoint: refmodel.CheckpointName(42), CheckpointTxID: 42, WALStart: 7},
	}
	for _, c := range cases {
		got := manifest.Encode(c)
		want := refmodel.CurrentFile(c.Checkpoint, c.CheckpointTxID, c.WALStart)
		if !bytes.Equal(got, want) {
			t.Fatalf("Encode(%+v)=\n%s\nwant\n%s", c, got, want)
		}
		d, err := manifest.Decode(got)
		if err != nil || d != c {
			t.Fatalf("Decode: %+v %v", d, err)
		}
	}
	if !strings.HasPrefix(string(manifest.Encode(cases[0])), "ANCHORDB-CURRENT 1\ncheckpoint none\ncheckpoint_txid 0\nwal_start 1\ncrc32c ") {
		t.Fatal("layout differs from §4.4")
	}
}

// reCRC rebuilds a CURRENT image with a correct CRC line for the given body.
func reCRC(body string) []byte {
	return []byte(body + "crc32c " + hex8(refmodel.CRC([]byte(body))) + "\n")
}

func hex8(v uint32) string {
	const d = "0123456789abcdef"
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = d[v&15]
		v >>= 4
	}
	return string(b)
}

func TestDecodeRejects(t *testing.T) {
	good := string(refmodel.CurrentFile(refmodel.CheckpointName(42), 42, 7))
	body := "ANCHORDB-CURRENT 1\ncheckpoint %s\ncheckpoint_txid %s\nwal_start %s\n"
	f := func(ck, tx, ws string) []byte {
		return reCRC(sprintf(body, ck, tx, ws))
	}
	upper := strings.Replace(good, good[len(good)-9:len(good)-1], strings.ToUpper(good[len(good)-9:len(good)-1]), 1)
	cases := map[string][]byte{
		"empty":                 nil,
		"bad crc":               []byte(strings.Replace(good, "wal_start 7", "wal_start 8", 1)),
		"trailing bytes":        []byte(good + "\n"),
		"missing final newline": []byte(good[:len(good)-1]),
		"crlf":                  []byte(strings.ReplaceAll(good, "\n", "\r\n")),
		"none with txid":        f("none", "5", "1"),
		"name/txid mismatch":    f(refmodel.CheckpointName(41), "42", "7"),
		"short name":            f("checkpoint-42.ckpt", "42", "7"),
		"wal_start zero":        f("none", "0", "0"),
		"leading zero txid":     f(refmodel.CheckpointName(42), "042", "7"),
		"leading zero walstart": f("none", "0", "01"),
		"plus sign":             f("none", "0", "+1"),
		"negative":              f("none", "0", "-1"),
		"bad version":           reCRC("ANCHORDB-CURRENT 2\ncheckpoint none\ncheckpoint_txid 0\nwal_start 1\n"),
		"extra space":           reCRC("ANCHORDB-CURRENT 1\ncheckpoint  none\ncheckpoint_txid 0\nwal_start 1\n"),
		"reordered":             reCRC("ANCHORDB-CURRENT 1\ncheckpoint_txid 0\ncheckpoint none\nwal_start 1\n"),
		"overflow":              f("none", "0", "18446744073709551616"),
		"empty checkpoint name": f("", "0", "1"),
	}
	if upper != good {
		cases["uppercase hex"] = []byte(upper)
	}
	for name, b := range cases {
		if c, err := manifest.Decode(b); err == nil {
			t.Errorf("%s: accepted as %+v", name, c)
		}
	}
	// Sanity: helper builds an accepted image for valid fields.
	if _, err := manifest.Decode(f("none", "0", "1")); err != nil {
		t.Fatalf("helper broken: %v", err)
	}
}

func sprintf(format string, a ...string) string {
	for _, s := range a {
		format = strings.Replace(format, "%s", s, 1)
	}
	return format
}

func FuzzDecode(f *testing.F) {
	f.Add(refmodel.CurrentFile("", 0, 1))
	f.Add(refmodel.CurrentFile(refmodel.CheckpointName(42), 42, 7))
	f.Add([]byte("ANCHORDB-CURRENT 1\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := manifest.Decode(b)
		if err != nil {
			return
		}
		// Canonical decoding (§4.4).
		if !bytes.Equal(manifest.Encode(c), b) {
			t.Fatalf("accepted non-canonical input %q", b)
		}
		if c.WALStart < 1 || (c.Checkpoint == "" && c.CheckpointTxID != 0) {
			t.Fatalf("accepted invalid fields %+v", c)
		}
		if c.Checkpoint != "" && c.Checkpoint != refmodel.CheckpointName(c.CheckpointTxID) {
			t.Fatalf("name/txid mismatch accepted %+v", c)
		}
	})
}
