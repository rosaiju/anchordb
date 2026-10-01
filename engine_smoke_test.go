package anchordb

// Smoke test owned by the main (implementation) agent. The independent test
// suite lives in the other *_test.go files.

import (
	"fmt"
	"testing"
)

func TestSmokeLifecycle(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{SegmentSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("k007")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k100"), nil); err != nil {
			return err
		}
		return tx.Delete([]byte("k000"))
	}); err != nil {
		t.Fatal(err)
	}
	st, _ := db.Stats()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st2, _ := db.Stats()
	if st2.Keys != 49 || st2.LastTxID != st.LastTxID || st2.Recovery.ReplayedTxs != 1 {
		t.Fatalf("after reopen: %+v (before %+v)", st2, st)
	}
	var keys []string
	db.Scan([]byte("k005"), []byte("k010"), func(k, v []byte) bool { keys = append(keys, string(k)); return true })
	if fmt.Sprint(keys) != "[k005 k006 k008 k009]" {
		t.Fatalf("scan = %v", keys)
	}
	if v, err := db.Get([]byte("k100")); err != nil || v == nil || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, err)
	}
}
