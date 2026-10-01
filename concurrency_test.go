package anchordb_test

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func iters(full, short int) int {
	if testing.Short() {
		return short
	}
	return full
}

// Serializable read-modify-write: no lost updates.
func TestConcurrentCounter(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &anchordb.Options{SyncMode: anchordb.SyncNone})
	workers, per := 8, iters(100, 25)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				err := db.Update(func(tx *anchordb.Tx) error {
					n := 0
					v, err := tx.Get([]byte("ctr"))
					if err == nil {
						n, _ = strconv.Atoi(string(v))
					} else if !errors.Is(err, anchordb.ErrNotFound) {
						return err
					}
					return tx.Put([]byte("ctr"), []byte(strconv.Itoa(n+1)))
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	want := strconv.Itoa(workers * per)
	if v, _ := getT(t, db, "ctr"); v != want {
		t.Fatalf("counter=%s want %s (lost updates)", v, want)
	}
	if s := statsT(t, db); s.LastTxID != uint64(workers*per) {
		t.Fatalf("LastTxID=%d", s.LastTxID)
	}
	db = reopenT(t, db, dir, nil)
	if v, _ := getT(t, db, "ctr"); v != want {
		t.Fatalf("after reopen counter=%s", v)
	}
	closeT(t, db)
}

// Every writer sets all keys to the same generation in one tx; readers must
// never see a mix (atomic visibility) and must see the same values twice
// within one read tx (repeatable read).
func TestReadersNeverSeePartialTx(t *testing.T) {
	db := openT(t, t.TempDir(), &anchordb.Options{SyncMode: anchordb.SyncNone})
	defer db.Close()
	const nkeys = 10
	write := func(gen int) error {
		return db.Update(func(tx *anchordb.Tx) error {
			for i := 0; i < nkeys; i++ {
				if err := tx.Put(k(i), []byte(strconv.Itoa(gen))); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var reads atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				err := db.View(func(tx *anchordb.Tx) error {
					first := ""
					for i := 0; i < nkeys; i++ {
						v, err := tx.Get(k(i))
						if err != nil {
							return err
						}
						if i == 0 {
							first = string(v)
						} else if string(v) != first {
							return fmt.Errorf("partial tx visible: key0=%s key%d=%s", first, i, v)
						}
					}
					var seen []string
					tx.Scan(nil, nil, func(_, v []byte) bool { seen = append(seen, string(v)); return true })
					if len(seen) != nkeys {
						return fmt.Errorf("scan saw %d keys", len(seen))
					}
					for _, s := range seen {
						if s != first {
							return fmt.Errorf("non-repeatable read: Get saw %s, Scan saw %s", first, s)
						}
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				// Convenience reads are their own transactions.
				reads.Add(1)
			}
		}()
	}
	for gen := 1; gen <= iters(300, 60); gen++ {
		if err := write(gen); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	if reads.Load() == 0 {
		t.Fatal("readers never ran")
	}
}

// Checkpoints, Stats and readers run concurrently with writers; the final
// state (deterministic because writers touch disjoint keys) must be exact,
// before and after reopen.
func TestCheckpointConcurrentWithWriters(t *testing.T) {
	dir := t.TempDir()
	opts := &anchordb.Options{SyncMode: anchordb.SyncNone, SegmentSize: 2048}
	db := openT(t, dir, opts)
	workers, per := 4, iters(150, 40)
	md := refmodel.New()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		for i := 0; i < per; i++ {
			key := []byte(fmt.Sprintf("w%d-%04d", w, i%37))
			md.Put(key, []byte(fmt.Sprintf("%d", i)))
		}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				key := []byte(fmt.Sprintf("w%d-%04d", w, i%37))
				if err := db.Put(key, []byte(fmt.Sprintf("%d", i))); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	var stop atomic.Bool
	var aux sync.WaitGroup
	aux.Add(2)
	go func() {
		defer aux.Done()
		for !stop.Load() {
			if err := db.Checkpoint(); err != nil {
				t.Error("Checkpoint:", err)
				return
			}
		}
	}()
	go func() {
		defer aux.Done()
		for !stop.Load() {
			s, err := db.Stats()
			if err != nil {
				t.Error(err)
				return
			}
			if s.WALStartSeq > s.ActiveSeq || s.CheckpointTxID > s.LastTxID {
				t.Errorf("inconsistent stats %+v", s)
				return
			}
		}
	}()
	wg.Wait()
	stop.Store(true)
	aux.Wait()
	assertModel(t, db, md)
	db = reopenT(t, db, dir, opts)
	assertModel(t, db, md)
	if s := statsT(t, db); s.LastTxID != uint64(workers*per) {
		t.Fatalf("LastTxID=%d", s.LastTxID)
	}
	closeT(t, db)
}

func TestCloseWaitsForOpenTx(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	ro := beginT(t, db, false)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while a transaction was open", err)
	default:
	}
	// A Begin that blocks while Close waits must return ErrClosed afterwards.
	began := make(chan error, 1)
	go func() {
		tx, err := db.Begin(false)
		if err == nil {
			tx.Rollback()
			began <- errors.New("Begin succeeded while Close was waiting")
			return
		}
		began <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if v, err := ro.Get([]byte("x")); !errors.Is(err, anchordb.ErrNotFound) {
		t.Fatalf("open tx stopped working while Close waits: %q %v", v, err)
	}
	ro.Rollback()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after tx ended")
	}
	select {
	case err := <-began:
		if !errors.Is(err, anchordb.ErrClosed) {
			t.Fatalf("blocked Begin: got %v, want ErrClosed (§2.2 rule 11)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Begin never returned after Close")
	}
}

// Writers exclude readers and each other: while a write tx is open, Begin
// blocks for everyone.
func TestWriterExcludesOthers(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	w := beginT(t, db, true)
	w.Put([]byte("a"), []byte("1"))
	got := make(chan string, 1)
	go func() {
		v, err := db.Get([]byte("a"))
		if err != nil {
			got <- "err:" + err.Error()
			return
		}
		got <- string(v)
	}()
	select {
	case v := <-got:
		t.Fatalf("reader ran while writer open (saw %q)", v)
	case <-time.After(100 * time.Millisecond):
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if v := <-got; v != "1" {
		t.Fatalf("reader saw %q after commit", v)
	}
}
