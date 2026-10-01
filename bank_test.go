package anchordb_test

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

const (
	bankAccounts = 20
	bankInitial  = 1000
)

func acct(i int) []byte { return []byte(fmt.Sprintf("acct-%02d", i)) }

func readBal(tx *anchordb.Tx, i int) (int, error) {
	v, err := tx.Get(acct(i))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(v))
}

// transfer moves amt from a to b. If conditional, it refuses (returns
// errInsufficient, rolling back) when a would go negative.
var errInsufficient = errors.New("insufficient funds")

func transfer(db *anchordb.DB, a, b, amt int, conditional bool) error {
	return db.Update(func(tx *anchordb.Tx) error {
		ba, err := readBal(tx, a)
		if err != nil {
			return err
		}
		bb, err := readBal(tx, b)
		if err != nil {
			return err
		}
		if conditional && ba < amt {
			return errInsufficient
		}
		if err := tx.Put(acct(a), []byte(strconv.Itoa(ba-amt))); err != nil {
			return err
		}
		return tx.Put(acct(b), []byte(strconv.Itoa(bb+amt)))
	})
}

func setupBank(t *testing.T, db *anchordb.DB, md *refmodel.Model) {
	err := db.Update(func(tx *anchordb.Tx) error {
		for i := 0; i < bankAccounts; i++ {
			if err := tx.Put(acct(i), []byte(strconv.Itoa(bankInitial))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < bankAccounts; i++ {
		md.Put(acct(i), []byte(strconv.Itoa(bankInitial)))
	}
}

func bankTotal(tx *anchordb.Tx) (int, error) {
	sum := 0
	n := 0
	err := tx.Scan([]byte("acct-"), []byte("acct."), func(_, v []byte) bool {
		x, _ := strconv.Atoi(string(v))
		sum += x
		n++
		return true
	})
	if err == nil && n != bankAccounts {
		err = fmt.Errorf("scan saw %d accounts", n)
	}
	return sum, err
}

// Concurrent unconditional transfers commute, so the exact final balance of
// every account is known regardless of the serial order the engine picks.
// A no-op or lossy engine fails the per-account comparison; readers check
// the total in every snapshot.
func TestBankConcurrentExactBalances(t *testing.T) {
	dir := t.TempDir()
	opts := &anchordb.Options{SyncMode: anchordb.SyncNone, SegmentSize: 4096}
	db := openT(t, dir, opts)
	md := refmodel.New()
	setupBank(t, db, md)
	expected := make([]int, bankAccounts)
	for i := range expected {
		expected[i] = bankInitial
	}
	workers, per := 6, iters(120, 30)
	type xfer struct{ a, b, amt int }
	plans := make([][]xfer, workers)
	for w := range plans {
		r := rand.New(rand.NewSource(int64(w) + 100))
		for i := 0; i < per; i++ {
			a := r.Intn(bankAccounts)
			b := (a + 1 + r.Intn(bankAccounts-1)) % bankAccounts
			x := xfer{a, b, 1 + r.Intn(50)}
			plans[w] = append(plans[w], x)
			expected[a] -= x.amt
			expected[b] += x.amt
		}
	}
	var stop atomic.Bool
	var rw sync.WaitGroup
	for r := 0; r < 3; r++ {
		rw.Add(1)
		go func() {
			defer rw.Done()
			for !stop.Load() {
				err := db.View(func(tx *anchordb.Tx) error {
					s, err := bankTotal(tx)
					if err == nil && s != bankAccounts*bankInitial {
						err = fmt.Errorf("total %d in a snapshot", s)
					}
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < workers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for i, x := range plans[w] {
				if err := transfer(db, x.a, x.b, x.amt, false); err != nil {
					t.Error(err)
					return
				}
				if w == 0 && i == per/2 {
					if err := db.Checkpoint(); err != nil {
						t.Error(err)
					}
				}
			}
		}(w)
	}
	ww.Wait()
	stop.Store(true)
	rw.Wait()
	check := func(db *anchordb.DB) {
		t.Helper()
		for i := 0; i < bankAccounts; i++ {
			v, _ := getT(t, db, string(acct(i)))
			if v != strconv.Itoa(expected[i]) {
				t.Fatalf("account %d = %s, want %d", i, v, expected[i])
			}
		}
		if s := statsT(t, db); s.LastTxID != uint64(1+workers*per) {
			t.Fatalf("LastTxID=%d want %d", s.LastTxID, 1+workers*per)
		}
	}
	check(db)
	db = reopenT(t, db, dir, opts)
	check(db)
	closeT(t, db)
}

// Sequential conditional transfers: the reference model replays the same
// decisions and must agree exactly, including which transfers were refused.
func TestBankSequentialMatchesModel(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &anchordb.Options{SyncMode: anchordb.SyncNone})
	md := refmodel.New()
	setupBank(t, db, md)
	md.LastTxID = 1
	r := rand.New(rand.NewSource(7))
	refused := 0
	for i := 0; i < iters(600, 150); i++ {
		a := r.Intn(bankAccounts)
		b := (a + 1 + r.Intn(bankAccounts-1)) % bankAccounts
		amt := 1 + r.Intn(700)
		err := transfer(db, a, b, amt, true)
		ba, _ := md.Get(acct(a))
		bb, _ := md.Get(acct(b))
		na, _ := strconv.Atoi(string(ba))
		nb, _ := strconv.Atoi(string(bb))
		if na < amt {
			if !errors.Is(err, errInsufficient) {
				t.Fatalf("transfer %d: engine allowed overdraft (model balance %d < %d), err=%v", i, na, amt, err)
			}
			refused++
			continue
		}
		if err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
		mtx := md.Begin()
		mtx.Put(acct(a), []byte(strconv.Itoa(na-amt)))
		mtx.Put(acct(b), []byte(strconv.Itoa(nb+amt)))
		mtx.Commit()
		if i%97 == 0 {
			db = reopenT(t, db, dir, nil)
		}
	}
	if refused == 0 {
		t.Fatal("test never exercised the refusal path")
	}
	assertModel(t, db, md)
	if statsT(t, db).LastTxID != md.LastTxID {
		t.Fatalf("LastTxID %d, model %d", statsT(t, db).LastTxID, md.LastTxID)
	}
	closeT(t, db)
}
