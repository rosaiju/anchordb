package anchordb_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

// TestRandomOpsAgainstModel runs seeded random operation sequences — single
// ops, multi-op transactions, rollbacks, scans, reopens and checkpoints —
// and compares every observation with the reference model.
// Reproduce a failure with ANCHORDB_SEED=<seed> go test -run TestRandomOps.
func TestRandomOpsAgainstModel(t *testing.T) {
	seeds := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	if testing.Short() {
		seeds = seeds[:3]
	}
	if s := os.Getenv("ANCHORDB_SEED"); s != "" {
		v, _ := strconv.ParseInt(s, 10, 64)
		seeds = []int64{v}
	}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runRandom(t, seed, iters(1500, 400))
		})
	}
}

func randKey(r *rand.Rand) []byte {
	switch r.Intn(10) {
	case 0:
		return []byte{byte(r.Intn(3))} // short binary keys incl. 0x00
	case 1:
		return bytes.Repeat([]byte{'z'}, 1+r.Intn(300))
	default:
		return []byte(fmt.Sprintf("k%02d", r.Intn(40)))
	}
}

func randVal(r *rand.Rand) []byte {
	switch r.Intn(8) {
	case 0:
		return []byte{}
	case 1:
		return bytes.Repeat([]byte{byte(r.Intn(256))}, r.Intn(3000))
	default:
		return []byte(strconv.Itoa(r.Int()))
	}
}

func randBound(r *rand.Rand) []byte {
	if r.Intn(4) == 0 {
		return nil
	}
	return randKey(r)
}

func runRandom(t *testing.T, seed int64, steps int) {
	r := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	opts := &anchordb.Options{SyncMode: anchordb.SyncNone, SegmentSize: int64(64 + r.Intn(4096))}
	db := openT(t, dir, opts)
	md := refmodel.New()
	fail := func(step int, format string, a ...any) {
		t.Helper()
		t.Fatalf("seed %d step %d: %s", seed, step, fmt.Sprintf(format, a...))
	}
	for step := 0; step < steps; step++ {
		switch op := r.Intn(100); {
		case op < 30:
			key, val := randKey(r), randVal(r)
			if err := db.Put(key, val); err != nil {
				fail(step, "Put: %v", err)
			}
			md.Apply([]refmodel.Op{{Key: key, Value: val}})
		case op < 40:
			key := randKey(r)
			if err := db.Delete(key); err != nil {
				fail(step, "Delete: %v", err)
			}
			md.Apply([]refmodel.Op{{Key: key, Delete: true}})
		case op < 55:
			// Multi-op transaction with interleaved reads; maybe rolled back.
			tx, err := db.Begin(true)
			if err != nil {
				fail(step, "Begin: %v", err)
			}
			mtx := md.Begin()
			for j := 0; j < 1+r.Intn(8); j++ {
				key := randKey(r)
				switch r.Intn(4) {
				case 0, 1:
					v := randVal(r)
					tx.Put(key, v)
					mtx.Put(key, v)
				case 2:
					tx.Delete(key)
					mtx.Delete(key)
				case 3:
					v, err := tx.Get(key)
					mv, ok := mtx.Get(key)
					if ok != (err == nil) || !bytes.Equal(v, mv) {
						fail(step, "tx.Get(%q)=%q,%v model %q,%v", key, trunc(v), err, trunc(mv), ok)
					}
				}
			}
			if r.Intn(3) == 0 {
				lo, hi := randBound(r), randBound(r)
				var got []refmodel.KV
				tx.Scan(lo, hi, func(k, v []byte) bool {
					got = append(got, refmodel.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
					return true
				})
				if kvString(got) != kvString(mtx.Scan(lo, hi)) {
					fail(step, "tx.Scan(%q,%q) mismatch", lo, hi)
				}
			}
			if r.Intn(5) == 0 {
				tx.Rollback()
			} else {
				if err := tx.Commit(); err != nil {
					fail(step, "Commit: %v", err)
				}
				mtx.Commit()
			}
		case op < 75:
			key := randKey(r)
			v, err := db.Get(key)
			mv, ok := md.Get(key)
			if ok != (err == nil) || !bytes.Equal(v, mv) || (!ok && !errors.Is(err, anchordb.ErrNotFound)) {
				fail(step, "Get(%q)=%q,%v model %q,%v", key, trunc(v), err, trunc(mv), ok)
			}
		case op < 85:
			lo, hi := randBound(r), randBound(r)
			limit := r.Intn(10)
			var got []refmodel.KV
			db.Scan(lo, hi, func(k, v []byte) bool {
				got = append(got, refmodel.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
				return limit == 0 || len(got) < limit
			})
			want := md.Scan(lo, hi)
			if limit > 0 && len(want) > limit {
				want = want[:limit]
			}
			if kvString(got) != kvString(want) || len(got) != len(want) {
				fail(step, "Scan(%q,%q,limit %d):\n got %s\nwant %s", lo, hi, limit, kvString(got), kvString(want))
			}
		case op < 92:
			db = reopenT(t, db, dir, opts)
			assertModel(t, db, md)
		default:
			if err := db.Checkpoint(); err != nil {
				fail(step, "Checkpoint: %v", err)
			}
		}
		if s := statsT(t, db); s.LastTxID != md.LastTxID || s.Keys != md.Len() {
			fail(step, "stats LastTxID=%d Keys=%d, model %d/%d", s.LastTxID, s.Keys, md.LastTxID, md.Len())
		}
	}
	db = reopenT(t, db, dir, opts)
	assertModel(t, db, md)
	closeT(t, db)
}
