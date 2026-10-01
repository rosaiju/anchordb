package anchordb_test

// Benchmarks owned by the main agent. Results and the exact commands used are
// recorded in BENCHMARKS.md. Durable (SyncAlways) and non-durable (SyncNone)
// results are reported under separate names and must not be compared as if
// they offered the same guarantee.

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/rosaiju/anchordb"
)

const (
	benchKeys      = 100_000
	benchValueSize = 100
)

func benchKey(i int) []byte { return []byte(fmt.Sprintf("key:%010d", i)) }

func benchValue(r *rand.Rand) []byte {
	v := make([]byte, benchValueSize)
	for i := range v {
		v[i] = byte('a' + r.IntN(26))
	}
	return v
}

// populate loads n keys in transactions of 1000 keys.
func populate(b *testing.B, db *anchordb.DB, n int) {
	b.Helper()
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < n; i += 1000 {
		err := db.Update(func(tx *anchordb.Tx) error {
			for j := i; j < i+1000 && j < n; j++ {
				if err := tx.Put(benchKey(j), benchValue(r)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func openBench(b *testing.B, dir string, mode anchordb.SyncMode) *anchordb.DB {
	b.Helper()
	db, err := anchordb.Open(dir, &anchordb.Options{SyncMode: mode})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

// BenchmarkGet: random point reads from a 100k-key database (in memory).
func BenchmarkGet(b *testing.B) {
	db := openBench(b, b.TempDir(), anchordb.SyncNone)
	defer db.Close()
	populate(b, db, benchKeys)
	r := rand.New(rand.NewPCG(3, 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(benchKey(r.IntN(benchKeys))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetParallel: concurrent readers share the RWMutex.
func BenchmarkGetParallel(b *testing.B) {
	db := openBench(b, b.TempDir(), anchordb.SyncNone)
	defer db.Close()
	populate(b, db, benchKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(rand.Uint64(), 5))
		for pb.Next() {
			if _, err := db.Get(benchKey(r.IntN(benchKeys))); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkPut: one key per transaction. Durable = one fsync per op.
func BenchmarkPut(b *testing.B) {
	for _, m := range []struct {
		name string
		mode anchordb.SyncMode
	}{{"durable", anchordb.SyncAlways}, {"nondurable", anchordb.SyncNone}} {
		b.Run(m.name, func(b *testing.B) {
			db := openBench(b, b.TempDir(), m.mode)
			defer db.Close()
			r := rand.New(rand.NewPCG(6, 7))
			v := benchValue(r)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Put(benchKey(r.IntN(benchKeys)), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTxBatch: N keys per durable transaction. Reports keys/s; one
// b.N iteration is one transaction.
func BenchmarkTxBatch(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("durable/keys=%d", n), func(b *testing.B) {
			db := openBench(b, b.TempDir(), anchordb.SyncAlways)
			defer db.Close()
			r := rand.New(rand.NewPCG(8, 9))
			v := benchValue(r)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Update(func(tx *anchordb.Tx) error {
					for j := 0; j < n; j++ {
						if err := tx.Put(benchKey(r.IntN(benchKeys)), v); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "keys/s")
		})
	}
}

// BenchmarkScan: range scans over a 100k-key database.
func BenchmarkScan(b *testing.B) {
	db := openBench(b, b.TempDir(), anchordb.SyncNone)
	defer db.Close()
	populate(b, db, benchKeys)
	for _, n := range []int{100, benchKeys} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			r := rand.New(rand.NewPCG(10, 11))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := r.IntN(benchKeys - n + 1)
				got := 0
				err := db.Scan(benchKey(start), benchKey(start+n), func(k, v []byte) bool {
					got++
					return true
				})
				if err != nil || got != n {
					b.Fatalf("scan: %v, got %d keys, want %d", err, got, n)
				}
			}
			b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "keys/s")
		})
	}
}

// BenchmarkRecovery: time for Open on a 100k-key database, either replaying
// everything from the WAL or loading it from a checkpoint.
func BenchmarkRecovery(b *testing.B) {
	for _, withCheckpoint := range []bool{false, true} {
		name := "wal-replay"
		if withCheckpoint {
			name = "checkpoint"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			db := openBench(b, dir, anchordb.SyncNone)
			populate(b, db, benchKeys)
			if withCheckpoint {
				if err := db.Checkpoint(); err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db := openBench(b, dir, anchordb.SyncAlways)
				b.StopTimer()
				st, _ := db.Stats()
				if st.Keys != benchKeys {
					b.Fatalf("recovered %d keys", st.Keys)
				}
				db.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkCheckpoint: writing (and fsyncing) a full snapshot of 100k keys.
func BenchmarkCheckpoint(b *testing.B) {
	db := openBench(b, b.TempDir(), anchordb.SyncNone)
	defer db.Close()
	populate(b, db, benchKeys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := db.Put(benchKey(0), []byte(fmt.Sprint(i))); err != nil { // make the checkpoint non-trivial
			b.Fatal(err)
		}
		b.StartTimer()
		if err := db.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
}
