package index_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/rosaiju/anchordb/internal/index"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

func collect(s *index.SkipList, start []byte) []refmodel.KV {
	var out []refmodel.KV
	for it := s.Seek(start); it.Valid(); it.Next() {
		out = append(out, refmodel.KV{Key: it.Key(), Value: it.Value()})
	}
	return out
}

func TestBasic(t *testing.T) {
	s := index.New(1)
	if _, ok := s.Get([]byte("a")); ok || s.Len() != 0 || s.Seek(nil).Valid() {
		t.Fatal("empty list not empty")
	}
	s.Set([]byte("b"), []byte("2"))
	s.Set([]byte("a"), []byte("1"))
	s.Set([]byte("b"), []byte("22"))
	if v, ok := s.Get([]byte("b")); !ok || string(v) != "22" || s.Len() != 2 {
		t.Fatal("overwrite")
	}
	if !s.Delete([]byte("a")) || s.Delete([]byte("a")) || s.Len() != 1 {
		t.Fatal("delete")
	}
	if it := s.Seek([]byte("a")); !it.Valid() || string(it.Key()) != "b" {
		t.Fatal("seek")
	}
	if it := s.Seek([]byte("c")); it.Valid() {
		t.Fatal("seek past end")
	}
}

// Randomized comparison against the map-based reference model.
func TestRandomAgainstModel(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		r := rand.New(rand.NewSource(seed))
		s := index.New(uint64(seed))
		md := refmodel.New()
		for i := 0; i < 5000; i++ {
			k := []byte(fmt.Sprintf("%03d", r.Intn(300)))
			switch r.Intn(3) {
			case 0, 1:
				v := []byte(fmt.Sprint(r.Int()))
				s.Set(k, v)
				md.Put(k, v)
			case 2:
				_, had := md.Get(k)
				if s.Delete(k) != had {
					t.Fatalf("seed %d: Delete existed mismatch", seed)
				}
				md.Delete(k)
			}
			if i%500 == 0 {
				start := []byte(fmt.Sprintf("%03d", r.Intn(300)))
				want := md.Scan(start, nil)
				got := collect(s, start)
				if len(want) != len(got) {
					t.Fatalf("seed %d: seek len %d vs %d", seed, len(got), len(want))
				}
			}
		}
		if s.Len() != md.Len() || !md.Equal(collect(s, nil)) {
			t.Fatalf("seed %d: final state differs", seed)
		}
		for _, kv := range md.All() {
			if v, ok := s.Get(kv.Key); !ok || !bytes.Equal(v, kv.Value) {
				t.Fatalf("seed %d: Get(%s)", seed, kv.Key)
			}
		}
	}
}
