package store

import (
	"fmt"
	"math/rand"
	"testing"
)

var benchmarkValue = testValue("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func BenchmarkLRU2HitRate(b *testing.B) {
	b.Run("zipf-hotset", func(b *testing.B) {
		rng := rand.New(rand.NewSource(1))
		zipf := rand.NewZipf(rng, 1.2, 1, 65535)
		runLRU2HitRateBenchmark(b, func(i int) string {
			return fmt.Sprintf("key-%d", zipf.Uint64())
		})
	})

	b.Run("hot-with-one-time-cold-keys", func(b *testing.B) {
		runLRU2HitRateBenchmark(b, func(i int) string {
			if i%10 == 0 {
				return fmt.Sprintf("cold-%d", i/10)
			}
			return fmt.Sprintf("hot-%d", (i*1103515245+12345)&2047)
		})
	})

	b.Run("scan-pollution", func(b *testing.B) {
		runLRU2HitRateBenchmark(b, func(i int) string {
			if i%4 == 0 {
				return fmt.Sprintf("scan-%d", i/4)
			}
			return fmt.Sprintf("hot-%d", (i*2654435761)&1023)
		})
	})
}

func BenchmarkLRU2Operations(b *testing.B) {
	b.Run("get-hit", func(b *testing.B) {
		s := newBenchmarkLRU2Store(b)
		keys := make([]string, 512)
		for i := range keys {
			keys[i] = fmt.Sprintf("key-%d", i)
			if err := s.Set(keys[i], benchmarkValue); err != nil {
				b.Fatalf("Set() error = %v", err)
			}
		}
		for _, key := range keys {
			_, _ = s.Get(key)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := s.Get(keys[i&511]); !ok {
				b.Fatal("Get() ok = false, want true")
			}
		}
	})

	b.Run("set", func(b *testing.B) {
		s := newBenchmarkLRU2Store(b)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := s.Set(fmt.Sprintf("key-%d", i), benchmarkValue); err != nil {
				b.Fatalf("Set() error = %v", err)
			}
		}
	})
}

func runLRU2HitRateBenchmark(b *testing.B, keyFor func(int) string) {
	s := newBenchmarkLRU2Store(b)

	for i := 0; i < 100000; i++ {
		key := keyFor(i)
		if _, ok := s.Get(key); !ok {
			if err := s.Set(key, benchmarkValue); err != nil {
				b.Fatalf("warmup Set() error = %v", err)
			}
		}
	}

	var hits, misses int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keyFor(i)
		if _, ok := s.Get(key); ok {
			hits++
			continue
		}
		misses++
		if err := s.Set(key, benchmarkValue); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}
	b.StopTimer()

	total := hits + misses
	if total > 0 {
		b.ReportMetric(float64(hits)*100/float64(total), "hit_pct")
		b.ReportMetric(float64(misses)*100/float64(total), "miss_pct")
	}
	b.ReportMetric(float64(s.Len()), "keys")
}

func newBenchmarkLRU2Store(b *testing.B) *LRU2Store {
	b.Helper()

	const (
		buckets      = 32
		entryBytes   = 96
		l1TargetKeys = 1024
		l2TargetKeys = 4096
	)

	s := NewLRU2Store(buckets, l1TargetKeys*entryBytes, l2TargetKeys*entryBytes)
	b.Cleanup(func() {
		_ = s.Close()
	})
	return s
}
