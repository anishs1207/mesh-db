package storage

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func BenchmarkStoreSetDurable1KiB(b *testing.B) {
	store, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	value := strings.Repeat("x", 1024)
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		applied, err := store.Set(fmt.Sprintf("key-%d", index), Value{
			Value: value, Version: uint64(index + 1), Origin: "benchmark-node",
		})
		if err != nil || !applied {
			b.Fatalf("Set: applied=%v err=%v", applied, err)
		}
	}
}

func BenchmarkStoreGetParallel(b *testing.B) {
	store, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	const keyCount = 256
	for index := 0; index < keyCount; index++ {
		_, err := store.Set(fmt.Sprintf("key-%d", index), Value{
			Value: "value", Version: uint64(index + 1), Origin: "benchmark-node",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			index := sequence.Add(1) % keyCount
			if _, ok := store.Get(fmt.Sprintf("key-%d", index)); !ok {
				b.Fatal("seeded key missing")
			}
		}
	})
}

func BenchmarkKeysAfterMillionKeys(b *testing.B) {
	store := &Store{data: make(map[string]Value, 1_000_000)}
	for index := 0; index < 1_000_000; index++ {
		store.data[fmt.Sprintf("key-%07d", index)] = Value{Version: 1, Origin: "benchmark-node"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		keys, _ := store.KeysAfter("key-0499999", 1024)
		if len(keys) != 1024 {
			b.Fatalf("page size = %d", len(keys))
		}
	}
}
