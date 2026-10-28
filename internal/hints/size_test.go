package hints

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anishs1207/mesh-db/internal/storage"
)

func TestOversizedHintIsQuarantinedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := storage.Value{Value: "healthy", Version: 1, Origin: "node"}
	if err := store.Put("owner", "key", value); err != nil {
		t.Fatal(err)
	}
	oversized := filepath.Join(dir, strings.Repeat("a", 64)+".json")
	file, err := os.Create(oversized)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxHintFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if !errors.Is(err, errHintTooLarge) || len(items) != 1 || items[0].Value != value {
		t.Fatalf("items=%#v error=%v", items, err)
	}
	if store.Quarantined() != 1 {
		t.Fatalf("quarantined=%d, want 1", store.Quarantined())
	}
	if _, err := os.Stat(oversized); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized hint remains active: %v", err)
	}
}
