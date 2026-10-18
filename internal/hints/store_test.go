package hints

import (
	"testing"

	"github.com/anishs1207/mesh-db/internal/storage"
)

func TestHintsSurviveReopenAndDeduplicate(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := storage.Value{Value: "v", Version: 7, Origin: "n"}
	if err := store.Put("owner", "key", value); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("owner", "key", value); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Value != value {
		t.Fatalf("hints = %#v", items)
	}
	if err := store.Delete(items[0].ID); err != nil {
		t.Fatal(err)
	}
	items, err = store.List()
	if err != nil || len(items) != 0 {
		t.Fatalf("after delete: %#v %v", items, err)
	}
}

func TestHintCapacityIsEnforcedAndReclaimed(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.SetMaxBytes(200)
	value := storage.Value{Value: "small", Version: 1, Origin: "n"}
	if err := store.Put("owner", "first", value); err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%v err=%v", items, err)
	}
	used, maximum := store.Usage()
	if used <= 0 || maximum != 200 {
		t.Fatalf("usage=%d max=%d", used, maximum)
	}
	if err := store.Put("owner", "second", storage.Value{Value: string(make([]byte, 300)), Version: 2, Origin: "n"}); err == nil {
		t.Fatal("expected capacity rejection")
	}
	if err := store.Delete(items[0].ID); err != nil {
		t.Fatal(err)
	}
	used, _ = store.Usage()
	if used != 0 {
		t.Fatalf("usage after delete=%d", used)
	}
}
