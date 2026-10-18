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
