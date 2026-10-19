package hints

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestPutCoalescesVersionsForOwnerAndKey(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []storage.Value{
		{Value: "first", Version: 1, Origin: "node"},
		{Value: "newest", Version: 3, Origin: "node"},
		{Value: "obsolete", Version: 2, Origin: "node"},
	} {
		if err := store.Put("owner", "hot-key", value); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.List()
	if err != nil || len(items) != 1 || items[0].Value.Version != 3 || items[0].Value.Value != "newest" {
		t.Fatalf("coalesced hints = %#v, err=%v", items, err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("owner", "hot-key", storage.Value{Value: "after restart", Version: 4, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	items, err = store.List()
	if err != nil || len(items) != 1 || items[0].Value.Version != 4 {
		t.Fatalf("post-restart hints = %#v, err=%v", items, err)
	}
}

func TestCorruptHintDoesNotBlockHealthyDelivery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := storage.Value{Value: "healthy", Version: 3, Origin: "n"}
	if err := store.Put("owner", "key", value); err != nil {
		t.Fatal(err)
	}
	corruptID := strings.Repeat("a", 64)
	if err := os.WriteFile(filepath.Join(dir, corruptID+".json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err == nil {
		t.Fatal("expected corrupt hint diagnostic")
	}
	if len(items) != 1 || items[0].Value != value {
		t.Fatalf("healthy hints were lost: %#v", items)
	}
}

func TestHintContentTamperingIsRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("owner", "key", storage.Value{Value: "original", Version: 3, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err != nil || len(items) != 1 {
		t.Fatalf("initial hints = %#v, err=%v", items, err)
	}
	path := filepath.Join(dir, items[0].ID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "original", "tampered", 1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	items, err = store.List()
	if err == nil || len(items) != 0 {
		t.Fatalf("tampered hint accepted: %#v, err=%v", items, err)
	}
}

func TestHintSemanticViolationsAreRejected(t *testing.T) {
	dir := t.TempDir()
	invalid := storage.Value{Value: "live", Version: 3, Origin: "node", Tombstone: true}
	id := contentID("owner", "key", invalid)
	data, err := json.Marshal(Hint{Format: hintFormat, ID: id, Owner: "owner", Key: "key", Value: invalid})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err == nil || len(items) != 0 {
		t.Fatalf("invalid persisted hint accepted: items=%#v err=%v", items, err)
	}
	if err := store.Put("owner", "key", invalid); err == nil {
		t.Fatal("invalid live hint accepted")
	}
}

func TestLegacyHintIdentityRemainsReadable(t *testing.T) {
	dir := t.TempDir()
	value := storage.Value{Value: "legacy", Version: 4, Origin: "old-node"}
	id := legacyID("owner", "key", value)
	data, err := json.Marshal(Hint{ID: id, Owner: "owner", Key: "key", Value: value})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err != nil || len(items) != 1 || items[0].Value != value {
		t.Fatalf("legacy hint = %#v, err=%v", items, err)
	}
}

func TestDeleteRejectsNonHexHintID(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(strings.Repeat("/", 64)); err == nil {
		t.Fatal("expected non-hex id rejection")
	}
}

func TestListLimitBoundsDecodedHints(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for version := uint64(1); version <= 5; version++ {
		if err := store.Put("owner", fmt.Sprintf("key-%d", version), storage.Value{Value: "v", Version: version, Origin: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.ListLimit(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("limited items = %d", len(items))
	}
	all, err := store.List()
	if err != nil || len(all) != 5 {
		t.Fatalf("all items = %d err=%v", len(all), err)
	}
}

func TestListLimitRotatesAcrossBacklog(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for version := uint64(1); version <= 5; version++ {
		if err := store.Put("owner", fmt.Sprintf("key-%d", version), storage.Value{Version: version, Origin: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]struct{})
	for range 3 {
		items, err := store.ListLimit(2)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			seen[item.ID] = struct{}{}
		}
	}
	if len(seen) != 5 {
		t.Fatalf("rotating batches reached %d hints, want 5", len(seen))
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
