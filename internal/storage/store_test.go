package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryAndVersionOrdering(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.Set("account", Value{Value: "10", Version: 2, Origin: "a"}); err != nil || !applied {
		t.Fatalf("first set: applied=%v err=%v", applied, err)
	}
	if applied, err := store.Set("account", Value{Value: "stale", Version: 1, Origin: "z"}); err != nil || applied {
		t.Fatalf("stale set: applied=%v err=%v", applied, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, ok := store.Get("account")
	if !ok || got.Value != "10" || got.Version != 2 || store.MaxVersion() != 2 {
		t.Fatalf("unexpected recovered value: %#v ok=%v max=%d", got, ok, store.MaxVersion())
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("key", Value{Value: "value", Version: 1, Origin: "n1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, ok := store.Get("key"); !ok || got.Value != "value" {
		t.Fatalf("valid record lost: %#v ok=%v", got, ok)
	}
}
