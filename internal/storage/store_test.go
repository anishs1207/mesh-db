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

func TestCompactPreservesLatestValuesAndShrinksWAL(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for version := uint64(1); version <= 100; version++ {
		if _, err := store.Set("hot-key", Value{Value: "value", Version: version, Origin: "node"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Set("other", Value{Value: "kept", Version: 101, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("after-compaction", Value{Value: "appended", Version: 102, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size()/4 {
		t.Fatalf("WAL did not shrink enough: before=%d after=%d", before.Size(), after.Size())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, ok := store.Get("hot-key"); !ok || got.Version != 100 || got.Value != "value" {
		t.Fatalf("latest value not recovered: %#v ok=%v", got, ok)
	}
	if got, ok := store.Get("other"); !ok || got.Value != "kept" {
		t.Fatalf("other value not recovered: %#v ok=%v", got, ok)
	}
	if got, ok := store.Get("after-compaction"); !ok || got.Value != "appended" {
		t.Fatalf("post-compaction append not recovered: %#v ok=%v", got, ok)
	}
}

func TestOpenRestoresInterruptedCompactionBackup(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("key", Value{Value: "value", Version: 1, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, walName)
	if err := os.Rename(path, path+".backup"); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, ok := store.Get("key"); !ok || got.Value != "value" {
		t.Fatalf("backup value not recovered: %#v ok=%v", got, ok)
	}
}

func TestEntriesReturnsIndependentSnapshot(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value := Value{Value: "original", Version: 1, Origin: "node"}
	if _, err := store.Set("key", value); err != nil {
		t.Fatal(err)
	}
	entries := store.Entries()
	entries["key"] = Value{Value: "mutated"}
	delete(entries, "missing")
	if got, _ := store.Get("key"); got != value {
		t.Fatalf("snapshot mutated store: %#v", got)
	}
}

func TestBackupRestoresValuesAndTombstones(t *testing.T) {
	source := t.TempDir()
	store, err := Open(source)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.Set("live", Value{Value: "data", Version: 3, Origin: "n"})
	_, _ = store.Set("gone", Value{Version: 4, Origin: "n", Tombstone: true})
	backup := filepath.Join(t.TempDir(), "backup.wal")
	if err := store.Backup(backup); err != nil {
		t.Fatal(err)
	}
	if err := store.Backup(backup); err == nil {
		t.Fatal("expected overwrite protection")
	}
	_ = store.Close()

	restoreDir := t.TempDir()
	if err := os.Rename(backup, filepath.Join(restoreDir, walName)); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(restoreDir)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if got, ok := restored.Get("live"); !ok || got.Value != "data" {
		t.Fatalf("live = %#v %v", got, ok)
	}
	if got, ok := restored.Get("gone"); !ok || !got.Tombstone {
		t.Fatalf("gone = %#v %v", got, ok)
	}
}
