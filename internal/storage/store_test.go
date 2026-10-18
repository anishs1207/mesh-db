package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func writeFramedPayload(t *testing.T, dir string, payload []byte) {
	t.Helper()
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if err := os.WriteFile(filepath.Join(dir, walName), append(header, payload...), 0o600); err != nil {
		t.Fatal(err)
	}
}

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

func TestRecoveryAcceptsLegacyRecordWithoutFormatMarker(t *testing.T) {
	dir := t.TempDir()
	payload, err := json.Marshal(struct {
		Key       string `json:"key"`
		Value     string `json:"value"`
		Version   uint64 `json:"version"`
		Origin    string `json:"origin"`
		Tombstone bool   `json:"tombstone,omitempty"`
	}{Key: "legacy", Value: "value", Version: 7, Origin: "old-node"})
	if err != nil {
		t.Fatal(err)
	}
	writeFramedPayload(t, dir, payload)

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, ok := store.Get("legacy"); !ok || got.Value != "value" || got.Version != 7 {
		t.Fatalf("legacy value = %#v, found=%v", got, ok)
	}
}

func TestRecoveryRejectsUnknownFormatAndFields(t *testing.T) {
	for name, payload := range map[string][]byte{
		"future format": []byte(`{"format":2,"key":"key","version":1,"origin":"node"}`),
		"unknown field": []byte(`{"format":1,"key":"key","version":1,"origin":"node","future":true}`),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFramedPayload(t, dir, payload)
			if _, err := Open(dir); err == nil {
				t.Fatal("incompatible WAL record was accepted")
			}
		})
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

func TestCompactionEligibilityUsesSizeAndStaleRatio(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for version := uint64(1); version <= 5; version++ {
		if _, err := store.Set("hot", Value{Value: "value", Version: version, Origin: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	bytes, records, live, err := store.WALStats()
	if err != nil || bytes <= 0 || records != 5 || live != 1 {
		t.Fatalf("stats bytes=%d records=%d live=%d err=%v", bytes, records, live, err)
	}
	needed, err := store.NeedsCompaction(1, 2)
	if err != nil || !needed {
		t.Fatalf("expected compaction: needed=%v err=%v", needed, err)
	}
	needed, err = store.NeedsCompaction(bytes+1, 2)
	if err != nil || needed {
		t.Fatalf("size floor ignored: needed=%v err=%v", needed, err)
	}
	if err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	_, records, live, err = store.WALStats()
	if err != nil || records != 1 || live != 1 {
		t.Fatalf("post-compact records=%d live=%d err=%v", records, live, err)
	}
}

func TestOperationsAfterCloseReturnErrors(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("key", Value{Value: "value", Version: 1, Origin: "n"}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Set after close error = %v", err)
	}
	if err := store.Compact(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Compact after close error = %v", err)
	}
	if err := store.Backup(filepath.Join(t.TempDir(), "backup.wal")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Backup after close error = %v", err)
	}
	if _, _, _, err := store.WALStats(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("WALStats after close error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}
