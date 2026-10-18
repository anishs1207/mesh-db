package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzWALRecovery(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, walName), data, 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := Open(dir)
		if err == nil {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
