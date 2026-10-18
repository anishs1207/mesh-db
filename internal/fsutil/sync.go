// Package fsutil contains filesystem durability helpers shared by persistent
// MeshDB components.
package fsutil

import (
	"fmt"
	"os"
	"runtime"
)

// SyncDir persists directory-entry changes on filesystems that expose
// directory fsync. Windows does not support Sync on directory handles through
// os.File; atomic rename still provides replacement atomicity there.
func SyncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}
