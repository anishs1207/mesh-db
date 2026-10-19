// Package identity manages a stable per-data-directory node identity.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anishs1207/mesh-db/internal/fsutil"
)

const fileName = "node.id"

// LoadOrCreate returns the stable 128-bit identity stored in dir.
func LoadOrCreate(dir string) (string, error) {
	path := filepath.Join(dir, fileName)
	contents, err := os.ReadFile(path)
	if err == nil {
		return validate(string(contents))
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("read node identity: %w", err)
	}

	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate node identity: %w", err)
	}
	id := hex.EncodeToString(raw[:])
	f, err := os.CreateTemp(dir, ".node-id-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temporary node identity: %w", err)
	}
	tempPath := f.Name()
	defer os.Remove(tempPath)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("protect node identity: %w", err)
	}
	if _, err := f.WriteString(id + "\n"); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write node identity: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("sync node identity: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close node identity: %w", err)
	}
	// Linking publishes a complete inode without overwriting an identity that a
	// concurrent creator may have installed first.
	if err := os.Link(tempPath, path); err != nil {
		if contents, readErr := os.ReadFile(path); readErr == nil {
			return validate(string(contents))
		}
		return "", fmt.Errorf("publish node identity: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return "", fmt.Errorf("persist node identity directory entry: %w", err)
	}
	return id, nil
}

func validate(value string) (string, error) {
	id := strings.TrimSpace(value)
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || len(id) != 32 {
		return "", fmt.Errorf("invalid node identity in %s", fileName)
	}
	return strings.ToLower(id), nil
}
