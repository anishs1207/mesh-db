// Package hints persists replica deliveries that must be retried.
package hints

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/anishs1207/mesh-db/internal/fsutil"
	"github.com/anishs1207/mesh-db/internal/storage"
)

type Hint struct {
	ID    string        `json:"id"`
	Owner string        `json:"owner"`
	Key   string        `json:"key"`
	Value storage.Value `json:"value"`
}

type Store struct {
	dir      string
	mu       sync.Mutex
	used     int64
	maxBytes int64
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create hints directory: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var used int64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		used += info.Size()
	}
	return &Store{dir: dir, used: used}, nil
}

func (s *Store) SetMaxBytes(maxBytes int64) { s.mu.Lock(); s.maxBytes = maxBytes; s.mu.Unlock() }

func (s *Store) Usage() (used, maximum int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used, s.maxBytes
}

func (s *Store) Put(owner, key string, value storage.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", owner, key, value.Version, value.Origin)))
	id := hex.EncodeToString(sum[:])
	target := filepath.Join(s.dir, id+".json")
	if _, err := os.Stat(target); err == nil {
		return nil
	}
	hint := Hint{ID: id, Owner: owner, Key: key, Value: value}
	data, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("encode hint: %w", err)
	}
	if s.maxBytes > 0 && s.used+int64(len(data)) > s.maxBytes {
		return fmt.Errorf("hint capacity exceeded: used=%d limit=%d", s.used, s.maxBytes)
	}
	temp, err := os.CreateTemp(s.dir, ".hint-*.tmp")
	if err != nil {
		return fmt.Errorf("create hint: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write hint: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync hint: %w", err)
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, target); err != nil {
		return fmt.Errorf("install hint: %w", err)
	}
	if err := fsutil.SyncDir(s.dir); err != nil {
		return fmt.Errorf("persist hint directory entry: %w", err)
	}
	s.used += int64(len(data))
	return nil
}

func (s *Store) List() ([]Hint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var result []Hint
	var decodeErrors []error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var hint Hint
		if err := json.Unmarshal(data, &hint); err != nil {
			decodeErrors = append(decodeErrors, fmt.Errorf("decode hint %s: %w", entry.Name(), err))
			continue
		}
		fileID := strings.TrimSuffix(entry.Name(), ".json")
		if !validID(fileID) || hint.ID != fileID {
			decodeErrors = append(decodeErrors, fmt.Errorf("hint %s has mismatched or invalid id", entry.Name()))
			continue
		}
		result = append(result, hint)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, errors.Join(decodeErrors...)
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validID(id) {
		return fmt.Errorf("invalid hint id")
	}
	path := filepath.Join(s.dir, id+".json")
	info, statErr := os.Stat(path)
	if os.IsNotExist(statErr) {
		return nil
	}
	if statErr != nil {
		return statErr
	}
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		s.used -= info.Size()
		if syncErr := fsutil.SyncDir(s.dir); syncErr != nil {
			return fmt.Errorf("persist hint deletion: %w", syncErr)
		}
	}
	return err
}

func validID(id string) bool {
	if len(id) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == sha256.Size
}
