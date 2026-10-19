// Package hints persists replica deliveries that must be retried.
package hints

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/anishs1207/mesh-db/internal/fsutil"
	"github.com/anishs1207/mesh-db/internal/storage"
)

const hintFormat = 1

type Hint struct {
	Format uint8         `json:"format,omitempty"`
	ID     string        `json:"id"`
	Owner  string        `json:"owner"`
	Key    string        `json:"key"`
	Value  storage.Value `json:"value"`
}

type Store struct {
	dir        string
	mu         sync.Mutex
	used       int64
	maxBytes   int64
	scanCursor string
	latest     map[string]storedHint
}

type storedHint struct {
	hint Hint
	size int64
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
	latest := make(map[string]storedHint)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		used += info.Size()
		data, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		if readErr != nil {
			return nil, readErr
		}
		hint, decodeErr := validatedHint(entry.Name(), data)
		if decodeErr != nil {
			continue
		}
		slot := hintSlot(hint.Owner, hint.Key)
		current, ok := latest[slot]
		if !ok || newerHint(hint, current.hint) {
			latest[slot] = storedHint{hint: hint, size: info.Size()}
		}
	}
	return &Store{dir: dir, used: used, latest: latest}, nil
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
	if strings.TrimSpace(owner) == "" || strings.ContainsAny(owner, " \t\r\n") {
		return fmt.Errorf("invalid hint owner")
	}
	if err := storage.Validate(key, value); err != nil {
		return fmt.Errorf("invalid hint value: %w", err)
	}
	slot := hintSlot(owner, key)
	previous, hasPrevious := s.latest[slot]
	incoming := Hint{Format: hintFormat, Owner: owner, Key: key, Value: value}
	if hasPrevious && storage.VersionConflict(previous.hint.Value, value) {
		return fmt.Errorf("hint version conflict: %w", storage.ErrVersionConflict)
	}
	if hasPrevious && !newerHint(incoming, previous.hint) {
		return nil
	}
	id := contentID(owner, key, value)
	target := filepath.Join(s.dir, id+".json")
	hint := Hint{Format: hintFormat, ID: id, Owner: owner, Key: key, Value: value}
	data, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("encode hint: %w", err)
	}
	projected := s.used + int64(len(data))
	if hasPrevious {
		projected -= previous.size
	}
	if s.maxBytes > 0 && projected > s.maxBytes {
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
	s.latest[slot] = storedHint{hint: hint, size: int64(len(data))}
	if hasPrevious && previous.hint.ID != id {
		oldPath := filepath.Join(s.dir, previous.hint.ID+".json")
		if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove superseded hint: %w", err)
		}
		if err := fsutil.SyncDir(s.dir); err != nil {
			return fmt.Errorf("persist superseded hint removal: %w", err)
		}
		s.used -= previous.size
	}
	return nil
}

func (s *Store) List() ([]Hint, error) {
	return s.ListLimit(0)
}

// ListLimit returns at most limit valid hints in stable filename order. A
// non-positive limit preserves the unbounded behavior for administrative use.
func (s *Store) ListLimit(limit int) ([]Hint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	files := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			files = append(files, entry)
		}
	}
	start := 0
	if limit > 0 && s.scanCursor != "" {
		start = sort.Search(len(files), func(i int) bool { return files[i].Name() > s.scanCursor })
		if start == len(files) {
			start = 0
		}
	}
	var result []Hint
	var decodeErrors []error
	for visited := 0; visited < len(files) && (limit <= 0 || len(result) < limit); visited++ {
		entry := files[(start+visited)%len(files)]
		if limit > 0 {
			s.scanCursor = entry.Name()
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		hint, err := validatedHint(entry.Name(), data)
		if err != nil {
			decodeErrors = append(decodeErrors, fmt.Errorf("decode hint %s: %w", entry.Name(), err))
			continue
		}
		result = append(result, hint)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, errors.Join(decodeErrors...)
}

func validatedHint(name string, data []byte) (Hint, error) {
	hint, err := decodeHint(data)
	if err != nil {
		return Hint{}, err
	}
	fileID := strings.TrimSuffix(name, ".json")
	if !validID(fileID) || hint.ID != fileID {
		return Hint{}, fmt.Errorf("mismatched or invalid id")
	}
	expectedID := contentID(hint.Owner, hint.Key, hint.Value)
	if hint.Format == 0 {
		expectedID = legacyID(hint.Owner, hint.Key, hint.Value)
	}
	if hint.ID != expectedID {
		return Hint{}, fmt.Errorf("failed content identity validation")
	}
	if strings.TrimSpace(hint.Owner) == "" || strings.ContainsAny(hint.Owner, " \t\r\n") {
		return Hint{}, fmt.Errorf("invalid hint owner")
	}
	if err := storage.Validate(hint.Key, hint.Value); err != nil {
		return Hint{}, fmt.Errorf("invalid hint value: %w", err)
	}
	return hint, nil
}

func hintSlot(owner, key string) string { return owner + "\x00" + key }

func newerHint(incoming, current Hint) bool {
	return incoming.Value.Version > current.Value.Version ||
		(incoming.Value.Version == current.Value.Version && incoming.Value.Origin > current.Value.Origin)
}

func decodeHint(data []byte) (Hint, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var hint Hint
	if err := decoder.Decode(&hint); err != nil {
		return Hint{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Hint{}, fmt.Errorf("multiple JSON values")
		}
		return Hint{}, fmt.Errorf("trailing data: %w", err)
	}
	if hint.Format != 0 && hint.Format != hintFormat {
		return Hint{}, fmt.Errorf("unsupported hint format %d", hint.Format)
	}
	return hint, nil
}

func contentID(owner, key string, value storage.Value) string {
	payload, _ := json.Marshal(struct {
		Owner string        `json:"owner"`
		Key   string        `json:"key"`
		Value storage.Value `json:"value"`
	}{Owner: owner, Key: key, Value: value})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func legacyID(owner, key string, value storage.Value) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", owner, key, value.Version, value.Origin)))
	return hex.EncodeToString(sum[:])
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
		for slot, stored := range s.latest {
			if stored.hint.ID == id {
				delete(s.latest, slot)
				break
			}
		}
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
