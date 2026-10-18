// Package hints persists replica deliveries that must be retried.
package hints

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/anishs1207/mesh-db/internal/storage"
)

type Hint struct {
	ID    string        `json:"id"`
	Owner string        `json:"owner"`
	Key   string        `json:"key"`
	Value storage.Value `json:"value"`
}

type Store struct{ dir string }

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create hints directory: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Put(owner, key string, value storage.Value) error {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", owner, key, value.Version, value.Origin)))
	id := hex.EncodeToString(sum[:])
	hint := Hint{ID: id, Owner: owner, Key: key, Value: value}
	data, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("encode hint: %w", err)
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
	target := filepath.Join(s.dir, id+".json")
	if _, err := os.Stat(target); err == nil {
		return nil
	}
	if err := os.Rename(tempName, target); err != nil {
		return fmt.Errorf("install hint: %w", err)
	}
	return nil
}

func (s *Store) List() ([]Hint, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var result []Hint
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
			return nil, fmt.Errorf("decode hint %s: %w", entry.Name(), err)
		}
		result = append(result, hint)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *Store) Delete(id string) error {
	if len(id) != 64 {
		return fmt.Errorf("invalid hint id")
	}
	err := os.Remove(filepath.Join(s.dir, id+".json"))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
