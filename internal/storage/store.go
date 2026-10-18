package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	walName       = "meshdb.wal"
	headerSize    = 8
	maxRecordSize = 16 << 20
)

// Value is a versioned database value. Version and Origin form a total order.
type Value struct {
	Value     string `json:"value,omitempty"`
	Version   uint64 `json:"version"`
	Origin    string `json:"origin"`
	Tombstone bool   `json:"tombstone,omitempty"`
}

type walRecord struct {
	Key string `json:"key"`
	Value
}

// Store is a durable, thread-safe key-value store backed by a checksummed WAL.
type Store struct {
	mu         sync.RWMutex
	data       map[string]Value
	wal        *os.File
	walPath    string
	maxVersion uint64
}

// Open recovers a store from dir and opens its WAL for appends.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	path := filepath.Join(dir, walName)
	backupPath := path + ".backup"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			if err := os.Rename(backupPath, path); err != nil {
				return nil, fmt.Errorf("restore WAL backup: %w", err)
			}
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	s := &Store{data: make(map[string]Value), wal: f, walPath: path}
	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek WAL: %w", err)
	}
	return s, nil
}

func (s *Store) recover() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.wal)
	var validOffset int64
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(r, header)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := s.wal.Truncate(validOffset); err != nil {
				return fmt.Errorf("truncate partial WAL header: %w", err)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("read WAL header: %w", err)
		}

		length := binary.BigEndian.Uint32(header[:4])
		checksum := binary.BigEndian.Uint32(header[4:])
		if length == 0 || length > maxRecordSize {
			return fmt.Errorf("invalid WAL record length %d at offset %d", length, validOffset)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if err := s.wal.Truncate(validOffset); err != nil {
					return fmt.Errorf("truncate partial WAL record: %w", err)
				}
				break
			}
			return fmt.Errorf("read WAL record: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return fmt.Errorf("WAL checksum mismatch at offset %d", validOffset)
		}
		var record walRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode WAL record at offset %d: %w", validOffset, err)
		}
		s.apply(record.Key, record.Value)
		validOffset += int64(headerSize) + int64(length)
	}
	return nil
}

// Set durably appends value before publishing it in memory. It returns false
// when the value loses the deterministic version/origin ordering.
func (s *Store) Set(key string, value Value) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.data[key]; ok && !newer(value, current) {
		return false, nil
	}
	if err := appendRecord(s.wal, key, value); err != nil {
		return false, err
	}
	if err := s.wal.Sync(); err != nil {
		return false, fmt.Errorf("sync WAL: %w", err)
	}
	s.apply(key, value)
	return true, nil
}

func appendRecord(w io.Writer, key string, value Value) error {
	payload, err := json.Marshal(walRecord{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("encode WAL record: %w", err)
	}
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if err := writeFull(w, header); err != nil {
		return fmt.Errorf("write WAL header: %w", err)
	}
	if err := writeFull(w, payload); err != nil {
		return fmt.Errorf("write WAL record: %w", err)
	}
	return nil
}

// Compact rewrites the WAL with only the newest value for every key. The old
// WAL is retained as a backup until the replacement has been installed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.walPath)
	temp, err := os.CreateTemp(dir, ".meshdb-wal-*.tmp")
	if err != nil {
		return fmt.Errorf("create compacted WAL: %w", err)
	}
	tempPath := temp.Name()
	cleanupTemp := true
	defer func() {
		_ = temp.Close()
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("protect compacted WAL: %w", err)
	}

	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := appendRecord(temp, key, s.data[key]); err != nil {
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync compacted WAL: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close compacted WAL: %w", err)
	}
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("close active WAL: %w", err)
	}

	backupPath := s.walPath + ".backup"
	_ = os.Remove(backupPath)
	if err := os.Rename(s.walPath, backupPath); err != nil {
		s.reopenWAL()
		return fmt.Errorf("backup active WAL: %w", err)
	}
	if err := os.Rename(tempPath, s.walPath); err != nil {
		_ = os.Rename(backupPath, s.walPath)
		s.reopenWAL()
		return fmt.Errorf("install compacted WAL: %w", err)
	}
	cleanupTemp = false
	if err := s.reopenWAL(); err != nil {
		_ = os.Remove(s.walPath)
		_ = os.Rename(backupPath, s.walPath)
		_ = s.reopenWAL()
		return fmt.Errorf("reopen compacted WAL: %w", err)
	}
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove WAL backup: %w", err)
	}
	return nil
}

func (s *Store) reopenWAL() error {
	f, err := os.OpenFile(s.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	s.wal = f
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func newer(incoming, current Value) bool {
	return incoming.Version > current.Version ||
		(incoming.Version == current.Version && incoming.Origin > current.Origin)
}

func (s *Store) apply(key string, value Value) {
	if current, ok := s.data[key]; !ok || newer(value, current) {
		s.data[key] = value
	}
	if value.Version > s.maxVersion {
		s.maxVersion = value.Version
	}
}

func (s *Store) Get(key string) (Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *Store) MaxVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxVersion
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}
