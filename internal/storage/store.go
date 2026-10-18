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
	"sync"
)

const (
	walName       = "meshdb.wal"
	headerSize    = 8
	maxRecordSize = 16 << 20
)

// Value is a versioned database value. Version and Origin form a total order.
type Value struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Origin  string `json:"origin"`
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
	maxVersion uint64
}

// Open recovers a store from dir and opens its WAL for appends.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	s := &Store{data: make(map[string]Value), wal: f}
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
	payload, err := json.Marshal(walRecord{Key: key, Value: value})
	if err != nil {
		return false, fmt.Errorf("encode WAL record: %w", err)
	}
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if err := writeFull(s.wal, header); err != nil {
		return false, fmt.Errorf("write WAL header: %w", err)
	}
	if err := writeFull(s.wal, payload); err != nil {
		return false, fmt.Errorf("write WAL record: %w", err)
	}
	if err := s.wal.Sync(); err != nil {
		return false, fmt.Errorf("sync WAL: %w", err)
	}
	s.apply(key, value)
	return true, nil
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
