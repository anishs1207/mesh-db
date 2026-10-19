package storage

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/anishs1207/mesh-db/internal/fsutil"
)

const (
	walName       = "meshdb.wal"
	headerSize    = 8
	maxRecordSize = 16 << 20
	maxKeySize    = 4 << 10
	maxValueSize  = 1 << 20
	maxOriginSize = 4 << 10
	walFormat     = 1
)

var (
	ErrWALCapacity     = errors.New("WAL capacity exceeded")
	ErrKeyCapacity     = errors.New("retained key capacity exceeded")
	ErrVersionConflict = errors.New("same version and origin contain different state")
)

// Value is a versioned database value. Version and Origin form a total order.
type Value struct {
	Value     string `json:"value,omitempty"`
	Version   uint64 `json:"version"`
	Origin    string `json:"origin"`
	Tombstone bool   `json:"tombstone,omitempty"`
}

type walRecord struct {
	Format uint8  `json:"format,omitempty"`
	Key    string `json:"key"`
	Value
}

type stringMaxHeap []string

func (h stringMaxHeap) Len() int           { return len(h) }
func (h stringMaxHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h stringMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *stringMaxHeap) Push(value any)    { *h = append(*h, value.(string)) }
func (h *stringMaxHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// Store is a durable, thread-safe key-value store backed by a checksummed WAL.
type Store struct {
	mu         sync.RWMutex
	data       map[string]Value
	wal        *os.File
	lockFile   *os.File
	walPath    string
	maxVersion uint64
	records    uint64
	maxBytes   int64
	rejections uint64
	maxKeys    int
	keyRejects uint64
	closed     bool
}

// Open recovers a store from dir and opens its WAL for appends.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, ".meshdb.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock: %w", err)
	}
	if err := lockExclusive(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("data directory is already in use: %w", err)
	}
	keepLock := false
	defer func() {
		if !keepLock {
			_ = unlockExclusive(lockFile)
			_ = lockFile.Close()
		}
	}()

	path := filepath.Join(dir, walName)
	backupPath := path + ".backup"
	walExists := true
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		walExists = false
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			if err := os.Rename(backupPath, path); err != nil {
				return nil, fmt.Errorf("restore WAL backup: %w", err)
			}
			if err := fsutil.SyncDir(dir); err != nil {
				return nil, fmt.Errorf("persist restored WAL backup: %w", err)
			}
			walExists = true
		} else if !errors.Is(backupErr, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect WAL backup: %w", backupErr)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect WAL: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	if !walExists {
		if err := fsutil.SyncDir(dir); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("persist new WAL: %w", err)
		}
	}

	s := &Store{data: make(map[string]Value), wal: f, lockFile: lockFile, walPath: path}
	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek WAL: %w", err)
	}
	keepLock = true
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
			if err := s.wal.Sync(); err != nil {
				return fmt.Errorf("sync truncated WAL header: %w", err)
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
				if err := s.wal.Sync(); err != nil {
					return fmt.Errorf("sync truncated WAL record: %w", err)
				}
				break
			}
			return fmt.Errorf("read WAL record: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return fmt.Errorf("WAL checksum mismatch at offset %d", validOffset)
		}
		record, err := decodeRecord(payload)
		if err != nil {
			return fmt.Errorf("decode WAL record at offset %d: %w", validOffset, err)
		}
		if current, ok := s.data[record.Key]; ok && VersionConflict(current, record.Value) {
			return fmt.Errorf("WAL version conflict at offset %d: %w", validOffset, ErrVersionConflict)
		}
		s.apply(record.Key, record.Value)
		s.records++
		validOffset += int64(headerSize) + int64(length)
	}
	return nil
}

func decodeRecord(payload []byte) (walRecord, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record walRecord
	if err := decoder.Decode(&record); err != nil {
		return walRecord{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return walRecord{}, fmt.Errorf("multiple JSON values in WAL record")
		}
		return walRecord{}, fmt.Errorf("trailing WAL data: %w", err)
	}
	if record.Format != 0 && record.Format != walFormat {
		return walRecord{}, fmt.Errorf("unsupported WAL format %d", record.Format)
	}
	if err := Validate(record.Key, record.Value); err != nil {
		return walRecord{}, err
	}
	return record, nil
}

// Validate checks the semantic invariants required by ordering, addressing,
// persistence, and repair. Every state-ingress path should call it before
// accepting a Value as authoritative.
func Validate(key string, value Value) error {
	if key == "" || len(key) > maxKeySize || strings.ContainsAny(key, " \t\r\n") {
		return fmt.Errorf("invalid WAL key")
	}
	if value.Version == 0 {
		return fmt.Errorf("invalid zero WAL version")
	}
	if value.Origin == "" || len(value.Origin) > maxOriginSize || strings.ContainsAny(value.Origin, " \t\r\n") {
		return fmt.Errorf("invalid WAL origin")
	}
	if len(value.Value) > maxValueSize {
		return fmt.Errorf("WAL value exceeds %d bytes", maxValueSize)
	}
	if value.Tombstone && value.Value != "" {
		return fmt.Errorf("WAL tombstone contains a value")
	}
	return nil
}

// Set durably appends value before publishing it in memory. It returns false
// when the value loses the deterministic version/origin ordering.
func (s *Store) Set(key string, value Value) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, os.ErrClosed
	}
	if err := Validate(key, value); err != nil {
		return false, err
	}
	if current, ok := s.data[key]; ok && !newer(value, current) {
		if VersionConflict(current, value) {
			return false, ErrVersionConflict
		}
		return false, nil
	}
	if _, exists := s.data[key]; !exists && s.maxKeys > 0 && len(s.data) >= s.maxKeys {
		s.keyRejects++
		return false, fmt.Errorf("%w: retained=%d limit=%d", ErrKeyCapacity, len(s.data), s.maxKeys)
	}
	payload, err := encodeRecord(key, value)
	if err != nil {
		return false, err
	}
	offset, err := s.wal.Seek(0, io.SeekEnd)
	if err != nil {
		return false, fmt.Errorf("locate WAL append boundary: %w", err)
	}
	recordBytes := int64(headerSize + len(payload))
	if s.maxBytes > 0 && (offset > s.maxBytes || recordBytes > s.maxBytes-offset) {
		s.rejections++
		return false, fmt.Errorf("%w: current=%d record=%d limit=%d", ErrWALCapacity, offset, recordBytes, s.maxBytes)
	}
	if err := appendPayload(s.wal, payload); err != nil {
		return false, s.rollbackAppend(offset, err)
	}
	if err := s.wal.Sync(); err != nil {
		return false, s.rollbackAppend(offset, fmt.Errorf("sync WAL: %w", err))
	}
	s.apply(key, value)
	s.records++
	return true, nil
}

func (s *Store) rollbackAppend(offset int64, cause error) error {
	if err := s.wal.Truncate(offset); err != nil {
		return fmt.Errorf("%v; WAL rollback failed: %w", cause, err)
	}
	if _, err := s.wal.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%v; WAL seek after rollback failed: %w", cause, err)
	}
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("%v; WAL rollback sync failed: %w", cause, err)
	}
	return cause
}

func appendRecord(w io.Writer, key string, value Value) error {
	payload, err := encodeRecord(key, value)
	if err != nil {
		return err
	}
	return appendPayload(w, payload)
}

func encodeRecord(key string, value Value) ([]byte, error) {
	payload, err := json.Marshal(walRecord{Format: walFormat, Key: key, Value: value})
	if err != nil {
		return nil, fmt.Errorf("encode WAL record: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxRecordSize {
		return nil, fmt.Errorf("WAL record size %d exceeds limit %d", len(payload), maxRecordSize)
	}
	return payload, nil
}

func appendPayload(w io.Writer, payload []byte) error {
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

func (s *Store) SetMaxWALBytes(maxBytes int64) {
	s.mu.Lock()
	s.maxBytes = maxBytes
	s.mu.Unlock()
}

func (s *Store) SetMaxLiveKeys(maxKeys int) {
	s.mu.Lock()
	s.maxKeys = maxKeys
	s.mu.Unlock()
}

// Compact rewrites the WAL with only the newest value for every key. The old
// WAL is retained as a backup until the replacement has been installed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}

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
	if err := fsutil.SyncDir(dir); err != nil {
		_ = os.Remove(s.walPath)
		_ = os.Rename(backupPath, s.walPath)
		s.reopenWAL()
		return fmt.Errorf("persist compacted WAL replacement: %w", err)
	}
	cleanupTemp = false
	if err := s.reopenWAL(); err != nil {
		_ = os.Remove(s.walPath)
		_ = os.Rename(backupPath, s.walPath)
		_ = s.reopenWAL()
		return fmt.Errorf("reopen compacted WAL: %w", err)
	}
	s.records = uint64(len(s.data))
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove WAL backup: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("persist WAL backup removal: %w", err)
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

// VersionConflict reports an invariant violation: a node identity allocated
// one logical version to two different states.
func VersionConflict(left, right Value) bool {
	return left.Version == right.Version && left.Origin == right.Origin && left != right
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

// Entries returns a point-in-time copy suitable for maintenance scans.
func (s *Store) Entries() map[string]Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make(map[string]Value, len(s.data))
	for key, value := range s.data {
		entries[key] = value
	}
	return entries
}

// KeysAfter returns the next lexicographically ordered key page without
// allocating in proportion to the full data set. An empty next cursor marks
// the end of a pass; callers can restart from the empty cursor on their next
// scheduled scan.
func (s *Store) KeysAfter(after string, limit int) (keys []string, next string) {
	if limit <= 0 {
		return nil, ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidateLimit := limit
	if limit < len(s.data) {
		candidateLimit++
	}
	candidates := make(stringMaxHeap, 0, candidateLimit)
	for key := range s.data {
		if key <= after {
			continue
		}
		if len(candidates) < candidateLimit {
			heap.Push(&candidates, key)
			continue
		}
		if key < candidates[0] {
			candidates[0] = key
			heap.Fix(&candidates, 0)
		}
	}
	keys = append(keys, candidates...)
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
		return keys, keys[len(keys)-1]
	}
	return keys, ""
}

// Backup writes a consistent, compact, checksummed WAL image. The destination
// must not already exist, preventing accidental backup overwrite.
func (s *Store) Backup(destination string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return os.ErrClosed
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("backup destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect backup destination: %w", err)
	}
	dir := filepath.Dir(destination)
	temp, err := os.CreateTemp(dir, ".meshdb-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("create backup: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := appendRecord(temp, key, s.data[key]); err != nil {
			_ = temp.Close()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync backup: %w", err)
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := publishNewFile(tempPath, destination); err != nil {
		return fmt.Errorf("install backup: %w", err)
	}
	return nil
}

func publishNewFile(tempPath, destination string) error {
	if err := os.Link(tempPath, destination); err != nil {
		return fmt.Errorf("publish without overwrite: %w", err)
	}
	dir := filepath.Dir(destination)
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("persist published file: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("remove published temporary link: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("persist temporary-link removal: %w", err)
	}
	return nil
}

func (s *Store) MaxVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxVersion
}

// WALStats returns the current log size, total records, and live keys. The
// record count makes obsolete-version amplification observable without
// decoding the WAL again.
func (s *Store) WALStats() (bytes int64, records uint64, liveKeys int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, 0, 0, os.ErrClosed
	}
	info, err := s.wal.Stat()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("stat WAL: %w", err)
	}
	return info.Size(), s.records, len(s.data), nil
}

// WALCapacityStats reports the configured hard limit and the number of writes
// rejected before append because they would exceed it.
func (s *Store) WALCapacityStats() (maximum int64, rejections uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxBytes, s.rejections
}

// KeyCapacityStats reports the retained-key limit and admission rejections.
// Tombstones count as retained keys because they consume memory and are needed
// to prevent resurrection.
func (s *Store) KeyCapacityStats() (maximum int, rejections uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxKeys, s.keyRejects
}

// NeedsCompaction reports whether both the size floor and stale-record ratio
// are exceeded. Requiring both avoids repeatedly rewriting a large but mostly
// live data set.
func (s *Store) NeedsCompaction(minBytes int64, staleRatio float64) (bool, error) {
	bytes, records, liveKeys, err := s.WALStats()
	if err != nil {
		return false, err
	}
	if minBytes <= 0 || bytes < minBytes || records == 0 {
		return false, nil
	}
	if staleRatio < 1 {
		staleRatio = 1
	}
	live := uint64(liveKeys)
	if live == 0 {
		return true, nil
	}
	return float64(records)/float64(live) >= staleRatio, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil && s.lockFile == nil {
		return nil
	}
	var closeErrors []error
	if s.wal != nil {
		closeErrors = append(closeErrors, s.wal.Close())
		s.wal = nil
	}
	if s.lockFile != nil {
		closeErrors = append(closeErrors, unlockExclusive(s.lockFile), s.lockFile.Close())
		s.lockFile = nil
	}
	s.closed = true
	return errors.Join(closeErrors...)
}
