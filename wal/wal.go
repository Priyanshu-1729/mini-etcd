package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Record is a single entry persisted to the WAL.
// We store Term, Index, and the raw Command bytes.
type Record struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

// WAL is an append-only log file for crash recovery.
// Each record is a newline-delimited JSON object so the file
// is human-readable and easy to replay on restart.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens or creates a WAL at the given path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &WAL{file: f}, nil
}

// Append durably writes a record to the WAL.
// The fsync ensures the record survives a crash.
func (w *WAL) Append(term, index uint64, command []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := Record{Term: term, Index: index, Command: command}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("wal: marshal: %w", err)
	}
	data = append(data, '\n')

	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	// fsync: flush OS buffer to disk so we don't lose data on crash
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// ReadAll replays all records from the WAL.
// Called once on startup to restore Raft log state.
func (w *WAL) ReadAll() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// seek to beginning for replay
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	var records []Record
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("wal: corrupt record: %w", err)
		}
		records = append(records, rec)
	}
	return records, scanner.Err()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}
