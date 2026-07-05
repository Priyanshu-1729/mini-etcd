package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
)

// Snapshot captures the full state machine at a point in time.
// lastIndex and lastTerm identify which Raft log entry it represents
// so the Raft layer knows how much of the WAL it can safely truncate.
type Snapshot struct {
	LastIndex uint64            `json:"last_index"`
	LastTerm  uint64            `json:"last_term"`
	Data      map[string]string `json:"data"`
}

// Save atomically writes a snapshot to disk.
// We write to a temp file first then rename so the file is
// never in a partial state if we crash mid-write.
func Save(path string, snap Snapshot) error {
	tmp := path + ".tmp"
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("snapshot: write tmp: %w", err)
	}
	// atomic rename — either the full file exists or the old one does
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	return nil
}

// Load reads a snapshot from disk.
// Returns nil, nil if no snapshot exists yet (first boot).
func Load(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snapshot: read: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("snapshot: unmarshal: %w", err)
	}
	return &snap, nil
}
