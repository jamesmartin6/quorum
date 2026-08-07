package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState is everything Raft must durably persist before replying
// to an RPC that changes it (currentTerm, votedFor, log).
type PersistentState struct {
	CurrentTerm uint64     `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// Storage persists and loads a node's PersistentState.
type Storage interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}

// MemoryStorage is a non-durable Storage used in unit tests, where surviving
// an actual process restart is irrelevant.
type MemoryStorage struct {
	mu    sync.Mutex
	state PersistentState
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(state PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

// FileStorage persists state as a single JSON file, written atomically
// (write to a temp file, then rename) so a crash mid-write never corrupts
// the previous good state. This is intentionally simple rather than a true
// append-only log or embedded DB: correctness and readability over
// throughput, which is the right tradeoff at this project's scale.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

// NewFileStorage returns a FileStorage persisting to <dir>/raft-state.json,
// creating dir if it doesn't exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{path: filepath.Join(dir, "raft-state.json")}, nil
}

func (f *FileStorage) Save(state PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

func (f *FileStorage) Load() (PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return PersistentState{}, nil
		}
		return PersistentState{}, err
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, err
	}
	return state, nil
}
