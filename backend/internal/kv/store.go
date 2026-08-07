// Package kv is the replicated state machine: an in-memory key-value map
// that advances strictly by applying committed Raft log entries, never by
// being written to directly. This is what makes it linearizable - every
// node applies the exact same sequence of commands in the exact same
// order, because that order was agreed on by consensus before any of them
// got here.
package kv

import (
	"sync"

	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

// Store is a simple string->string map advanced by Apply.
type Store struct {
	mu          sync.RWMutex
	data        map[string]string
	lastApplied uint64
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply advances the state machine by one committed log entry. Must be
// called with entries in strictly increasing index order (exactly what
// raft.Raft.ApplyChan delivers) - out-of-order or skipped indices would
// silently desync this node's state from the rest of the cluster.
func (s *Store) Apply(entry raft.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch entry.Op {
	case "SET":
		s.data[entry.Key] = entry.Value
	case "DELETE":
		delete(s.data, entry.Key)
	case "NOOP":
		// No-op entries exist purely so a new leader can advance
		// commitIndex right after election (see raft.becomeLeader) -
		// they carry no state-machine effect.
	}
	s.lastApplied = entry.Index
}

// Get returns the current value for key and whether it exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// LastApplied returns the index of the most recently applied entry.
func (s *Store) LastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

// Snapshot returns a copy of the entire key-value map, for debugging/UI.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Run consumes committed entries from applyCh until it's closed or ch
// yields no more values, applying each one in order. Intended to run in
// its own goroutine for the lifetime of the node process.
func (s *Store) Run(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		s.Apply(msg.Entry)
	}
}
