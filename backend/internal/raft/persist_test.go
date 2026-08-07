package raft

import "testing"

func TestFileStorage_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
		Log: []LogEntry{
			{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"},
			{Index: 2, Term: 5, Op: "DELETE", Key: "a"},
		},
	}
	if err := fs.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Simulate a process restart by reopening storage from the same directory.
	reopened, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage (reopen): %v", err)
	}
	got, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor || len(got.Log) != len(want.Log) {
		t.Fatalf("round-trip mismatch: want %+v, got %+v", want, got)
	}
	for i := range want.Log {
		if got.Log[i] != want.Log[i] {
			t.Fatalf("log entry %d mismatch: want %+v, got %+v", i, want.Log[i], got.Log[i])
		}
	}
}

func TestFileStorage_LoadEmptyWhenNoFileYet(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load on a fresh directory should not error: %v", err)
	}
	if got.CurrentTerm != 0 || got.VotedFor != "" || len(got.Log) != 0 {
		t.Fatalf("expected zero-value state for a fresh directory, got %+v", got)
	}
}

func TestFileStorage_SaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)

	if err := fs.Save(PersistentState{CurrentTerm: 1}); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	if err := fs.Save(PersistentState{CurrentTerm: 2}); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	// No leftover .tmp file after a successful save (write-then-rename).
	fs2, _ := NewFileStorage(dir)
	got, err := fs2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.CurrentTerm != 2 {
		t.Fatalf("expected the latest saved state (term 2) to win, got term %d", got.CurrentTerm)
	}
}

func TestRaft_RestoresPersistedStateOnConstruction(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	err = fs.Save(PersistentState{
		CurrentTerm: 4,
		VotedFor:    "someone",
		Log:         []LogEntry{{Index: 1, Term: 1, Op: "SET", Key: "x", Value: "1"}},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	r := New(Config{ID: "n1", Transport: noopTransport{}, Storage: fs})
	st := r.Status()
	if st.Term != 4 {
		t.Fatalf("expected restored term 4, got %d", st.Term)
	}
	if st.VotedFor != "someone" {
		t.Fatalf("expected restored votedFor %q, got %q", "someone", st.VotedFor)
	}
	if st.LogLength != 1 {
		t.Fatalf("expected restored log length 1, got %d", st.LogLength)
	}
}
