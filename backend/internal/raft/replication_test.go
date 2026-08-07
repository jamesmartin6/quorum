package raft

import "testing"

func TestAppendEntries_RejectsOnPrevLogMismatch(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Save(PersistentState{
		CurrentTerm: 2,
		Log:         []LogEntry{{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"}},
	})
	r := New(Config{ID: "n1", Peers: []string{"n2"}, Transport: noopTransport{}, Storage: storage})

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  99, // our entry at index 1 is actually term 1
	})
	if reply.Success {
		t.Fatalf("expected rejection on prevLogTerm mismatch")
	}
	if reply.ConflictTerm != 1 {
		t.Fatalf("expected conflictTerm=1, got %d", reply.ConflictTerm)
	}
	if reply.ConflictIndex != 1 {
		t.Fatalf("expected conflictIndex=1, got %d", reply.ConflictIndex)
	}
}

func TestAppendEntries_RejectsWhenLogTooShort(t *testing.T) {
	r := New(Config{ID: "n1", Peers: []string{"n2"}, Transport: noopTransport{}, Storage: NewMemoryStorage()})

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 5, // we have no entries at all
		PrevLogTerm:  1,
	})
	if reply.Success {
		t.Fatalf("expected rejection when follower's log doesn't reach prevLogIndex")
	}
	if reply.ConflictIndex != 1 {
		t.Fatalf("expected conflictIndex=1 (append starting point) for an empty log, got %d", reply.ConflictIndex)
	}
}

func TestAppendEntries_TruncatesConflictingSuffix(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Save(PersistentState{
		CurrentTerm: 3,
		Log: []LogEntry{
			{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"},
			{Index: 2, Term: 1, Op: "SET", Key: "b", Value: "2"},
			{Index: 3, Term: 2, Op: "SET", Key: "c", Value: "stale"},
		},
	})
	r := New(Config{ID: "n1", Peers: []string{"n2"}, Transport: noopTransport{}, Storage: storage})

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:         3,
		LeaderID:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Index: 3, Term: 3, Op: "SET", Key: "c", Value: "fresh"}},
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	entries := r.LogEntries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after truncate+append, got %d", len(entries))
	}
	if entries[2].Term != 3 || entries[2].Value != "fresh" {
		t.Fatalf("expected the stale term-2 entry at index 3 to be overwritten, got %+v", entries[2])
	}
}

func TestAppendEntries_AdvancesCommitIndexFromLeaderCommit(t *testing.T) {
	r := New(Config{ID: "n1", Transport: noopTransport{}, Storage: NewMemoryStorage()})

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:     1,
		LeaderID: "leader",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Op: "SET", Key: "x", Value: "1"},
			{Index: 2, Term: 1, Op: "SET", Key: "y", Value: "2"},
		},
		LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	if got := r.Status().CommitIndex; got != 1 {
		t.Fatalf("expected commitIndex=1 (min of leaderCommit and last new entry), got %d", got)
	}
}

func TestAppendEntries_CommitIndexNeverExceedsLastNewEntry(t *testing.T) {
	// leaderCommit can outrun what THIS AppendEntries call actually
	// delivered (e.g. a lagging follower); commitIndex must not jump past
	// entries the follower doesn't actually have yet.
	r := New(Config{ID: "n1", Transport: noopTransport{}, Storage: NewMemoryStorage()})

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		Entries:      []LogEntry{{Index: 1, Term: 1, Op: "SET", Key: "x", Value: "1"}},
		LeaderCommit: 100,
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	if got := r.Status().CommitIndex; got != 1 {
		t.Fatalf("expected commitIndex clamped to 1 (last new entry), got %d", got)
	}
}

func TestPropose_SingleNodeClusterCommitsImmediately(t *testing.T) {
	r := New(Config{ID: "solo", Transport: noopTransport{}, Storage: NewMemoryStorage()})
	r.mu.Lock()
	r.role = Leader
	r.currentTerm = 1
	r.leaderID = r.id
	r.nextIndex = map[string]uint64{}
	r.matchIndex = map[string]uint64{}
	r.mu.Unlock()

	idx, _, ok := r.Propose("SET", "k", "v")
	if !ok {
		t.Fatalf("expected Propose to succeed on a leader")
	}
	if got := r.Status().CommitIndex; got != idx {
		t.Fatalf("expected a single-node cluster to commit immediately: commitIndex=%d, want %d", got, idx)
	}
}

func TestPropose_RejectedWhenNotLeader(t *testing.T) {
	r := New(Config{ID: "n1", Peers: []string{"n2"}, Transport: noopTransport{}, Storage: NewMemoryStorage()})
	_, _, ok := r.Propose("SET", "k", "v")
	if ok {
		t.Fatalf("expected Propose to fail on a non-leader (default role is Follower)")
	}
}

// TestAdvanceCommitIndex_RequiresCurrentTermEntry guards against the
// classic Raft "Figure 8" safety hazard: a leader must never directly
// commit a log entry from an earlier term just because it's replicated on
// a majority - it may only commit entries from ITS OWN term directly.
// Earlier-term entries become committed transitively once a later,
// current-term entry commits.
func TestAdvanceCommitIndex_RequiresCurrentTermEntry(t *testing.T) {
	r := New(Config{ID: "leader", Peers: []string{"p1", "p2"}, Transport: noopTransport{}, Storage: NewMemoryStorage()})

	r.mu.Lock()
	r.currentTerm = 2
	r.role = Leader
	r.leaderID = r.id
	r.entries = NewLog([]LogEntry{{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"}})
	r.nextIndex = map[string]uint64{"p1": 2, "p2": 2}
	r.matchIndex = map[string]uint64{"p1": 1, "p2": 1} // replicated on all 3 nodes (majority)
	r.advanceCommitIndexLocked()
	oldTermCommit := r.commitIndex
	r.mu.Unlock()

	if oldTermCommit != 0 {
		t.Fatalf("expected leader NOT to directly commit an old-term entry despite majority replication, got commitIndex=%d", oldTermCommit)
	}

	r.mu.Lock()
	idx := r.entries.Append(r.currentTerm, LogEntry{Op: "SET", Key: "b", Value: "2"})
	r.matchIndex["p1"] = idx
	r.matchIndex["p2"] = idx
	r.advanceCommitIndexLocked()
	finalCommit := r.commitIndex
	r.mu.Unlock()

	if finalCommit != idx {
		t.Fatalf("expected commitIndex to advance to %d (transitively committing the old entry too) once a current-term entry reaches a majority, got %d", idx, finalCommit)
	}
}
