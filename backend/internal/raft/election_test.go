package raft

import (
	"testing"
	"time"
)

func TestElectsSingleLeaderWithinOneSecond(t *testing.T) {
	tc := newTestCluster(3)
	defer tc.stopAll()
	tc.startAll()

	leader, err := tc.waitForSingleLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("expected a single leader to be elected: %v", err)
	}
	if leader.Status().Term == 0 {
		t.Fatalf("leader's term should be > 0, got 0")
	}
}

func TestReElectionAfterLeaderKill(t *testing.T) {
	tc := newTestCluster(3)
	defer tc.stopAll()
	tc.startAll()

	firstLeader, err := tc.waitForSingleLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}
	firstTerm := firstLeader.Status().Term
	firstLeader.Kill() // simulate the leader process dying / being partitioned

	newLeader, err := tc.waitForSingleLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("re-election after leader kill failed: %v", err)
	}
	if newLeader.ID() == firstLeader.ID() {
		t.Fatalf("expected a different node to become leader after the old leader was killed")
	}
	if newLeader.Status().Term <= firstTerm {
		t.Fatalf("expected new leader's term (%d) to be greater than old leader's term (%d)", newLeader.Status().Term, firstTerm)
	}
}

func TestSplitVoteEventuallyResolves(t *testing.T) {
	// An even node count and simultaneous start maximize the chance of a
	// genuine split vote in at least one election round; regardless of
	// whether one actually occurs, this asserts the system as a whole
	// always converges on a single leader given enough retries.
	tc := newTestCluster(4)
	defer tc.stopAll()
	tc.startAll()

	if _, err := tc.waitForSingleLeader(3 * time.Second); err != nil {
		t.Fatalf("cluster failed to converge on a single leader (possible unresolved split vote): %v", err)
	}
}

func TestHigherTermCausesStepDown(t *testing.T) {
	r := New(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Transport: noopTransport{},
		Storage:   NewMemoryStorage(),
	})
	// Manually promote to leader at term 1 without starting the tick loop,
	// so this test is fully deterministic.
	r.mu.Lock()
	r.currentTerm = 1
	r.role = Leader
	r.leaderID = r.id
	r.mu.Unlock()

	reply := r.HandleAppendEntries(&AppendEntriesArgs{
		Term:     5,
		LeaderID: "n2",
	})
	if !reply.Success {
		t.Fatalf("expected AppendEntries at a higher term to succeed, got failure: %+v", reply)
	}
	st := r.Status()
	if st.Role != "follower" {
		t.Fatalf("expected node to step down to follower on seeing higher term, got role=%s", st.Role)
	}
	if st.Term != 5 {
		t.Fatalf("expected currentTerm to update to 5, got %d", st.Term)
	}
	if st.LeaderID != "n2" {
		t.Fatalf("expected leaderId to update to n2, got %s", st.LeaderID)
	}
}

func TestHigherTermInVoteReplyCausesStepDown(t *testing.T) {
	r := New(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Transport: noopTransport{},
		Storage:   NewMemoryStorage(),
	})
	r.mu.Lock()
	r.currentTerm = 1
	r.role = Candidate
	r.mu.Unlock()

	reply := r.HandleRequestVote(&RequestVoteArgs{
		Term:        7,
		CandidateID: "n2",
	})
	if !reply.VoteGranted {
		t.Fatalf("expected vote to be granted to a candidate with a higher term and an empty log")
	}
	st := r.Status()
	if st.Role != "follower" {
		t.Fatalf("expected candidate to step down to follower after seeing higher term, got role=%s", st.Role)
	}
	if st.Term != 7 {
		t.Fatalf("expected term to update to 7, got %d", st.Term)
	}
}

func TestVoteGrantedOncePerTerm(t *testing.T) {
	r := New(Config{
		ID:        "n1",
		Peers:     []string{"n2", "n3"},
		Transport: noopTransport{},
		Storage:   NewMemoryStorage(),
	})

	first := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !first.VoteGranted {
		t.Fatalf("expected first vote request in term 1 to be granted")
	}

	second := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n3"})
	if second.VoteGranted {
		t.Fatalf("expected second vote request in the same term to be denied (already voted for n2)")
	}

	// A repeat request from the SAME candidate in the same term should
	// still be granted (idempotent, e.g. retried RPC).
	repeat := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !repeat.VoteGranted {
		t.Fatalf("expected repeated vote request from the already-voted-for candidate to be granted")
	}
}

func TestRequestVoteDeniesStaleLogCandidate(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Save(PersistentState{
		CurrentTerm: 3,
		Log: []LogEntry{
			{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"},
			{Index: 2, Term: 3, Op: "SET", Key: "b", Value: "2"},
		},
	})
	r := New(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Transport: noopTransport{},
		Storage:   storage,
	})

	// Candidate's log ends at term 2, index 2 - less up-to-date than ours
	// (our last entry is term 3), so the vote must be denied even though
	// the candidate's term (4) is higher than ours.
	reply := r.HandleRequestVote(&RequestVoteArgs{
		Term:         4,
		CandidateID:  "n2",
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	if reply.VoteGranted {
		t.Fatalf("expected vote to be denied for a candidate with a less up-to-date log")
	}
}

func TestElectionTimeoutTriggersCandidacy(t *testing.T) {
	// A single node with one permanently-unreachable peer: it can never
	// win an election (majority of 2 requires both), so it will remain
	// Candidate, repeatedly retrying with a fresh randomized timeout -
	// directly demonstrating that the election timeout drives it out of
	// Follower and into candidacy.
	r := New(Config{
		ID:        "n1",
		Peers:     []string{"ghost"}, // never registered in any transport, always errors
		Transport: &localTransport{reg: newLocalRegistry()},
		Storage:   NewMemoryStorage(),
	})
	r.Start()
	defer r.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if r.Status().Role == "candidate" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected node to become a candidate after its election timeout elapsed with no leader present, got role=%s", r.Status().Role)
}
