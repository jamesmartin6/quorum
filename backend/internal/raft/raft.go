// Package raft implements a Raft consensus state machine (leader election
// and, from Phase 2 onward, log replication) independent of any transport
// or storage technology. See https://raft.github.io/raft.pdf.
package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	// electionTimeoutMin/Max bound the randomized follower/candidate
	// election timeout, per the Raft paper's recommendation.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond

	// heartbeatInterval is how often a leader sends AppendEntries to
	// keep its peers from timing out. Must be well under electionTimeoutMin.
	heartbeatInterval = 50 * time.Millisecond

	tickInterval = 10 * time.Millisecond
)

// Config carries everything a Raft instance needs at construction time.
type Config struct {
	ID        string
	Peers     []string // IDs of all OTHER nodes in the cluster (not self)
	Transport Transport
	Storage   Storage

	// Logger is optional; if nil, a default stdlib logger is used.
	Logger *log.Logger
}

// StatusSnapshot is a point-in-time, lock-free copy of a node's state,
// safe to read from any goroutine (used by the HTTP/WS gateway).
type StatusSnapshot struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leaderId"`
	VotedFor    string `json:"votedFor"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
	LogLength   int    `json:"logLength"`
	Alive       bool   `json:"alive"`
}

// ApplyMsg is delivered on the Raft instance's Apply channel once a log
// entry has been committed by a majority and is safe for the state machine
// (the KV store, in Phase 3) to apply.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	Entry LogEntry
}

// Raft is one node's consensus state machine.
type Raft struct {
	mu sync.Mutex

	id        string
	peers     []string
	transport Transport
	storage   Storage
	logger    *log.Logger

	// Persistent state (must be written to storage before responding to
	// any RPC that changes it - see persistLocked).
	currentTerm uint64
	votedFor    string
	entries     *Log

	// Volatile state, all nodes.
	role        Role
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// Volatile state, leaders only (reset on election).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// alive is false while this node is "killed" by the chaos API: it stops
	// participating in the protocol (no RPCs sent or accepted) without
	// tearing down the process, so it can be "revived" later.
	alive bool

	lastHeartbeat time.Time // last time we heard from a valid leader, or granted a vote, or (re)started an election
	rng           *rand.Rand

	applyCh     chan ApplyMsg
	applySignal chan struct{}
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// New constructs a Raft instance. Call Start to begin the timer loop.
func New(cfg Config) *Raft {
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	st, err := cfg.Storage.Load()
	if err != nil {
		logger.Printf("[%s] failed to load persisted state, starting fresh: %v", cfg.ID, err)
	}

	r := &Raft{
		id:          cfg.ID,
		peers:       cfg.Peers,
		transport:   cfg.Transport,
		storage:     cfg.Storage,
		logger:      logger,
		currentTerm: st.CurrentTerm,
		votedFor:    st.VotedFor,
		entries:     NewLog(st.Log),
		role:        Follower,
		alive:       true,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(hashString(cfg.ID)))),
		applyCh:     make(chan ApplyMsg, 256),
		applySignal: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	r.lastHeartbeat = time.Now()
	return r
}

// hashString gives each node a distinct RNG seed component so nodes started
// in the same millisecond don't all pick identical "random" timeouts.
func hashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ApplyChan returns the channel on which committed entries are delivered,
// in order, exactly once each.
func (r *Raft) ApplyChan() <-chan ApplyMsg {
	return r.applyCh
}

// Start begins the background tick loop (election timeouts + leader
// heartbeats/replication) and the apply-loop that delivers committed
// entries to ApplyChan.
func (r *Raft) Start() {
	r.wg.Add(2)
	go r.runLoop()
	go r.applyLoop()
}

// signalApplyLocked wakes the apply loop. Non-blocking; caller holds r.mu.
func (r *Raft) signalApplyLocked() {
	select {
	case r.applySignal <- struct{}{}:
	default:
	}
}

// applyLoop delivers newly committed entries to applyCh in order, exactly
// once each. It runs independently of r.mu so a slow consumer of
// ApplyChan can never block the Raft protocol's critical section.
func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.applySignal:
			r.mu.Lock()
			var toApply []LogEntry
			for r.lastApplied < r.commitIndex {
				r.lastApplied++
				if e, ok := r.entries.Get(r.lastApplied); ok {
					toApply = append(toApply, e)
				}
			}
			r.mu.Unlock()
			for _, e := range toApply {
				select {
				case r.applyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Entry: e}:
				case <-r.stopCh:
					return
				}
			}
		}
	}
}

// Stop halts the background loop permanently. The Raft instance cannot be
// restarted after this; construct a new one instead.
func (r *Raft) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *Raft) runLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

func (r *Raft) tick() {
	r.mu.Lock()
	if !r.alive {
		r.mu.Unlock()
		return
	}
	role := r.role
	switch role {
	case Leader:
		due := time.Since(r.lastHeartbeat) >= heartbeatInterval
		if due {
			r.lastHeartbeat = time.Now()
		}
		r.mu.Unlock()
		if due {
			r.broadcastAppendEntries()
		}
	default:
		timeout := r.randomElectionTimeoutLocked()
		elapsed := time.Since(r.lastHeartbeat)
		r.mu.Unlock()
		if elapsed >= timeout {
			r.startElection()
		}
	}
}

func (r *Raft) randomElectionTimeoutLocked() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(r.rng.Int63n(int64(span)))
}

// persistLocked writes currentTerm/votedFor/log to storage. Caller must
// hold r.mu. Per the Raft paper, this must complete before the node
// responds to the RPC that triggered the change.
func (r *Raft) persistLocked() {
	if err := r.storage.Save(PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.entries.All(),
	}); err != nil {
		r.logger.Printf("[%s] persist failed: %v", r.id, err)
	}
}

// becomeFollowerLocked steps down to Follower at the given term. Caller
// holds r.mu.
func (r *Raft) becomeFollowerLocked(term uint64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}
	if r.role != Follower {
		r.logger.Printf("[%s] %s -> follower (term %d)", r.id, r.role, term)
	}
	r.role = Follower
	r.nextIndex = nil
	r.matchIndex = nil
}

// Status returns a snapshot safe for concurrent read by HTTP handlers.
func (r *Raft) Status() StatusSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return StatusSnapshot{
		ID:          r.id,
		Role:        r.role.String(),
		Term:        r.currentTerm,
		LeaderID:    r.leaderID,
		VotedFor:    r.votedFor,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		LogLength:   r.entries.Len(),
		Alive:       r.alive,
	}
}

// LogEntries returns a copy of the full log, for debugging/visualization.
func (r *Raft) LogEntries() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries.All()
}

// ID returns this node's ID.
func (r *Raft) ID() string { return r.id }

// IsLeader reports whether this node currently believes itself to be leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader && r.alive
}

// LeaderID returns the last known leader ID (may be stale/empty).
func (r *Raft) LeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// Kill stops this node from participating in the Raft protocol (chaos
// testing): it no longer sends or accepts RPCs, and its election/heartbeat
// timers are suspended. The process keeps running.
func (r *Raft) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alive = false
	r.logger.Printf("[%s] killed (chaos)", r.id)
}

// Revive resumes protocol participation after Kill, rejoining as a
// Follower so it must catch up via normal AppendEntries/log-matching.
func (r *Raft) Revive() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alive = true
	r.role = Follower
	r.leaderID = ""
	r.lastHeartbeat = time.Now()
	r.logger.Printf("[%s] revived (chaos)", r.id)
}

// Alive reports whether this node is currently participating (see Kill).
func (r *Raft) Alive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.alive
}

// Propose appends a new command to the log if (and only if) this node is
// currently the leader, then kicks off replication to followers
// immediately (rather than waiting for the next heartbeat tick). The
// returned index/term identify the entry; callers must wait for it to be
// committed (poll Status().CommitIndex, or watch ApplyChan) before
// treating it as durable — appending alone is not commitment.
func (r *Raft) Propose(op, key, value string) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	if r.role != Leader || !r.alive {
		r.mu.Unlock()
		return 0, 0, false
	}
	term = r.currentTerm
	index = r.entries.Append(term, LogEntry{Op: op, Key: key, Value: value})
	r.persistLocked()
	// Handles the single-node (or already-caught-up-peers) case, where no
	// peer reply will ever arrive to trigger advanceCommitIndexLocked.
	r.advanceCommitIndexLocked()
	r.mu.Unlock()

	r.broadcastAppendEntries()
	return index, term, true
}
