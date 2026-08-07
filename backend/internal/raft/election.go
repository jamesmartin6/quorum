package raft

import (
	"context"
	"time"
)

// HandleRequestVote processes an incoming RequestVote RPC. Safe for
// concurrent use; called directly by an in-memory Transport in tests, or
// from the gRPC server adapter in production.
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.alive {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
		r.persistLocked()
	}

	reply := &RequestVoteReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	myLastIndex := r.entries.LastIndex()
	myLastTerm := r.entries.LastTerm()
	candidateUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	canVote := r.votedFor == "" || r.votedFor == args.CandidateID
	if canVote && candidateUpToDate {
		r.votedFor = args.CandidateID
		r.persistLocked()
		r.lastHeartbeat = time.Now() // granting a vote counts as "heard from the cluster"
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return reply
}

// startElection converts this node to Candidate and requests votes from
// all peers in parallel. Called from the tick loop when the election
// timeout elapses with no valid leader heartbeat.
func (r *Raft) startElection() {
	r.mu.Lock()
	if !r.alive {
		r.mu.Unlock()
		return
	}
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
	r.persistLocked()
	r.lastHeartbeat = time.Now()
	term := r.currentTerm
	lastLogIndex := r.entries.LastIndex()
	lastLogTerm := r.entries.LastTerm()
	peers := append([]string(nil), r.peers...)
	r.logger.Printf("[%s] starting election for term %d", r.id, term)
	r.mu.Unlock()

	votes := 1 // vote for self
	majority := len(peers)/2 + 1 // +1 self, so total cluster = len(peers)+1
	// len(peers)+1 total nodes; majority of (len(peers)+1) is (len(peers)+1)/2 + 1.
	majority = (len(peers)+1)/2 + 1

	if votes >= majority {
		r.becomeLeader(term)
		return
	}

	type voteResult struct {
		reply *RequestVoteReply
		err   error
	}
	results := make(chan voteResult, len(peers))
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), electionTimeoutMin/2)
			defer cancel()
			reply, err := r.transport.SendRequestVote(ctx, peer, args)
			results <- voteResult{reply, err}
		}()
	}

	granted := votes
	for i := 0; i < len(peers); i++ {
		res := <-results
		if res.err != nil || res.reply == nil {
			continue
		}
		r.mu.Lock()
		if res.reply.Term > r.currentTerm {
			r.becomeFollowerLocked(res.reply.Term)
			r.persistLocked()
			r.mu.Unlock()
			return
		}
		stillCandidate := r.role == Candidate && r.currentTerm == term
		r.mu.Unlock()
		if !stillCandidate {
			return // election superseded (stepped down, or already won/lost)
		}
		if res.reply.VoteGranted {
			granted++
			if granted >= majority {
				r.becomeLeader(term)
				return
			}
		}
	}
	// Not enough votes this round (split vote or peers unreachable). The
	// tick loop will retry with a fresh randomized timeout once the
	// election timer next elapses.
}

// becomeLeader transitions to Leader for the given term, if still valid.
func (r *Raft) becomeLeader(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm != term || r.role != Candidate || !r.alive {
		return // stale: superseded by a higher term or another winner while votes were in flight
	}
	r.role = Leader
	r.leaderID = r.id
	r.nextIndex = make(map[string]uint64, len(r.peers))
	r.matchIndex = make(map[string]uint64, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p] = r.entries.LastIndex() + 1
		r.matchIndex[p] = 0
	}
	// Append a no-op entry in the new term and persist it immediately. A
	// leader may only directly commit entries from its OWN term (the
	// Raft "Figure 8" safety rule) - without this, a freshly elected
	// leader could sit with commitIndex stuck at 0 (even though a
	// majority already safely holds every prior entry) until a client
	// happens to write something new. This no-op commits as soon as a
	// majority acks it, which transitively commits everything before it
	// too, so the cluster regains full read/write availability right
	// after an election instead of waiting on the next client write.
	r.entries.Append(term, LogEntry{Op: "NOOP"})
	r.persistLocked()
	r.advanceCommitIndexLocked() // handles the single-node-cluster case, where no peer reply will ever trigger this
	// Force an immediate heartbeat on the next tick instead of waiting a
	// full heartbeatInterval, so followers learn about the new leader fast.
	r.lastHeartbeat = time.Time{}
	r.logger.Printf("[%s] elected leader for term %d", r.id, term)
}
