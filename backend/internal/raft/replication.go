package raft

import (
	"context"
	"time"
)

// HandleAppendEntries processes an incoming AppendEntries RPC (heartbeat
// or real log entries). Safe for concurrent use.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.alive {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	if args.Term < r.currentTerm {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	// A valid leader for a term >= ours: accept it and reset to Follower
	// (covers the Candidate-loses-election and stale-Leader cases too).
	r.becomeFollowerLocked(args.Term)
	r.leaderID = args.LeaderID
	r.lastHeartbeat = time.Now()

	reply := &AppendEntriesReply{Term: r.currentTerm}

	// Log matching property: reject unless we have an entry at
	// PrevLogIndex whose term matches PrevLogTerm.
	if args.PrevLogIndex > 0 {
		term, ok := r.entries.TermAt(args.PrevLogIndex)
		if !ok {
			reply.Success = false
			reply.ConflictIndex = r.entries.LastIndex() + 1
			reply.ConflictTerm = 0
			r.persistLocked()
			return reply
		}
		if term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = term
			reply.ConflictIndex = r.entries.FirstIndexOfTerm(term)
			r.persistLocked()
			return reply
		}
	}

	// Append new entries, truncating our log at the first point of
	// conflict (a term mismatch at the same index means everything from
	// there on is invalid and must be replaced).
	insertAt := args.PrevLogIndex + 1
	for i, e := range args.Entries {
		idx := insertAt + uint64(i)
		existingTerm, exists := r.entries.TermAt(idx)
		if exists {
			if existingTerm != e.Term {
				r.entries.TruncateFrom(idx)
				r.entries.AppendRaw(args.Entries[i:])
				break
			}
			continue // already present and matching; nothing to do
		}
		r.entries.AppendRaw(args.Entries[i:])
		break
	}

	if args.LeaderCommit > r.commitIndex {
		newCommit := args.LeaderCommit
		if lastNew := args.PrevLogIndex + uint64(len(args.Entries)); lastNew < newCommit {
			newCommit = lastNew
		}
		r.commitIndex = newCommit
		r.signalApplyLocked()
	}

	r.persistLocked()
	reply.Success = true
	return reply
}

// broadcastAppendEntries sends AppendEntries (heartbeat, or real entries if
// any peer is behind) to every peer in parallel. Called from the leader's
// tick loop at heartbeatInterval, and immediately after a client write is
// appended to the leader's own log.
func (r *Raft) broadcastAppendEntries() {
	r.mu.Lock()
	if r.role != Leader || !r.alive {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	peers := append([]string(nil), r.peers...)
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		go r.replicateToPeer(peer, term)
	}
}

// replicateToPeer sends one AppendEntries RPC to a single peer carrying
// whatever entries that peer still needs (per its nextIndex), and applies
// the result: on success, advance matchIndex/nextIndex and try to commit;
// on failure, back off nextIndex using the conflict hint.
func (r *Raft) replicateToPeer(peer string, term uint64) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term || !r.alive {
		r.mu.Unlock()
		return
	}
	next := r.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevLogIndex := next - 1
	prevLogTerm, _ := r.entries.TermAt(prevLogIndex)
	entries := r.entries.Slice(next)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval*4)
	defer cancel()
	reply, err := r.transport.SendAppendEntries(ctx, peer, args)
	if err != nil || reply == nil {
		return // peer unreachable/down; the next heartbeat tick retries
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		r.persistLocked()
		return
	}
	if r.role != Leader || r.currentTerm != term || !r.alive {
		return // stale reply from an old term/role
	}

	if reply.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > r.matchIndex[peer] {
			r.matchIndex[peer] = newMatch
		}
		if newMatch+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = newMatch + 1
		}
		r.advanceCommitIndexLocked()
		return
	}

	// Fast nextIndex backoff using the follower's conflict hint.
	if reply.ConflictTerm != 0 {
		if idx := r.entries.LastIndexOfTerm(reply.ConflictTerm); idx > 0 {
			r.nextIndex[peer] = idx + 1
		} else {
			r.nextIndex[peer] = reply.ConflictIndex
		}
	} else {
		r.nextIndex[peer] = reply.ConflictIndex
	}
	if r.nextIndex[peer] < 1 {
		r.nextIndex[peer] = 1
	}
}

// advanceCommitIndexLocked finds the highest index replicated on a
// majority of nodes (including the leader itself) whose entry belongs to
// the current term, and commits up to it. Per Raft's safety rule, a leader
// may only directly commit entries from its own term — earlier-term
// entries are committed indirectly once a later entry commits.
func (r *Raft) advanceCommitIndexLocked() {
	majority := (len(r.peers)+1)/2 + 1
	for n := r.entries.LastIndex(); n > r.commitIndex; n-- {
		term, ok := r.entries.TermAt(n)
		if !ok || term != r.currentTerm {
			continue
		}
		count := 1 // the leader itself has this entry
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= majority {
			r.commitIndex = n
			r.signalApplyLocked()
			return
		}
	}
}
