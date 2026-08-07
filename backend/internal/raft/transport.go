package raft

import "context"

// RequestVoteArgs/Reply and AppendEntriesArgs/Reply mirror the gRPC messages
// defined in internal/rpc/raft.proto, but as plain Go structs so the core
// Raft state machine has zero dependency on the generated protobuf code.
// This keeps unit tests fast and deterministic (an in-memory Transport can
// connect several Raft instances in one process, no sockets involved) while
// production wiring (internal/rpc/server.go) adapts real gRPC calls to this
// same interface.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport sends outbound RPCs to a named peer. Implementations must be
// safe for concurrent use and should apply their own timeout: a slow or
// dead peer must never block the caller beyond that timeout.
type Transport interface {
	SendRequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
