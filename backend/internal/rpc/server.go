// Package rpc adapts the transport-agnostic core Raft state machine
// (internal/raft) onto real gRPC: Server implements the generated
// RaftServer interface by delegating to a *raft.Raft, and GRPCTransport
// implements raft.Transport by dialing peer addresses over gRPC.
package rpc

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

// Server implements RaftServer by forwarding to a core raft.Raft instance.
type Server struct {
	UnimplementedRaftServer
	node *raft.Raft
}

func NewServer(node *raft.Raft) *Server {
	return &Server{node: node}
}

func (s *Server) RequestVote(ctx context.Context, in *RequestVoteRequest) (*RequestVoteResponse, error) {
	reply := s.node.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         in.GetTerm(),
		CandidateID:  in.GetCandidateId(),
		LastLogIndex: in.GetLastLogIndex(),
		LastLogTerm:  in.GetLastLogTerm(),
	})
	return &RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(in.GetEntries()))
	for i, e := range in.GetEntries() {
		entries[i] = raft.LogEntry{
			Index: e.GetIndex(),
			Term:  e.GetTerm(),
			Op:    e.GetOp(),
			Key:   e.GetKey(),
			Value: e.GetValue(),
		}
	}
	reply := s.node.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         in.GetTerm(),
		LeaderID:     in.GetLeaderId(),
		PrevLogIndex: in.GetPrevLogIndex(),
		PrevLogTerm:  in.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: in.GetLeaderCommit(),
	})
	return &AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

// GRPCTransport implements raft.Transport over real gRPC connections,
// dialing peers lazily and caching connections/clients by peer ID.
type GRPCTransport struct {
	mu        sync.Mutex
	addresses map[string]string // peer ID -> "host:port"
	clients   map[string]RaftClient
}

func NewGRPCTransport(addresses map[string]string) *GRPCTransport {
	return &GRPCTransport{
		addresses: addresses,
		clients:   make(map[string]RaftClient),
	}
}

func (t *GRPCTransport) client(peerID string) (RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[peerID]; ok {
		return c, nil
	}
	addr, ok := t.addresses[peerID]
	if !ok {
		return nil, fmt.Errorf("rpc: unknown peer %q", peerID)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("rpc: dial %s (%s): %w", peerID, addr, err)
	}
	c := NewRaftClient(conn)
	t.clients[peerID] = c
	return c, nil
}

func (t *GRPCTransport) SendRequestVote(ctx context.Context, peerID string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.GetTerm(), VoteGranted: resp.GetVoteGranted()}, nil
}

func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peerID string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	entries := make([]*LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Op:    e.Op,
			Key:   e.Key,
			Value: e.Value,
		}
	}
	resp, err := c.AppendEntries(ctx, &AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.GetTerm(),
		Success:       resp.GetSuccess(),
		ConflictIndex: resp.GetConflictIndex(),
		ConflictTerm:  resp.GetConflictTerm(),
	}, nil
}

// NewGRPCServer registers a Raft node onto a fresh *grpc.Server.
func NewGRPCServer(node *raft.Raft) *grpc.Server {
	s := grpc.NewServer()
	RegisterRaftServer(s, NewServer(node))
	return s
}
