package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// localRegistry + localTransport wire multiple in-process Raft instances
// together without any real network, so unit tests are fast and
// deterministic (beyond the randomized election timers, which are
// intentionally real to exercise genuine timing-driven behavior).
type localRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*Raft
}

func newLocalRegistry() *localRegistry {
	return &localRegistry{nodes: make(map[string]*Raft)}
}

func (reg *localRegistry) register(r *Raft) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.nodes[r.ID()] = r
}

func (reg *localRegistry) get(id string) (*Raft, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.nodes[id]
	return r, ok
}

type localTransport struct {
	reg *localRegistry
}

var errPeerUnreachable = errors.New("peer unreachable")

func (t *localTransport) SendRequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	peer, ok := t.reg.get(peerID)
	if !ok || !peer.Alive() {
		return nil, errPeerUnreachable
	}
	return peer.HandleRequestVote(args), nil
}

func (t *localTransport) SendAppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	peer, ok := t.reg.get(peerID)
	if !ok || !peer.Alive() {
		return nil, errPeerUnreachable
	}
	return peer.HandleAppendEntries(args), nil
}

// testCluster is a small in-process Raft cluster for unit/integration tests.
type testCluster struct {
	reg   *localRegistry
	nodes []*Raft
}

func newTestCluster(n int) *testCluster {
	reg := newLocalRegistry()
	tc := &testCluster{reg: reg}
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		r := New(Config{
			ID:        ids[i],
			Peers:     peers,
			Transport: &localTransport{reg: reg},
			Storage:   NewMemoryStorage(),
		})
		reg.register(r)
		tc.nodes = append(tc.nodes, r)
	}
	return tc
}

func (tc *testCluster) startAll() {
	for _, n := range tc.nodes {
		n.Start()
	}
}

func (tc *testCluster) stopAll() {
	for _, n := range tc.nodes {
		n.Stop()
	}
}

func (tc *testCluster) leaders() []*Raft {
	var out []*Raft
	for _, n := range tc.nodes {
		if n.IsLeader() {
			out = append(out, n)
		}
	}
	return out
}

// waitForSingleLeader polls until exactly one node reports itself leader
// and every other alive node agrees (same term, same leader ID), or the
// timeout elapses.
func (tc *testCluster) waitForSingleLeader(timeout time.Duration) (*Raft, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := tc.leaders()
		if len(leaders) == 1 {
			leader := leaders[0]
			term := leader.Status().Term
			agree := true
			for _, n := range tc.nodes {
				if !n.Alive() || n == leader {
					continue
				}
				st := n.Status()
				if st.Term == term && st.LeaderID != leader.ID() {
					agree = false
					break
				}
			}
			if agree {
				return leader, nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, fmt.Errorf("no single leader agreed upon within %s (leaders=%d)", timeout, len(tc.leaders()))
}

// noopTransport is used by tests that construct a lone Raft instance and
// drive its RPC handlers directly, never exercising the tick loop's
// outbound calls.
type noopTransport struct{}

func (noopTransport) SendRequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errors.New("noopTransport: unexpected outbound RequestVote")
}

func (noopTransport) SendAppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, errors.New("noopTransport: unexpected outbound AppendEntries")
}
