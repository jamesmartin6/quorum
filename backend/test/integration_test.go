// Package integration exercises the full stack over REAL gRPC connections
// on loopback sockets (as opposed to internal/raft's unit tests, which use
// an in-memory Transport). Each node here is a genuine goroutine with its
// own gRPC server and TCP listener, wired together exactly as the actual
// node binary wires them - this is the closest thing to a real multi-node
// cluster short of separate OS processes / Docker containers.
package integration

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jamesmartin6/quorum/backend/internal/raft"
	"github.com/jamesmartin6/quorum/backend/internal/rpc"
	"google.golang.org/grpc"
)

type testNode struct {
	id      string
	addr    string
	dataDir string
	peers   map[string]string // peer ID -> peer gRPC addr, excluding self

	node       *raft.Raft
	grpcServer *grpc.Server
	stopped    bool
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startNode(t *testing.T, id, addr, dataDir string, peers map[string]string) *testNode {
	t.Helper()
	storage, err := raft.NewFileStorage(dataDir)
	if err != nil {
		t.Fatalf("[%s] storage: %v", id, err)
	}
	peerIDs := make([]string, 0, len(peers))
	for pid := range peers {
		peerIDs = append(peerIDs, pid)
	}
	transport := rpc.NewGRPCTransport(peers)
	node := raft.New(raft.Config{ID: id, Peers: peerIDs, Transport: transport, Storage: storage})
	node.Start()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("[%s] listen %s: %v", id, addr, err)
	}
	grpcServer := rpc.NewGRPCServer(node)
	go grpcServer.Serve(lis) //nolint:errcheck // server.Stop() during teardown causes an expected "use of closed network connection"

	return &testNode{id: id, addr: addr, dataDir: dataDir, peers: peers, node: node, grpcServer: grpcServer}
}

// stop tears the node down completely (both its Raft timers and its
// listening socket) without deleting its on-disk state - simulating a
// process crash/kill. Idempotent, since a test may explicitly stop() a
// node and later restart() (which itself stops first) it.
func (n *testNode) stop() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.grpcServer.Stop()
	n.node.Stop()
}

// restart simulates the node process being killed (if not already) and
// started again: a brand new Raft instance is built from whatever was
// persisted to n.dataDir, and rebinds to the same address.
func (n *testNode) restart(t *testing.T) {
	t.Helper()
	n.stop()
	fresh := startNode(t, n.id, n.addr, n.dataDir, n.peers)
	n.node = fresh.node
	n.grpcServer = fresh.grpcServer
	n.stopped = false
}

// cluster is a set of testNodes fully connected to each other over real gRPC.
type cluster struct {
	nodes map[string]*testNode
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	ids := make([]string, n)
	addrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i)
		addrs[ids[i]] = freeLoopbackAddr(t)
	}

	c := &cluster{nodes: make(map[string]*testNode, n)}
	for i := 0; i < n; i++ {
		id := ids[i]
		peers := make(map[string]string, n-1)
		for j := 0; j < n; j++ {
			if ids[j] != id {
				peers[ids[j]] = addrs[ids[j]]
			}
		}
		c.nodes[id] = startNode(t, id, addrs[id], t.TempDir(), peers)
	}
	return c
}

func (c *cluster) stopAll() {
	for _, n := range c.nodes {
		n.stop()
	}
}

func (c *cluster) leader() *testNode {
	for _, n := range c.nodes {
		if n.node.IsLeader() {
			return n
		}
	}
	return nil
}

// waitForLeader polls until exactly one node believes it's leader.
func (c *cluster) waitForLeader(t *testing.T, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*testNode
		for _, n := range c.nodes {
			if n.node.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader emerged within %s", timeout)
	return nil
}

// waitForCommit polls a node until its commitIndex reaches at least index.
func waitForCommit(t *testing.T, n *testNode, index uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.node.Status().CommitIndex >= index {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("[%s] commitIndex did not reach %d within %s (got %d)", n.id, index, timeout, n.node.Status().CommitIndex)
}

// waitForLogLen polls a node until its log has at least n entries.
func waitForLogLen(t *testing.T, n *testNode, length int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(n.node.LogEntries()) >= length {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("[%s] log did not reach length %d within %s (got %d)", n.id, length, timeout, len(n.node.LogEntries()))
}

func TestCluster_ElectsLeaderAndReplicatesOverRealGRPC(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()

	leader := c.waitForLeader(t, 3*time.Second)

	idx, _, ok := leader.node.Propose("SET", "foo", "bar")
	if !ok {
		t.Fatalf("expected Propose to succeed on the leader")
	}
	waitForCommit(t, leader, idx, 2*time.Second)

	for id, n := range c.nodes {
		waitForLogLen(t, n, int(idx), 2*time.Second)
		entries := n.node.LogEntries()
		got := entries[idx-1]
		if got.Key != "foo" || got.Value != "bar" || got.Op != "SET" {
			t.Fatalf("[%s] expected replicated entry {SET foo bar}, got %+v", id, got)
		}
	}
}

func TestCluster_KilledFollowerRestartsAndConverges(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()

	leader := c.waitForLeader(t, 3*time.Second)

	idx1, _, ok := leader.node.Propose("SET", "a", "1")
	if !ok {
		t.Fatalf("propose 1 failed")
	}
	waitForCommit(t, leader, idx1, 2*time.Second)

	// Pick a follower and kill it (full process-restart simulation, not
	// just the soft chaos-Kill) while it's briefly behind.
	var follower *testNode
	for _, n := range c.nodes {
		if n != leader {
			follower = n
			break
		}
	}
	follower.stop()

	// Write more entries while the follower is down - it must miss these
	// and catch up later purely through normal AppendEntries replication.
	idx2, _, ok := leader.node.Propose("SET", "b", "2")
	if !ok {
		t.Fatalf("propose 2 failed")
	}
	idx3, _, ok := leader.node.Propose("DELETE", "a", "")
	if !ok {
		t.Fatalf("propose 3 failed")
	}
	waitForCommit(t, leader, idx3, 2*time.Second)

	// Bring the follower back - it should rejoin, and the leader's log
	// matching / nextIndex backoff logic should bring it fully up to date.
	follower.restart(t)

	waitForLogLen(t, follower, int(idx3), 3*time.Second)
	waitForCommit(t, follower, idx3, 3*time.Second)

	leaderEntries := leader.node.LogEntries()
	followerEntries := follower.node.LogEntries()
	if len(followerEntries) != len(leaderEntries) {
		t.Fatalf("expected follower log to converge with leader: leader has %d entries, follower has %d", len(leaderEntries), len(followerEntries))
	}
	for i := range leaderEntries {
		if leaderEntries[i] != followerEntries[i] {
			t.Fatalf("log divergence at position %d: leader=%+v follower=%+v", i, leaderEntries[i], followerEntries[i])
		}
	}
	_ = idx2
}

// TestCluster_KillLeaderMidWrite_NoDataLossNewLeaderElected is the core
// correctness guarantee of Raft, called out explicitly in the build plan:
// killing the leader after it has committed a write must not lose that
// write, and the cluster must recover with a newly elected leader that has
// the committed data and can keep serving new writes.
func TestCluster_KillLeaderMidWrite_NoDataLossNewLeaderElected(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stopAll()

	firstLeader := c.waitForLeader(t, 3*time.Second)

	idx, term, ok := firstLeader.node.Propose("SET", "critical", "committed-before-crash")
	if !ok {
		t.Fatalf("expected Propose to succeed on the leader")
	}
	waitForCommit(t, firstLeader, idx, 2*time.Second)

	// Make sure a majority actually has it durably before we pull the rug -
	// otherwise this wouldn't be testing what it claims to.
	haveIt := 0
	for _, n := range c.nodes {
		entries := n.node.LogEntries()
		if len(entries) >= int(idx) && entries[idx-1].Key == "critical" {
			haveIt++
		}
	}
	if haveIt < 3 { // majority of 5
		t.Fatalf("expected a majority (>=3) of nodes to already have the entry before killing the leader, got %d", haveIt)
	}

	firstLeaderID := firstLeader.id
	firstLeader.stop() // the leader process dies mid-write, mid-cluster

	// A new leader must be elected among the 4 survivors.
	deadline := time.Now().Add(4 * time.Second)
	var newLeader *testNode
	for time.Now().Before(deadline) {
		var leaders []*testNode
		for id, n := range c.nodes {
			if id == firstLeaderID {
				continue
			}
			if n.node.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			newLeader = leaders[0]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatalf("no new leader was elected among survivors within the timeout")
	}
	if newLeader.node.Status().Term <= term {
		t.Fatalf("expected new leader's term (%d) to exceed the old leader's term (%d)", newLeader.node.Status().Term, term)
	}

	// No data loss: the committed entry must still be there and still committed.
	waitForCommit(t, newLeader, idx, 2*time.Second)
	entries := newLeader.node.LogEntries()
	if len(entries) < int(idx) || entries[idx-1].Key != "critical" || entries[idx-1].Value != "committed-before-crash" {
		t.Fatalf("data loss: new leader's log is missing the entry committed before the old leader crashed: %+v", entries)
	}

	// The cluster must still be able to serve new writes.
	idx2, _, ok := newLeader.node.Propose("SET", "after-recovery", "yes")
	if !ok {
		t.Fatalf("expected new leader to accept new writes")
	}
	waitForCommit(t, newLeader, idx2, 2*time.Second)
}
