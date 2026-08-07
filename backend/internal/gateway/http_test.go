package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jamesmartin6/quorum/backend/internal/kv"
	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

// noopTransport lets tests construct a Raft node that never actually needs
// to talk to peers (single-node "leader by majority-of-one" cluster, or a
// follower whose state we drive directly via exported RPC handlers).
type noopTransport struct{}

func (noopTransport) SendRequestVote(ctx context.Context, peerID string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, errors.New("noopTransport: unexpected outbound call")
}

func (noopTransport) SendAppendEntries(ctx context.Context, peerID string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, errors.New("noopTransport: unexpected outbound call")
}

// newLeaderNode returns a single-node cluster (so it becomes its own
// leader by majority-of-one) wired to a live KV store.
func newLeaderNode(t *testing.T) (*raft.Raft, *kv.Store) {
	t.Helper()
	node := raft.New(raft.Config{ID: "n1", Transport: noopTransport{}, Storage: raft.NewMemoryStorage()})
	node.Start()
	t.Cleanup(node.Stop)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && !node.IsLeader() {
		time.Sleep(5 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatalf("expected a single-node cluster to become its own leader")
	}

	store := kv.NewStore()
	go store.Run(node.ApplyChan())
	return node, store
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestGateway_SetGetDeleteRoundTrip(t *testing.T) {
	node, store := newLeaderNode(t)
	srv := NewServer(node, store, nil, nil)
	h := srv.Handler()

	rec := doJSON(t, h, http.MethodPost, "/kv/foo", setRequest{Value: "bar"})
	if rec.Code != http.StatusOK {
		t.Fatalf("SET: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var setResp writeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &setResp); err != nil {
		t.Fatalf("decode SET response: %v", err)
	}
	if !setResp.Committed {
		t.Fatalf("expected SET to report committed=true, got %+v", setResp)
	}

	rec = doJSON(t, h, http.MethodGet, "/kv/foo", nil)
	var getResp getResponse
	json.Unmarshal(rec.Body.Bytes(), &getResp)
	if !getResp.Found || getResp.Value != "bar" {
		t.Fatalf("GET: expected foo=bar found=true, got %+v", getResp)
	}

	rec = doJSON(t, h, http.MethodDelete, "/kv/foo", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE: expected 200, got %d", rec.Code)
	}

	rec = doJSON(t, h, http.MethodGet, "/kv/foo", nil)
	json.Unmarshal(rec.Body.Bytes(), &getResp)
	if getResp.Found {
		t.Fatalf("expected foo to be gone after DELETE, got %+v", getResp)
	}
}

func TestGateway_SetRejectsMalformedBody(t *testing.T) {
	node, store := newLeaderNode(t)
	srv := NewServer(node, store, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/kv/foo", bytes.NewReader([]byte("{not json")))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON body, got %d", rec.Code)
	}
}

func TestGateway_NonLeaderRejectsWritesWithLeaderRedirect(t *testing.T) {
	node := raft.New(raft.Config{ID: "follower1", Peers: []string{"leader1"}, Transport: noopTransport{}, Storage: raft.NewMemoryStorage()})
	// Drive the node into Follower-with-known-leader state via the real
	// RPC handler, rather than reaching into unexported fields.
	node.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "leader1"})

	store := kv.NewStore()
	peerHTTP := map[string]string{"leader1": "localhost:8080"}
	srv := NewServer(node, store, peerHTTP, nil)

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/kv/foo", setRequest{Value: "bar"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp notLeaderResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.LeaderID != "leader1" || resp.LeaderHTTPAddr != "localhost:8080" {
		t.Fatalf("expected redirect pointing at leader1/localhost:8080, got %+v", resp)
	}
}

func TestGateway_NonLeaderRejectsReads(t *testing.T) {
	node := raft.New(raft.Config{ID: "follower1", Peers: []string{"leader1"}, Transport: noopTransport{}, Storage: raft.NewMemoryStorage()})
	node.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "leader1"})
	store := kv.NewStore()
	srv := NewServer(node, store, map[string]string{"leader1": "localhost:8080"}, nil)

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/kv/foo", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected reads on a non-leader to be rejected with 503, got %d", rec.Code)
	}
}

func TestGateway_ClusterStatus(t *testing.T) {
	node, store := newLeaderNode(t)
	srv := NewServer(node, store, nil, nil)

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/cluster/status", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var status raft.StatusSnapshot
	json.Unmarshal(rec.Body.Bytes(), &status)
	if status.Role != "leader" || status.ID != "n1" {
		t.Fatalf("unexpected status snapshot: %+v", status)
	}
}

func TestGateway_ChaosKillAndRevive(t *testing.T) {
	node, store := newLeaderNode(t)
	srv := NewServer(node, store, nil, nil)

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/chaos/kill", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /chaos/kill, got %d", rec.Code)
	}
	if node.Alive() {
		t.Fatalf("expected node to be marked not-alive after /chaos/kill")
	}

	rec = doJSON(t, srv.Handler(), http.MethodPost, "/chaos/revive", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /chaos/revive, got %d", rec.Code)
	}
	if !node.Alive() {
		t.Fatalf("expected node to be marked alive after /chaos/revive")
	}
}
