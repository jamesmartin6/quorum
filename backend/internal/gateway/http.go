// Package gateway exposes each node's REST API (and chaos-testing hooks)
// to clients and the frontend dashboard.
package gateway

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jamesmartin6/quorum/backend/internal/kv"
	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

// commitWaitTimeout bounds how long a write request blocks waiting for its
// entry to commit before responding. Well above normal commit latency
// (a couple of heartbeat/replication round trips) but still snappy.
const commitWaitTimeout = 3 * time.Second

// Server is one node's HTTP gateway.
type Server struct {
	node     *raft.Raft
	store    *kv.Store
	peerHTTP map[string]string // peer ID -> HTTP "host:port", used to build leader-redirect responses
	logger   *log.Logger
	mux      *http.ServeMux
}

func NewServer(node *raft.Raft, store *kv.Store, peerHTTP map[string]string, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{node: node, store: store, peerHTTP: peerHTTP, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.withCORS(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /cluster/status", s.handleStatus)
	s.mux.HandleFunc("GET /cluster/log", s.handleLog)
	s.mux.HandleFunc("POST /kv/{key}", s.handleSet)
	s.mux.HandleFunc("GET /kv/{key}", s.handleGet)
	s.mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	s.mux.HandleFunc("POST /chaos/kill", s.handleChaosKill)
	s.mux.HandleFunc("POST /chaos/revive", s.handleChaosRevive)
}

// withCORS allows the frontend dev server (a different origin) to poll
// every node directly from the browser.
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Printf("gateway: failed writing JSON response: %v", err)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.node.Status())
}

func (s *Server) handleLog(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"nodeId":  s.node.ID(),
		"entries": s.node.LogEntries(),
	})
}

// notLeaderResponse is returned (503) by any write, or any read, made
// against a node that isn't currently the leader. It carries enough for a
// client to follow the redirect itself, per the spec: "simplifies frontend
// logic - no need for smart client-side leader tracking beyond following
// redirects".
type notLeaderResponse struct {
	Error          string `json:"error"`
	LeaderID       string `json:"leaderId,omitempty"`
	LeaderHTTPAddr string `json:"leaderHttpAddr,omitempty"`
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	leaderID := s.node.LeaderID()
	resp := notLeaderResponse{Error: "not the leader"}
	if leaderID != "" {
		resp.LeaderID = leaderID
		if addr, ok := s.peerHTTP[leaderID]; ok {
			resp.LeaderHTTPAddr = addr
		}
	} else {
		resp.Error = "not the leader, and no leader is currently known (election in progress?)"
	}
	s.writeJSON(w, http.StatusServiceUnavailable, resp)
}

type setRequest struct {
	Value string `json:"value"`
}

type writeResponse struct {
	OK        bool   `json:"ok"`
	Key       string `json:"key"`
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	HandledBy string `json:"handledBy"`
	Committed bool   `json:"committed"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body setRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body, expected {\"value\": \"...\"}"})
		return
	}

	idx, term, ok := s.node.Propose("SET", key, body.Value)
	if !ok {
		s.writeNotLeader(w)
		return
	}
	committed := s.waitForCommit(idx)
	s.writeJSON(w, http.StatusOK, writeResponse{OK: true, Key: key, Index: idx, Term: term, HandledBy: s.node.ID(), Committed: committed})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	idx, term, ok := s.node.Propose("DELETE", key, "")
	if !ok {
		s.writeNotLeader(w)
		return
	}
	committed := s.waitForCommit(idx)
	s.writeJSON(w, http.StatusOK, writeResponse{OK: true, Key: key, Index: idx, Term: term, HandledBy: s.node.ID(), Committed: committed})
}

type getResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Found     bool   `json:"found"`
	HandledBy string `json:"handledBy"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	// Reads are served by the leader only, for linearizability: a
	// follower's applied state can lag the committed log by a heartbeat
	// or two, so it could return a stale value.
	if !s.node.IsLeader() {
		s.writeNotLeader(w)
		return
	}
	key := r.PathValue("key")
	value, found := s.store.Get(key)
	s.writeJSON(w, http.StatusOK, getResponse{Key: key, Value: value, Found: found, HandledBy: s.node.ID()})
}

// waitForCommit polls until the given log index has been committed AND
// applied to the local KV store, or commitWaitTimeout elapses. Waiting for
// application (not just commitment) matters: commitIndex advances the
// instant a majority acks the entry, but the KV map is only updated
// asynchronously afterward via the apply loop - without this, a client
// could SET a key and immediately GET it back as missing. Returns false
// (not an error) on timeout; the write is still very likely to land
// shortly after, the caller just stops waiting for confirmation.
func (s *Server) waitForCommit(index uint64) bool {
	deadline := time.Now().Add(commitWaitTimeout)
	for time.Now().Before(deadline) {
		if s.node.Status().CommitIndex >= index && s.store.LastApplied() >= index {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func (s *Server) handleChaosKill(w http.ResponseWriter, r *http.Request) {
	s.node.Kill()
	s.writeJSON(w, http.StatusOK, map[string]any{"nodeId": s.node.ID(), "alive": s.node.Alive()})
}

func (s *Server) handleChaosRevive(w http.ResponseWriter, r *http.Request) {
	s.node.Revive()
	s.writeJSON(w, http.StatusOK, map[string]any{"nodeId": s.node.ID(), "alive": s.node.Alive()})
}
