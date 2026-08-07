// Package gateway exposes each node's REST API (and, later, WebSocket
// stream) to clients and the frontend dashboard.
package gateway

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

// Server is one node's HTTP gateway.
type Server struct {
	node   *raft.Raft
	logger *log.Logger
	mux    *http.ServeMux
}

func NewServer(node *raft.Raft, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{node: node, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.withCORS(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /cluster/status", s.handleStatus)
	s.mux.HandleFunc("GET /cluster/log", s.handleLog)
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
