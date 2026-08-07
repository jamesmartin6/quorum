package raft

// Role is a node's current position in the Raft state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one committed (or pending) command in the replicated log.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Op    string `json:"op"` // "SET" | "DELETE" | "NOOP"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}
