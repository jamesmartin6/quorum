// Package cluster resolves a single node's runtime configuration (its own
// ID/ports and its peers' addresses) from environment variables, so the
// same binary run with different env vars becomes any node in the cluster
// (see docker-compose.yml).
package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is one node's fully-resolved runtime configuration.
type Config struct {
	NodeID string

	// RaftPort is the gRPC port other nodes dial for RequestVote/AppendEntries.
	RaftPort int
	// HTTPPort serves the REST/WS gateway for clients and the frontend.
	HTTPPort int

	// Peers maps every OTHER node's ID to its gRPC "host:port" address.
	Peers map[string]string

	// PeerHTTP maps every OTHER node's ID to its HTTP "host:port" address,
	// used by the gateway to build a public-facing redirect URL to the
	// current leader.
	PeerHTTP map[string]string

	DataDir string
}

// PeerIDs returns the peer ID list in a stable (sorted) order.
func (c Config) PeerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for id := range c.Peers {
		ids = append(ids, id)
	}
	return ids
}

// FromEnv builds a Config from environment variables:
//
//	NODE_ID     - this node's unique ID (required)
//	PEERS       - comma-separated peer list, required unless single-node. Each entry is
//	              "id=raftHost:raftPort:httpPort" (3-part) or, when the address a browser
//	              should use to reach a peer's HTTP gateway differs from the address other
//	              nodes use to reach its gRPC port - e.g. in Docker Compose, where peers
//	              dial each other by service name but a leader-redirect response has to
//	              give the browser a host-published address - "id=raftHost:raftPort:httpHost:httpPort"
//	              (4-part).
//	RAFT_PORT   - this node's gRPC port (default 9090)
//	HTTP_PORT   - this node's HTTP gateway port (default 8080)
//	DATA_DIR    - persistence directory (default "./data/<NODE_ID>")
func FromEnv() (Config, error) {
	id := os.Getenv("NODE_ID")
	if id == "" {
		return Config{}, fmt.Errorf("cluster: NODE_ID is required")
	}

	raftPort, err := intEnv("RAFT_PORT", 9090)
	if err != nil {
		return Config{}, err
	}
	httpPort, err := intEnv("HTTP_PORT", 8080)
	if err != nil {
		return Config{}, err
	}

	peers := map[string]string{}
	peerHTTP := map[string]string{}
	if raw := os.Getenv("PEERS"); raw != "" {
		for _, entry := range strings.Split(raw, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			parts := strings.SplitN(entry, "=", 2)
			if len(parts) != 2 {
				return Config{}, fmt.Errorf("cluster: malformed PEERS entry %q (want id=host:raftPort:httpPort)", entry)
			}
			peerID, addr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			segs := strings.Split(addr, ":")
			switch len(segs) {
			case 3: // host:raftPort:httpPort - same host for both
				peers[peerID] = fmt.Sprintf("%s:%s", segs[0], segs[1])
				peerHTTP[peerID] = fmt.Sprintf("%s:%s", segs[0], segs[2])
			case 4: // raftHost:raftPort:httpHost:httpPort - distinct hosts
				peers[peerID] = fmt.Sprintf("%s:%s", segs[0], segs[1])
				peerHTTP[peerID] = fmt.Sprintf("%s:%s", segs[2], segs[3])
			default:
				return Config{}, fmt.Errorf("cluster: malformed PEERS address %q for %q (want host:raftPort:httpPort or raftHost:raftPort:httpHost:httpPort)", addr, peerID)
			}
		}
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = fmt.Sprintf("./data/%s", id)
	}

	return Config{
		NodeID:   id,
		RaftPort: raftPort,
		HTTPPort: httpPort,
		Peers:    peers,
		PeerHTTP: peerHTTP,
		DataDir:  dataDir,
	}, nil
}

func intEnv(key string, def int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("cluster: %s must be an integer, got %q", key, raw)
	}
	return v, nil
}
