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
//	PEERS       - comma-separated "id=raftHost:raftPort:httpPort" for every OTHER node (required unless single-node)
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
			hostAndPorts := strings.Split(addr, ":")
			if len(hostAndPorts) != 3 {
				return Config{}, fmt.Errorf("cluster: malformed PEERS address %q for %q (want host:raftPort:httpPort)", addr, peerID)
			}
			host := hostAndPorts[0]
			peers[peerID] = fmt.Sprintf("%s:%s", host, hostAndPorts[1])
			peerHTTP[peerID] = fmt.Sprintf("%s:%s", host, hostAndPorts[2])
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
