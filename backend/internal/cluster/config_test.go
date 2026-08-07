package cluster

import "testing"

// setEnv sets each var via t.Setenv, which restores the previous value
// automatically when the test finishes.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestFromEnv_RequiresNodeID(t *testing.T) {
	setEnv(t, map[string]string{"NODE_ID": ""})
	_, err := FromEnv()
	if err == nil {
		t.Fatalf("expected an error when NODE_ID is unset")
	}
}

func TestFromEnv_Defaults(t *testing.T) {
	setEnv(t, map[string]string{"NODE_ID": "solo", "PEERS": "", "RAFT_PORT": "", "HTTP_PORT": "", "DATA_DIR": ""})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.RaftPort != 9090 || cfg.HTTPPort != 8080 {
		t.Fatalf("expected default ports 9090/8080, got %d/%d", cfg.RaftPort, cfg.HTTPPort)
	}
	if cfg.DataDir != "./data/solo" {
		t.Fatalf("expected default data dir ./data/solo, got %q", cfg.DataDir)
	}
	if len(cfg.Peers) != 0 {
		t.Fatalf("expected no peers, got %v", cfg.Peers)
	}
}

func TestFromEnv_ThreePartPeerFormat(t *testing.T) {
	setEnv(t, map[string]string{
		"NODE_ID": "node-1",
		"PEERS":   "node-2=localhost:9092:8082,node-3=localhost:9093:8083",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.Peers["node-2"] != "localhost:9092" {
		t.Fatalf("expected node-2 raft addr localhost:9092, got %q", cfg.Peers["node-2"])
	}
	if cfg.PeerHTTP["node-2"] != "localhost:8082" {
		t.Fatalf("expected node-2 http addr localhost:8082, got %q", cfg.PeerHTTP["node-2"])
	}
	if cfg.Peers["node-3"] != "localhost:9093" || cfg.PeerHTTP["node-3"] != "localhost:8083" {
		t.Fatalf("unexpected node-3 addrs: raft=%q http=%q", cfg.Peers["node-3"], cfg.PeerHTTP["node-3"])
	}
}

func TestFromEnv_FourPartPeerFormatWithDistinctHosts(t *testing.T) {
	// Simulates Docker Compose: peers dial each other by service name
	// internally, but the leader-redirect address a browser needs must be
	// the host-published address instead.
	setEnv(t, map[string]string{
		"NODE_ID": "node-1",
		"PEERS":   "node-2=node-2:9090:localhost:8082",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.Peers["node-2"] != "node-2:9090" {
		t.Fatalf("expected internal raft addr node-2:9090, got %q", cfg.Peers["node-2"])
	}
	if cfg.PeerHTTP["node-2"] != "localhost:8082" {
		t.Fatalf("expected browser-facing http addr localhost:8082, got %q", cfg.PeerHTTP["node-2"])
	}
}

func TestFromEnv_MalformedPeerEntry(t *testing.T) {
	setEnv(t, map[string]string{
		"NODE_ID": "node-1",
		"PEERS":   "node-2=onlyonefield",
	})
	_, err := FromEnv()
	if err == nil {
		t.Fatalf("expected an error for a malformed PEERS entry")
	}
}

func TestFromEnv_InvalidPort(t *testing.T) {
	setEnv(t, map[string]string{"NODE_ID": "node-1", "RAFT_PORT": "not-a-number"})
	_, err := FromEnv()
	if err == nil {
		t.Fatalf("expected an error for a non-numeric RAFT_PORT")
	}
}
