# Distributed KV Store with Raft — Build Plan & Spec

## Project Overview

A full-stack distributed key-value store built on the Raft consensus algorithm, with a real-time web dashboard for visualizing cluster state, leader election, and log replication as they happen.

**Goal:** A working 3–5 node Raft cluster (Go) exposing a KV API, plus a React frontend that visualizes leader election, heartbeats, log replication, and lets a user kill a node live to watch the cluster recover.

**Tech stack:**
- Backend: Go, gRPC (inter-node Raft communication), a lightweight HTTP + WebSocket gateway per node
- Frontend: React, WebSockets for live state, Framer Motion (or CSS transitions) for animations
- Orchestration: Docker Compose (spin up N nodes + frontend with one command)
- Testing: Go's built-in testing package + testify; a small integration test harness that kills/restarts nodes

---

## Repo Structure

```
raft-kv/
├── docker-compose.yml
├── README.md
├── backend/
│   ├── go.mod
│   ├── cmd/
│   │   └── node/
│   │       └── main.go            # entrypoint: starts a single node
│   ├── internal/
│   │   ├── raft/
│   │   │   ├── raft.go            # core state machine (leader/follower/candidate)
│   │   │   ├── election.go        # leader election logic
│   │   │   ├── replication.go     # log replication (AppendEntries)
│   │   │   ├── log.go             # log entry storage + persistence
│   │   │   ├── state.go           # persistent state (term, votedFor, log)
│   │   │   └── raft_test.go
│   │   ├── rpc/
│   │   │   ├── raft.proto         # gRPC service definitions
│   │   │   ├── raft_grpc.pb.go    # generated
│   │   │   └── server.go          # gRPC server wiring
│   │   ├── kv/
│   │   │   ├── store.go           # in-memory KV store, applies committed log entries
│   │   │   └── store_test.go
│   │   ├── gateway/
│   │   │   ├── http.go            # REST endpoints (GET/SET/DELETE)
│   │   │   ├── websocket.go       # live event stream to frontend
│   │   │   └── chaos.go           # kill/restart node endpoint
│   │   └── cluster/
│   │       └── config.go          # cluster membership, node addresses
│   └── test/
│       └── integration_test.go    # multi-node cluster tests, kill/recover scenarios
└── frontend/
    ├── package.json
    ├── src/
    │   ├── App.jsx
    │   ├── components/
    │   │   ├── ClusterView.jsx    # node graph, leader highlight, heartbeat pulses
    │   │   ├── KVConsole.jsx      # SET/GET/DELETE UI
    │   │   ├── LogViewer.jsx      # per-node log + commit index table
    │   │   └── ChaosControls.jsx  # kill/restart node buttons
    │   ├── hooks/
    │   │   └── useClusterSocket.js # WebSocket connection + state management
    │   └── lib/
    │       └── api.js             # REST client
    └── public/
```

---

## Phase 1 — Core Raft: Leader Election

**Deliverable:** A cluster of nodes that can elect a single leader and maintain it via heartbeats, with no data storage yet.

**Spec:**
- Each node has persistent state: `currentTerm` (int), `votedFor` (node ID or nil), `log` (empty for now)
- Each node has volatile state: `role` (Follower/Candidate/Leader), `commitIndex`, `lastApplied`
- Nodes start as Followers with a randomized election timeout (150–300ms)
- If a Follower's timeout elapses with no heartbeat from a leader, it becomes a Candidate: increments `currentTerm`, votes for itself, sends `RequestVote` RPCs to all peers
- `RequestVote` RPC: candidate wins a peer's vote if the candidate's term ≥ voter's term AND voter hasn't already voted this term AND candidate's log is at least as up-to-date as the voter's
- Candidate becomes Leader on majority vote; immediately starts sending empty `AppendEntries` (heartbeats) to all peers at a fixed interval (e.g. every 50ms)
- Any node that sees a higher term than its own immediately reverts to Follower and updates its term
- gRPC service (`raft.proto`) must define `RequestVote` and `AppendEntries` RPCs with standard Raft fields (term, candidateId/leaderId, lastLogIndex, lastLogTerm, etc.)

**Definition of done:**
- Spin up 3 nodes via Docker Compose; exactly one becomes leader within ~1 second
- Kill the leader process; a new leader is elected within ~1–2 seconds
- Unit tests cover: election timeout triggers candidacy, split-vote scenario resolves on retry, higher term causes step-down

---

## Phase 2 — Log Replication

**Deliverable:** Leader accepts log entries and replicates them to followers with correct commit semantics.

**Spec:**
- `AppendEntries` RPC extended to carry log entries: `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`
- Leader appends a new entry to its own log first, then replicates to followers in parallel
- Follower consistency check: reject `AppendEntries` if its log doesn't contain an entry at `prevLogIndex` matching `prevLogTerm` (log matching property) — leader retries with a decremented `nextIndex` for that follower until consistent
- Leader advances `commitIndex` once an entry is replicated on a majority of nodes; committed entries are applied to the KV state machine in order
- Followers apply entries once they learn the new `commitIndex` from the leader's next heartbeat
- Persist the log to disk (simple append-only file or embedded key-value store like BoltDB/Badger is fine) so a restarted node doesn't lose its log

**Definition of done:**
- Write requests sent to the leader are visible on all nodes within a few hundred ms
- Killing and restarting a follower results in it catching up correctly via log replication
- Integration test: partition/kill a follower mid-write, bring it back, assert its log converges with the leader's

---

## Phase 3 — KV API Layer

**Deliverable:** A usable key-value store on top of committed Raft log entries, reachable over HTTP.

**Spec:**
- Log entries are KV commands: `{op: "SET"|"DELETE", key: string, value: string}`
- `internal/kv/store.go`: an `Apply(entry)` method invoked once an entry commits; maintains an in-memory `map[string]string`
- REST endpoints exposed by each node's gateway:
  - `POST /kv/{key}` body `{value: string}` → SET (must be forwarded to the leader if this node isn't leader; return `503` with leader address if unknown, or transparently proxy)
  - `GET /kv/{key}` → return current value (reads served by leader only, for linearizability — reject/redirect if not leader)
  - `DELETE /kv/{key}` → DELETE
  - `GET /cluster/status` → returns this node's role, term, leader ID, commit index, log length
- Non-leader nodes reject writes with a clear error pointing to the current leader's address (simplifies frontend logic — no need for smart client-side leader tracking beyond following redirects)

**Definition of done:**
- Can SET a key via any node's REST endpoint and read it back consistently from all nodes after commit
- Writing to a follower correctly errors/redirects rather than silently failing or diverging state

---

## Phase 4 — Frontend: Cluster Visualizer

**Deliverable:** A React dashboard showing live cluster state.

**Spec:**
- Each node's gateway exposes a WebSocket endpoint (`/ws`) that pushes state-change events: `{type: "role_change"|"heartbeat"|"log_append"|"commit", nodeId, term, ...}`
- Frontend connects to all nodes' WebSocket endpoints (or a single aggregator endpoint if simpler — pick one node as the aggregation point, or have the frontend poll `/cluster/status` on all nodes plus subscribe to one WS stream for animation events)
- `ClusterView.jsx`: renders each node as a circle; leader is visually distinct (color + crown icon); draws animated pulse lines from leader to followers on each heartbeat; shows current term number prominently
- `KVConsole.jsx`: simple form to SET/GET/DELETE a key, shows request result and which node handled it
- `LogViewer.jsx`: table of each node's log entries (index, term, command) with the commit index marked, so replication lag is visible
- Use polling (every 200–500ms) as a fallback/simpler alternative to full WebSocket event streaming if that's faster to implement — the visual goal matters more than the transport mechanism

**Definition of done:**
- Opening the dashboard shows all nodes, correct leader highlighted, term number updating on new elections
- Writing a key via the console visibly animates replication across nodes and updates the log viewer

---

## Phase 5 — Chaos Testing UI

**Deliverable:** A "kill node" button that triggers real leader re-election, visible live on the dashboard.

**Spec:**
- `chaos.go`: an endpoint (`POST /chaos/kill`) that stops this node's Raft participation (e.g. closes its gRPC server / stops responding to RPCs) without killing the whole process, so it can be "revived" via a matching `/chaos/revive` endpoint
- `ChaosControls.jsx`: buttons per node — "Kill" / "Revive" — calling these endpoints
- When a node is killed, the visualizer should show it going gray/offline, then show the remaining nodes electing a new leader in real time, then (on revive) show the node rejoining and catching up its log

**Definition of done:**
- Killing the current leader from the UI triggers a visible re-election within a couple seconds, with the new leader clearly highlighted
- Reviving a killed node shows it catching up and rejoining as a follower without manual restart

---

## Phase 6 — Dockerization & Polish

**Deliverable:** One-command startup, finished README with a demo GIF.

**Spec:**
- `docker-compose.yml`: 3–5 backend node services (each with a distinct node ID and peer list via env vars) + 1 frontend service; expose each node's REST/WS port so the frontend can reach all of them
- README: architecture diagram, setup instructions (`docker compose up`), and a short GIF/screen recording demoing leader election + chaos kill
- Clean up logging (structured logs with node ID prefix) so `docker compose logs` is readable during a live demo

**Definition of done:**
- `docker compose up` brings up the full cluster + dashboard with zero manual steps
- A stranger cloning the repo can get the demo running from the README alone

---

## Testing Requirements (applies across phases)

- Unit tests for Raft core logic (election, replication, log matching) — don't rely on real network calls, use an in-memory RPC mock between nodes for fast deterministic tests
- Integration tests using real gRPC between processes/goroutines: multi-node cluster formation, leader election under node failure, log convergence after partition/recovery
- At minimum, one test that simulates the exact "kill leader mid-write, verify no data loss, verify new leader elected" scenario — this is the core correctness guarantee of Raft and worth testing explicitly

---

## Suggested Build Order for Claude Code

Implement and verify each phase fully (including its tests) before moving to the next — each phase depends on the last being correct:

1. Phase 1 (leader election) → verify with `docker compose up` + manual `docker kill` on the leader container
2. Phase 2 (log replication) → verify with integration test
3. Phase 3 (KV API) → verify with curl/Postman against a running cluster
4. Phase 4 (frontend visualizer) → verify visually
5. Phase 5 (chaos UI) → verify visually
6. Phase 6 (Docker + docs) → final polish pass
