# Progress — Quorum (Distributed KV Store with Raft)

This file is the single source of truth for build progress. It is updated after
every completed task so work can be resumed from a clean state by anyone (or any
agent) picking this up cold — read this file first, then check `git log`.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done

## Environment notes (local dev box only — irrelevant for a fresh clone/CI)

The local Windows dev machine did not have git/node/protoc preinstalled. They were
downloaded as portable binaries under `C:\Users\James\tools\` (node, PortableGit,
protoc) and are added to PATH via an inline `$env:PATH` prefix in every shell
command (registry PATH edits did not propagate to new shell processes in this
harness). This is a local-machine quirk only — the repo itself has no dependency
on this; a normal `git clone` + `go build` + `npm install` on any machine with
Go 1.22+/Node 20+/git works normally. Docker Compose is the primary supported way
to run the whole system.

## Key decisions

- Repo name: **quorum** (fits the Raft "majority quorum" theme).
- Inter-node RPC: real gRPC (protoc + protoc-gen-go + protoc-gen-go-grpc), per spec.
- Log persistence: append-only JSON-lines file per node under `backend/data/<nodeID>/`
  (simple, human-inspectable, good enough for the project's scope — not BoltDB).
- Frontend transport: polling `/cluster/status` + `/kv/*` every ~300ms per node,
  which is simpler and just as visually smooth as WebSockets for this use case.
  (May add a WS event stream later if time permits — polling is the committed baseline.)
- No Claude co-authorship in any git commit message.
- GitHub repo is public, created via `gh repo create`.

## Phase 0 — Setup
- [x] Install missing local toolchain (git, node/npm, protoc + Go protoc plugins)
- [x] Scaffold directory structure per build plan
- [ ] `git init`, initial commit
- [ ] Create GitHub repo `quorum` (public), push
- [ ] Schedule cloud-agent safety net (RemoteTrigger one-shots) to continue overnight
      if this local session hits a usage limit before the project is finished

## Phase 1 — Core Raft: Leader Election
- [ ] `backend/go.mod` module init
- [ ] `internal/rpc/raft.proto` — RequestVote + AppendEntries (empty entries for now) RPCs
- [ ] Generate `raft.pb.go` / `raft_grpc.pb.go`
- [ ] `internal/raft/state.go` — persistent state (currentTerm, votedFor, log placeholder)
- [ ] `internal/raft/raft.go` — core struct, roles (Follower/Candidate/Leader), state transitions
- [ ] `internal/raft/election.go` — randomized election timeout, RequestVote handling/sending
- [ ] `internal/rpc/server.go` — gRPC server wiring, in-process transport interface for tests
- [ ] Unit tests: election timeout → candidacy; split vote resolves on retry; higher term → step-down
- [ ] `cmd/node/main.go` — starts one node from env vars (ID, peers, ports)
- [ ] Manual verify: `docker compose up` (minimal compose) → one leader within ~1s; `docker kill` leader → re-election within ~1-2s

## Phase 2 — Log Replication
- [ ] Extend proto: AppendEntries carries entries[], prevLogIndex/Term, leaderCommit
- [ ] `internal/raft/log.go` — log storage + append-only file persistence, replay on restart
- [ ] Leader replication loop (parallel per-follower), nextIndex/matchIndex tracking
- [ ] Follower consistency check + nextIndex backoff on mismatch
- [ ] commitIndex advancement on majority replication; apply to state machine in order
- [ ] Unit tests for log matching property
- [ ] Integration test: kill+restart follower mid-write, assert log convergence

## Phase 3 — KV API Layer
- [ ] `internal/kv/store.go` — in-memory map + `Apply(entry)`
- [ ] `internal/gateway/http.go` — POST/GET/DELETE /kv/{key}, GET /cluster/status
- [ ] Leader forwarding/redirect for writes and reads on non-leader nodes
- [ ] Tests: SET via any node visible everywhere after commit; write-to-follower errors correctly

## Phase 4 — Frontend Cluster Visualizer
- [ ] Vite + React app scaffold
- [ ] `lib/api.js` REST client, `hooks/useClusterSocket.js` (polling-based) state hook
- [ ] `ClusterView.jsx` — node graph, leader highlight, heartbeat pulses, term display
- [ ] `KVConsole.jsx` — SET/GET/DELETE form + result/handler display
- [ ] `LogViewer.jsx` — per-node log table with commit index marker
- [ ] Visual verification against a running local cluster

## Phase 5 — Chaos Testing UI
- [ ] `internal/gateway/chaos.go` — POST /chaos/kill, /chaos/revive (stop/resume Raft participation without killing process)
- [ ] `ChaosControls.jsx` — Kill/Revive buttons per node
- [ ] Visual verification: kill leader → re-election visible; revive → catch-up visible

## Phase 6 — Dockerization & Polish
- [ ] `docker-compose.yml` — 5 backend nodes + frontend, one command startup
- [ ] Structured logging with node ID prefix
- [ ] `README.md` — architecture diagram, quickstart, demo GIF/recording
- [ ] Final pass: lint/vet/test all green, clean up TODOs

## Testing Requirements (cross-phase)
- [ ] Unit tests use in-memory RPC mock (no real network) for fast deterministic runs
- [ ] Integration tests use real gRPC across goroutines/processes
- [ ] Explicit test: kill leader mid-write → no data loss → new leader elected

## Final delivery checklist
- [ ] `docker compose up` works from a clean clone with zero manual steps
- [ ] All unit + integration tests pass (`go test ./...`)
- [ ] Frontend builds (`npm run build`)
- [ ] README reviewed for a strong first impression (this is the GitHub homepage)
- [ ] progress.md fully checked off, final commit pushed

---
**If you are an agent resuming this project:** read this file top to bottom, run
`git log --oneline -20` to see what's actually landed, pick the first unchecked
box, and continue. Commit after every meaningful step with a plain, descriptive
message (no AI co-author trailers). Do not ask the user questions — make
reasonable engineering decisions and note them in "Key decisions" above.
