// Command node starts a single Quorum cluster node: a Raft consensus
// participant reachable over gRPC by its peers, plus an HTTP gateway for
// clients and the dashboard. Which node it is, and who its peers are, is
// entirely determined by environment variables (see internal/cluster).
package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/jamesmartin6/quorum/backend/internal/cluster"
	"github.com/jamesmartin6/quorum/backend/internal/gateway"
	"github.com/jamesmartin6/quorum/backend/internal/kv"
	"github.com/jamesmartin6/quorum/backend/internal/raft"
	"github.com/jamesmartin6/quorum/backend/internal/rpc"
)

func main() {
	cfg, err := cluster.FromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", cfg.NodeID), log.Ltime|log.Lmicroseconds)
	logger.Printf("starting: raftPort=%d httpPort=%d peers=%v dataDir=%s", cfg.RaftPort, cfg.HTTPPort, cfg.PeerIDs(), cfg.DataDir)

	storage, err := raft.NewFileStorage(cfg.DataDir)
	if err != nil {
		logger.Fatalf("storage: %v", err)
	}

	transport := rpc.NewGRPCTransport(cfg.Peers)

	node := raft.New(raft.Config{
		ID:        cfg.NodeID,
		Peers:     cfg.PeerIDs(),
		Transport: transport,
		Storage:   storage,
		Logger:    logger,
	})
	node.Start()
	defer node.Stop()

	store := kv.NewStore()
	go store.Run(node.ApplyChan())

	grpcServer := rpc.NewGRPCServer(node)
	raftLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.RaftPort))
	if err != nil {
		logger.Fatalf("raft listen: %v", err)
	}
	go func() {
		logger.Printf("gRPC (Raft) listening on :%d", cfg.RaftPort)
		if err := grpcServer.Serve(raftLis); err != nil {
			logger.Fatalf("grpc serve: %v", err)
		}
	}()

	httpServer := gateway.NewServer(node, store, cfg.PeerHTTP, logger)
	logger.Printf("HTTP gateway listening on :%d", cfg.HTTPPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.HTTPPort), httpServer.Handler()); err != nil {
		logger.Fatalf("http serve: %v", err)
	}
}
