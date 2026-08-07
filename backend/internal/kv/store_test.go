package kv

import (
	"sync"
	"testing"

	"github.com/jamesmartin6/quorum/backend/internal/raft"
)

func TestStore_ApplySetAndGet(t *testing.T) {
	s := NewStore()
	s.Apply(raft.LogEntry{Index: 1, Term: 1, Op: "SET", Key: "a", Value: "1"})

	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("expected a=1, got %q ok=%v", v, ok)
	}
	if s.LastApplied() != 1 {
		t.Fatalf("expected lastApplied=1, got %d", s.LastApplied())
	}
}

func TestStore_ApplyOverwritesExistingKey(t *testing.T) {
	s := NewStore()
	s.Apply(raft.LogEntry{Index: 1, Op: "SET", Key: "a", Value: "1"})
	s.Apply(raft.LogEntry{Index: 2, Op: "SET", Key: "a", Value: "2"})

	v, ok := s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("expected a=2 after overwrite, got %q ok=%v", v, ok)
	}
}

func TestStore_ApplyDelete(t *testing.T) {
	s := NewStore()
	s.Apply(raft.LogEntry{Index: 1, Op: "SET", Key: "a", Value: "1"})
	s.Apply(raft.LogEntry{Index: 2, Op: "DELETE", Key: "a"})

	_, ok := s.Get("a")
	if ok {
		t.Fatalf("expected a to be deleted")
	}
}

func TestStore_GetMissingKey(t *testing.T) {
	s := NewStore()
	_, ok := s.Get("nope")
	if ok {
		t.Fatalf("expected missing key to report ok=false")
	}
}

func TestStore_NoopHasNoStateEffect(t *testing.T) {
	s := NewStore()
	s.Apply(raft.LogEntry{Index: 1, Op: "NOOP"})

	if len(s.Snapshot()) != 0 {
		t.Fatalf("expected NOOP to have no effect on the map, got %v", s.Snapshot())
	}
	if s.LastApplied() != 1 {
		t.Fatalf("expected lastApplied to still advance past a NOOP, got %d", s.LastApplied())
	}
}

func TestStore_RunConsumesApplyChannel(t *testing.T) {
	s := NewStore()
	ch := make(chan raft.ApplyMsg, 4)
	ch <- raft.ApplyMsg{Index: 1, Entry: raft.LogEntry{Index: 1, Op: "SET", Key: "x", Value: "1"}}
	ch <- raft.ApplyMsg{Index: 2, Entry: raft.LogEntry{Index: 2, Op: "SET", Key: "y", Value: "2"}}
	close(ch)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run(ch)
	}()
	wg.Wait()

	if v, ok := s.Get("x"); !ok || v != "1" {
		t.Fatalf("expected x=1, got %q ok=%v", v, ok)
	}
	if v, ok := s.Get("y"); !ok || v != "2" {
		t.Fatalf("expected y=2, got %q ok=%v", v, ok)
	}
}

func TestStore_ConcurrentAccessIsSafe(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Apply(raft.LogEntry{Index: uint64(i + 1), Op: "SET", Key: "k", Value: "v"})
			s.Get("k")
			s.Snapshot()
		}(i)
	}
	wg.Wait()
}
