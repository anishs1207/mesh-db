package main

import (
	"strings"
	"testing"

	"github.com/anishs1207/mesh-db/internal/storage"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &Server{store: store, nodeID: "test-node", clock: store.MaxVersion()}
}

func TestCommandLifecycle(t *testing.T) {
	server := newTestServer(t)
	if got := server.handleCommand("PING"); got != "PONG" {
		t.Fatalf("PING = %q", got)
	}
	if got := server.handleCommand("SET greeting hello distributed world"); !strings.HasPrefix(got, "OK version=") {
		t.Fatalf("SET = %q", got)
	}
	if got := server.handleCommand("GET greeting"); got != "hello distributed world (version=1 origin=test-node)" {
		t.Fatalf("GET = %q", got)
	}
}

func TestReplicaAdvancesLogicalClock(t *testing.T) {
	server := newTestServer(t)
	if got := server.handleCommand("REPLICA_SET remote 9 peer-a replicated value"); got != "OK" {
		t.Fatalf("REPLICA_SET = %q", got)
	}
	if got := server.handleCommand("SET local next"); got != "OK version=10" {
		t.Fatalf("SET after replica = %q", got)
	}
}
