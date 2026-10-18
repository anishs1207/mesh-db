package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anishs1207/mesh-db/internal/cluster"
	"github.com/anishs1207/mesh-db/internal/storage"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	self := "test-node"
	return &Server{
		store:             store,
		ring:              cluster.NewRing([]string{self}, 16),
		self:              self,
		replicationFactor: 1,
		readQuorum:        1,
		writeQuorum:       1,
		nodeID:            self,
		clock:             store.MaxVersion(),
	}
}

func startTestReplica(t *testing.T) (string, *Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(t.TempDir())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	address := listener.Addr().String()
	server := &Server{
		store:             store,
		self:              address,
		nodeID:            address,
		replicationFactor: 1,
		readQuorum:        1,
		writeQuorum:       1,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConnection(conn, server)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		_ = store.Close()
	})
	return address, server
}

func TestCommandLifecycle(t *testing.T) {
	server := newTestServer(t)
	if got := server.handleCommand("PING"); got != "PONG" {
		t.Fatalf("PING = %q", got)
	}
	if got := server.handleCommand("SET greeting hello distributed world"); !strings.HasPrefix(got, "OK version=1 acknowledgements=1") {
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
	if got := server.handleCommand("SET local next"); got != "OK version=10 acknowledgements=1" {
		t.Fatalf("SET after replica = %q", got)
	}
}

func TestWriteQuorumWithUnavailableReplica(t *testing.T) {
	firstAddress, first := startTestReplica(t)
	secondAddress, second := startTestReplica(t)
	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddress := deadListener.Addr().String()
	_ = deadListener.Close()

	first.ring = cluster.NewRing([]string{firstAddress, secondAddress, deadAddress}, 64)
	first.replicationFactor = 3
	first.writeQuorum = 2
	if got := first.handleCommand("SET quorum survives one failure"); !strings.HasPrefix(got, "OK version=1 acknowledgements=2") {
		t.Fatalf("quorum SET = %q", got)
	}
	if value, ok := second.store.Get("quorum"); !ok || value.Value != "survives one failure" {
		t.Fatalf("second replica missing value: %#v ok=%v", value, ok)
	}

	first.writeQuorum = 3
	if got := first.handleCommand("SET strict unavailable"); !strings.HasPrefix(got, "ERR write_quorum_unavailable acknowledgements=2 required=3") {
		t.Fatalf("strict quorum SET = %q", got)
	}
}

func TestQuorumReadSelectsNewestAndRepairs(t *testing.T) {
	firstAddress, first := startTestReplica(t)
	secondAddress, second := startTestReplica(t)
	thirdAddress, third := startTestReplica(t)
	members := []string{firstAddress, secondAddress, thirdAddress}
	first.ring = cluster.NewRing(members, 64)
	first.replicationFactor = 3
	first.readQuorum = 2

	_, _ = second.store.Set("account", storage.Value{Value: "stale", Version: 4, Origin: "node-a"})
	winner := storage.Value{Value: "current balance", Version: 5, Origin: "node-b"}
	_, _ = third.store.Set("account", winner)

	if got := first.handleCommand("GET account"); got != "current balance (version=5 origin=node-b)" {
		t.Fatalf("quorum GET = %q", got)
	}
	for name, replica := range map[string]*Server{"first": first, "second": second, "third": third} {
		got, ok := replica.store.Get("account")
		if !ok || got != winner {
			t.Fatalf("%s replica not repaired: %#v ok=%v", name, got, ok)
		}
	}
}

func TestOperationsEndpoints(t *testing.T) {
	server := newTestServer(t)
	operations := newOperationsServer("127.0.0.1:0", server)

	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	operations.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready status = %d", response.Code)
	}

	server.stats.ready.Store(true)
	_ = server.handleCommand("PING")
	response = httptest.NewRecorder()
	operations.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "meshdb_commands_total 1") ||
		!strings.Contains(response.Body.String(), "meshdb_ready 1") {
		t.Fatalf("unexpected metrics response: status=%d body=%q", response.Code, response.Body.String())
	}
}
