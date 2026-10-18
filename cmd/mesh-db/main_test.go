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

var testClusterSecret = []byte("test-secret-that-is-at-least-32-bytes-long")

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
		clusterSecret:     testClusterSecret,
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
		clusterSecret:     testClusterSecret,
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
	if got := server.handleCommand("DELETE greeting"); got != "OK version=2 acknowledgements=1" {
		t.Fatalf("DELETE = %q", got)
	}
	if got := server.handleCommand("GET greeting"); got != "NOT_FOUND" {
		t.Fatalf("GET deleted key = %q", got)
	}
}

func TestReplicaAdvancesLogicalClock(t *testing.T) {
	server := newTestServer(t)
	command := "REPLICA_SET remote 9 peer-a replicated value"
	if got := server.handleCommand(authenticatedCommand(command, testClusterSecret)); got != "OK" {
		t.Fatalf("REPLICA_SET = %q", got)
	}
	if got := server.handleCommand("SET local next"); got != "OK version=10 acknowledgements=1" {
		t.Fatalf("SET after replica = %q", got)
	}
}

func TestRejectsUnauthenticatedReplicaCommands(t *testing.T) {
	server := newTestServer(t)
	if got := server.handleCommand("REPLICA_SET key 999 attacker poisoned"); got != "ERR unauthorized" {
		t.Fatalf("unauthenticated replica write = %q", got)
	}
	if _, ok := server.store.Get("key"); ok {
		t.Fatal("unauthenticated replica write mutated storage")
	}
	if got := server.handleCommand("AUTH bad REPLICA_GET key"); got != "ERR unauthorized" {
		t.Fatalf("bad signature = %q", got)
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

func TestQuorumReadDoesNotResurrectDeletedValue(t *testing.T) {
	firstAddress, first := startTestReplica(t)
	secondAddress, second := startTestReplica(t)
	thirdAddress, third := startTestReplica(t)
	first.ring = cluster.NewRing([]string{firstAddress, secondAddress, thirdAddress}, 64)
	first.replicationFactor = 3
	first.readQuorum = 2

	_, _ = first.store.Set("deleted", storage.Value{Value: "stale", Version: 4, Origin: "node-a"})
	_, _ = second.store.Set("deleted", storage.Value{Version: 5, Origin: "node-b", Tombstone: true})
	_, _ = third.store.Set("deleted", storage.Value{Value: "older", Version: 3, Origin: "node-c"})

	if got := first.handleCommand("GET deleted"); got != "NOT_FOUND" {
		t.Fatalf("deleted GET = %q", got)
	}
	for name, replica := range map[string]*Server{"first": first, "second": second, "third": third} {
		got, ok := replica.store.Get("deleted")
		if !ok || !got.Tombstone || got.Version != 5 {
			t.Fatalf("%s tombstone not repaired: %#v ok=%v", name, got, ok)
		}
	}
}

func TestCoordinatorAdvancesPastRemoteVersionBeforeWrite(t *testing.T) {
	firstAddress, first := startTestReplica(t)
	secondAddress, second := startTestReplica(t)
	first.ring = cluster.NewRing([]string{firstAddress, secondAddress}, 64)
	first.replicationFactor = 2
	first.readQuorum = 2
	first.writeQuorum = 2
	_, _ = second.store.Set("counter", storage.Value{Value: "old", Version: 50, Origin: "remote"})

	if got := first.handleCommand("SET counter new"); got != "OK version=51 acknowledgements=2" {
		t.Fatalf("SET after remote version = %q", got)
	}
	for name, replica := range map[string]*Server{"first": first, "second": second} {
		got, ok := replica.store.Get("counter")
		if !ok || got.Version != 51 || got.Value != "new" {
			t.Fatalf("%s replica has %#v ok=%v", name, got, ok)
		}
	}
}

func TestAntiEntropyRepairsKeyWithoutClientRead(t *testing.T) {
	firstAddress, first := startTestReplica(t)
	secondAddress, second := startTestReplica(t)
	thirdAddress, third := startTestReplica(t)
	members := []string{firstAddress, secondAddress, thirdAddress}
	first.ring = cluster.NewRing(members, 64)
	first.replicationFactor = 3
	first.readQuorum = 2

	winner := storage.Value{Value: "durable", Version: 8, Origin: "node-a"}
	_, _ = first.store.Set("unread-key", winner)
	_, _ = second.store.Set("unread-key", winner)

	first.repairOnce()
	if got, ok := third.store.Get("unread-key"); !ok || got != winner {
		t.Fatalf("third replica was not repaired: %#v ok=%v", got, ok)
	}
	if first.stats.repairScans.Load() != 1 || first.stats.repairKeys.Load() != 1 {
		t.Fatalf("unexpected repair metrics: scans=%d keys=%d", first.stats.repairScans.Load(), first.stats.repairKeys.Load())
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

func TestTLSConfigurationMustBeComplete(t *testing.T) {
	if config, err := loadTLSConfig("", "", ""); err != nil || config != nil {
		t.Fatalf("disabled TLS = %#v, %v", config, err)
	}
	if _, err := loadTLSConfig("certificate.pem", "", "ca.pem"); err == nil {
		t.Fatal("expected partial TLS configuration to fail")
	}
}
