package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anishs1207/mesh-db/internal/cluster"
	hintstore "github.com/anishs1207/mesh-db/internal/hints"
	"github.com/anishs1207/mesh-db/internal/storage"
)

var testClusterSecret = []byte("test-secret-that-is-at-least-32-bytes-long")
var testTopology = strings.Repeat("a", 64)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	self := "test-node"
	server := &Server{
		store:             store,
		ring:              cluster.NewRing([]string{self}, 16),
		self:              self,
		replicationFactor: 1,
		readQuorum:        1,
		writeQuorum:       1,
		nodeID:            self,
		clock:             store.MaxVersion(),
		clusterSecret:     testClusterSecret,
		topologyID:        testTopology,
	}
	t.Cleanup(server.stopBackground)
	return server
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
		topologyID:        testTopology,
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
		server.stopBackground()
		_ = store.Close()
	})
	return address, server
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true before deadline")
		}
		time.Sleep(time.Millisecond)
	}
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
	command := "REPLICA_SET " + testTopology + " remote 9 peer-a replicated value"
	if got := server.handleCommand(authenticatedCommand(command, testClusterSecret)); got != "OK" {
		t.Fatalf("REPLICA_SET = %q", got)
	}
	if got := server.handleCommand("SET local next"); got != "OK version=10 acknowledgements=1" {
		t.Fatalf("SET after replica = %q", got)
	}
}

func TestRejectsUnauthenticatedReplicaCommands(t *testing.T) {
	server := newTestServer(t)
	if got := server.handleCommand("REPLICA_SET " + testTopology + " key 999 attacker poisoned"); got != "ERR unauthorized" {
		t.Fatalf("unauthenticated replica write = %q", got)
	}
	if _, ok := server.store.Get("key"); ok {
		t.Fatal("unauthenticated replica write mutated storage")
	}
	if got := server.handleCommand("AUTH bad REPLICA_GET " + testTopology + " key"); got != "ERR unauthorized" {
		t.Fatalf("bad signature = %q", got)
	}
}

func TestRejectsReplicaCommandFromDifferentTopology(t *testing.T) {
	server := newTestServer(t)
	otherTopology := strings.Repeat("b", 64)
	command := "REPLICA_SET " + otherTopology + " key 1 peer value"
	if got := server.handleCommand(authenticatedCommand(command, testClusterSecret)); got != "ERR topology_mismatch" {
		t.Fatalf("mismatched topology response = %q", got)
	}
	if _, ok := server.store.Get("key"); ok {
		t.Fatal("mismatched topology mutated storage")
	}
}

func TestTopologyFingerprintIncludesConsistencyConfiguration(t *testing.T) {
	ring := cluster.NewRing([]string{"a", "b", "c"}, 64)
	baseline := topologyFingerprint(ring, 3, 2, 2)
	for name, fingerprint := range map[string]string{
		"membership":   topologyFingerprint(cluster.NewRing([]string{"a", "b", "d"}, 64), 3, 2, 2),
		"replication":  topologyFingerprint(ring, 2, 2, 2),
		"read quorum":  topologyFingerprint(ring, 3, 3, 2),
		"write quorum": topologyFingerprint(ring, 3, 2, 3),
	} {
		if fingerprint == baseline {
			t.Errorf("%s change did not change topology fingerprint", name)
		}
	}
}

func TestClusterMembershipValidationRejectsAmbiguity(t *testing.T) {
	if err := validateClusterMembers("node-a:7000", []string{"node-b:7000", "[::1]:7000"}); err != nil {
		t.Fatalf("valid members rejected: %v", err)
	}
	for name, peers := range map[string][]string{
		"self repeated":    {"node-a:7000"},
		"duplicate peer":   {"node-b:7000", "NODE-B:7000"},
		"missing port":     {"node-b"},
		"empty host":       {":7000"},
		"invalid port":     {"node-b:70000"},
		"non-numeric port": {"node-b:http"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateClusterMembers("node-a:7000", peers); err == nil {
				t.Fatal("invalid membership accepted")
			}
		})
	}
}

func TestReplicationFactorResolutionDoesNotSilentlyClamp(t *testing.T) {
	for _, test := range []struct {
		configured int
		members    int
		want       int
		wantErr    bool
	}{
		{configured: 0, members: 1, want: 1},
		{configured: 0, members: 5, want: 3},
		{configured: 2, members: 3, want: 2},
		{configured: 4, members: 3, wantErr: true},
		{configured: -1, members: 3, wantErr: true},
		{configured: 0, members: 0, wantErr: true},
	} {
		got, err := resolveReplicationFactor(test.configured, test.members)
		if (err != nil) != test.wantErr || (!test.wantErr && got != test.want) {
			t.Errorf("resolveReplicationFactor(%d, %d) = %d, %v; want %d, error=%v",
				test.configured, test.members, got, err, test.want, test.wantErr)
		}
	}
}

func TestRejectsReplicaCommandForNonOwner(t *testing.T) {
	server := newTestServer(t)
	server.self = "node-a"
	server.ring = cluster.NewRing([]string{"node-a", "node-b"}, 64)
	server.replicationFactor = 1
	key := ""
	for candidate := 0; candidate < 10000; candidate++ {
		possible := fmt.Sprintf("key-%d", candidate)
		if server.owners(possible)[0] == "node-b" {
			key = possible
			break
		}
	}
	if key == "" {
		t.Fatal("failed to find key owned by node-b")
	}
	command := "REPLICA_SET " + testTopology + " " + key + " 1 peer value"
	if got := server.handleCommand(authenticatedCommand(command, testClusterSecret)); got != "ERR not_owner" {
		t.Fatalf("non-owner response = %q", got)
	}
	if _, ok := server.store.Get(key); ok {
		t.Fatal("non-owner replica command mutated storage")
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
	for _, replica := range map[string]*Server{"first": first, "second": second, "third": third} {
		eventually(t, func() bool {
			got, ok := replica.store.Get("account")
			return ok && got == winner
		})
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
	for _, replica := range map[string]*Server{"first": first, "second": second, "third": third} {
		eventually(t, func() bool {
			got, ok := replica.store.Get("deleted")
			return ok && got.Tombstone && got.Version == 5
		})
	}
}

func TestSuccessfulWriteReturnsWithoutWaitingForSlowReplica(t *testing.T) {
	server := newTestServer(t)
	server.ring = cluster.NewRing([]string{"fast-a", "fast-b", "slow"}, 16)
	server.replicationFactor = 3
	server.writeQuorum = 2
	releaseSlow := make(chan struct{})
	server.replicaPut = func(owner, _ string, _ storage.Value) error {
		if owner == "slow" {
			<-releaseSlow
		}
		return nil
	}

	done := make(chan int, 1)
	go func() {
		acks, _ := server.quorumWrite("key", storage.Value{Version: 1, Origin: "test"})
		done <- acks
	}()
	select {
	case acks := <-done:
		if acks != 2 {
			t.Fatalf("acknowledgements = %d, want quorum of 2", acks)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseSlow)
		t.Fatal("write waited for a replica after reaching quorum")
	}
	close(releaseSlow)
}

func TestSuccessfulReadReturnsWithoutWaitingForSlowReplica(t *testing.T) {
	server := newTestServer(t)
	server.ring = cluster.NewRing([]string{"fast-a", "fast-b", "slow"}, 16)
	server.replicationFactor = 3
	server.readQuorum = 2
	releaseSlow := make(chan struct{})
	winner := storage.Value{Value: "value", Version: 2, Origin: "test"}
	server.replicaGet = func(owner, _ string) (storage.Value, bool, error) {
		if owner == "slow" {
			<-releaseSlow
		}
		return winner, true, nil
	}
	server.replicaPut = func(string, string, storage.Value) error { return nil }

	done := make(chan storage.Value, 1)
	go func() {
		value, _, _, _ := server.quorumRead("key")
		done <- value
	}()
	select {
	case got := <-done:
		if got != winner {
			t.Fatalf("value = %#v, want %#v", got, winner)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseSlow)
		t.Fatal("read waited for a replica after reaching quorum")
	}
	close(releaseSlow)
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
	eventually(t, func() bool {
		got, ok := third.store.Get("unread-key")
		return ok && got == winner
	})
	if first.stats.repairScans.Load() != 1 || first.stats.repairKeys.Load() != 1 {
		t.Fatalf("unexpected repair metrics: scans=%d keys=%d", first.stats.repairScans.Load(), first.stats.repairKeys.Load())
	}
}

func TestHintedHandoffDeliversPersistedMutation(t *testing.T) {
	_, coordinator := startTestReplica(t)
	ownerAddress, owner := startTestReplica(t)
	hints, err := hintstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coordinator.hints = hints
	value := storage.Value{Value: "eventual", Version: 12, Origin: "node-a"}
	if err := hints.Put(ownerAddress, "missed", value); err != nil {
		t.Fatal(err)
	}
	coordinator.deliverHints()
	if got, ok := owner.store.Get("missed"); !ok || got != value {
		t.Fatalf("hint not delivered: %#v ok=%v", got, ok)
	}
	items, err := hints.List()
	if err != nil || len(items) != 0 {
		t.Fatalf("delivered hint retained: %#v err=%v", items, err)
	}
	if coordinator.stats.hintsDelivered.Load() != 1 {
		t.Fatalf("delivered metric = %d", coordinator.stats.hintsDelivered.Load())
	}
}

func TestHintedHandoffDeletesSupersededMutation(t *testing.T) {
	_, coordinator := startTestReplica(t)
	ownerAddress, owner := startTestReplica(t)
	hints, err := hintstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coordinator.hints = hints
	newerValue := storage.Value{Value: "newer", Version: 13, Origin: "node-b"}
	if _, err := owner.store.Set("key", newerValue); err != nil {
		t.Fatal(err)
	}
	obsolete := storage.Value{Value: "obsolete", Version: 12, Origin: "node-a"}
	if err := hints.Put(ownerAddress, "key", obsolete); err != nil {
		t.Fatal(err)
	}

	coordinator.deliverHints()
	items, err := hints.List()
	if err != nil || len(items) != 0 {
		t.Fatalf("obsolete hint retained: %#v err=%v", items, err)
	}
	if got, _ := owner.store.Get("key"); got != newerValue {
		t.Fatalf("newer replica value changed: %#v", got)
	}
	if coordinator.stats.hintsSuperseded.Load() != 1 {
		t.Fatalf("superseded metric = %d", coordinator.stats.hintsSuperseded.Load())
	}
}

func TestUnavailableHintDoesNotStarveLaterDelivery(t *testing.T) {
	server := newTestServer(t)
	hints, err := hintstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server.hints = hints
	server.hintBatchSize = 1
	if err := hints.Put("down", "blocked", storage.Value{Version: 1, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	if err := hints.Put("up", "healthy", storage.Value{Version: 2, Origin: "node"}); err != nil {
		t.Fatal(err)
	}
	server.replicaPut = func(owner, _ string, _ storage.Value) error {
		if owner == "down" {
			return errors.New("unavailable")
		}
		return nil
	}

	server.deliverHints()
	server.deliverHints()
	items, err := hints.List()
	if err != nil || len(items) != 1 || items[0].Owner != "down" {
		t.Fatalf("remaining hints = %#v, err=%v; want only unavailable owner", items, err)
	}
}

func TestHintDeliveryStopsAfterCancellation(t *testing.T) {
	server := newTestServer(t)
	hints, err := hintstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server.hints = hints
	server.hintBatchSize = 256
	for index, key := range []string{"first", "second", "third"} {
		if err := hints.Put("owner", key, storage.Value{Version: uint64(index + 1), Origin: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	server.replicaPut = func(string, string, storage.Value) error {
		calls++
		cancel()
		return nil
	}

	server.deliverHintsContext(ctx)
	if calls != 1 {
		t.Fatalf("deliveries after cancellation = %d, want 1", calls)
	}
	remaining, err := hints.List()
	if err != nil || len(remaining) != 2 {
		t.Fatalf("remaining hints = %d, err=%v; want 2", len(remaining), err)
	}
}

func TestRepairScanStopsAfterCancellation(t *testing.T) {
	server := newTestServer(t)
	for index, key := range []string{"first", "second", "third"} {
		if _, err := server.store.Set(key, storage.Value{Version: uint64(index + 1), Origin: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	server.replicaGet = func(_ string, key string) (storage.Value, bool, error) {
		calls++
		cancel()
		value, ok := server.store.Get(key)
		return value, ok, nil
	}

	server.repairOnceContext(ctx)
	if calls != 1 {
		t.Fatalf("repairs after cancellation = %d, want 1", calls)
	}
	if server.stats.repairKeys.Load() != 1 {
		t.Fatalf("repaired keys = %d, want 1", server.stats.repairKeys.Load())
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

func TestBoundedPeerResponseReader(t *testing.T) {
	for name, test := range map[string]struct {
		input   string
		limit   int
		want    string
		wantErr bool
	}{
		"normal":            {input: "OK\n", limit: 2, want: "OK"},
		"exact boundary":    {input: "1234\n", limit: 4, want: "1234"},
		"oversized":         {input: "12345\n", limit: 4, wantErr: true},
		"missing delimiter": {input: "OK", limit: 4, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := readBoundedLine(strings.NewReader(test.input), test.limit)
			if test.wantErr && err == nil {
				t.Fatalf("read = %q, expected error", got)
			}
			if !test.wantErr && (err != nil || got != test.want) {
				t.Fatalf("read = %q, err=%v; want %q", got, err, test.want)
			}
		})
	}
}

func TestPlaintextListenRequiresLoopbackOrExplicitOptIn(t *testing.T) {
	for _, address := range []string{"127.0.0.1:7000", "[::1]:7000", "localhost:7000"} {
		if err := validateListenAddress(address, false, false); err != nil {
			t.Errorf("loopback address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:7000", ":7000", "192.0.2.10:7000", "db.internal:7000"} {
		if err := validateListenAddress(address, false, false); err == nil {
			t.Errorf("plaintext non-loopback address %q accepted", address)
		}
		if err := validateListenAddress(address, true, false); err != nil {
			t.Errorf("TLS address %q rejected: %v", address, err)
		}
		if err := validateListenAddress(address, false, true); err != nil {
			t.Errorf("explicit insecure address %q rejected: %v", address, err)
		}
	}
	if err := validateListenAddress("not-an-address", true, false); err == nil {
		t.Fatal("malformed listen address accepted")
	}
}
