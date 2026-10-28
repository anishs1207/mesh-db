package main

import (
	"errors"
	"testing"

	"github.com/anishs1207/mesh-db/internal/storage"
)

func TestReplicaWriteConflictFencesReceiver(t *testing.T) {
	for _, test := range []struct {
		name    string
		command string
	}{
		{"set", "REPLICA_SET " + testTopology + " key 7 origin different"},
		{"delete", "REPLICA_DELETE " + testTopology + " key 7 origin"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newTestServer(t)
			if _, err := server.store.Set("key", storage.Value{Value: "original", Version: 7, Origin: "origin"}); err != nil {
				t.Fatal(err)
			}
			if got := server.handleCommand(mustAuthenticate(t, test.command)); got != "ERR version_conflict" {
				t.Fatalf("replica response = %q", got)
			}
			if !server.stats.consistencyFenced.Load() || server.stats.versionConflicts.Load() != 1 {
				t.Fatal("receiver was not fenced after version conflict")
			}
			if got := server.handleCommand("GET key"); got != "ERR consistency_fenced" {
				t.Fatalf("client read after conflict = %q", got)
			}
		})
	}
}

func TestRemoteReplicaWriteConflictFencesCoordinator(t *testing.T) {
	address, receiver := startTestReplica(t)
	if _, err := receiver.store.Set("key", storage.Value{Value: "original", Version: 7, Origin: "origin"}); err != nil {
		t.Fatal(err)
	}
	coordinator := newTestServer(t)
	err := coordinator.putReplica(address, "key", storage.Value{Value: "different", Version: 7, Origin: "origin"})
	if !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("replica write error = %v", err)
	}
	if !coordinator.stats.consistencyFenced.Load() || !receiver.stats.consistencyFenced.Load() {
		t.Fatal("version conflict did not fence both nodes")
	}
}
