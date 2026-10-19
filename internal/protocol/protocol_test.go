package protocol

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	topology := strings.Repeat("a", 64)
	tests := []struct {
		line  string
		want  Request
		fails bool
	}{
		{"PING", Request{Command: Ping}, false},
		{"GET user:1", Request{Command: Get, Key: "user:1"}, false},
		{"DELETE user:1", Request{Command: Delete, Key: "user:1"}, false},
		{"SET greeting hello distributed world", Request{Command: Set, Key: "greeting", Value: "hello distributed world"}, false},
		{"REPLICA_SET " + topology + " greeting 42 node-a hello world", Request{Command: ReplicaSet, Topology: topology, Key: "greeting", Version: 42, Origin: "node-a", Value: "hello world"}, false},
		{"REPLICA_GET " + topology + " greeting", Request{Command: ReplicaGet, Topology: topology, Key: "greeting"}, false},
		{"REPLICA_DELETE " + topology + " greeting 43 node-a", Request{Command: ReplicaDelete, Topology: topology, Key: "greeting", Version: 43, Origin: "node-a"}, false},
		{"", Request{}, true},
		{"PING extra", Request{}, true},
		{"GET", Request{}, true},
		{"SET key", Request{}, true},
		{"REPLICA_SET " + topology + " key nope node value", Request{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			got, err := Parse(tt.line)
			if (err != nil) != tt.fails {
				t.Fatalf("Parse() error = %v, fails = %v", err, tt.fails)
			}
			if !tt.fails && got != tt.want {
				t.Fatalf("Parse() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRejectsOversizedInput(t *testing.T) {
	if _, err := Parse("SET key " + strings.Repeat("x", MaxValueBytes+1)); err == nil {
		t.Fatal("expected oversized value to fail")
	}
	if _, err := Parse("PING" + strings.Repeat(" ", MaxCommandBytes)); err == nil {
		t.Fatal("expected oversized command frame to fail")
	}
	if request, err := Parse("SET key " + strings.Repeat("x", MaxValueBytes)); err != nil || len(request.Value) != MaxValueBytes {
		t.Fatalf("maximum value rejected: value=%d err=%v", len(request.Value), err)
	}
}

func TestRejectsInvalidUTF8(t *testing.T) {
	for name, command := range map[string]string{
		"key":    "GET " + string([]byte{0xff}),
		"value":  "SET key " + string([]byte{0xff}),
		"origin": "REPLICA_DELETE " + strings.Repeat("a", 64) + " key 1 " + string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(command); err == nil {
				t.Fatal("invalid UTF-8 command was accepted")
			}
		})
	}
}
