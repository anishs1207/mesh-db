// Package protocol parses MeshDB's versioned line protocol.
package protocol

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	MaxKeyBytes   = 4 << 10
	MaxValueBytes = 1 << 20
	// MaxCommandBytes includes command syntax, the maximum key and value, and
	// enough authenticated-replica envelope metadata for the same mutation.
	MaxCommandBytes = MaxValueBytes + MaxKeyBytes + 1024
)

type Command uint8

const (
	Ping Command = iota + 1
	Get
	Set
	ReplicaSet
	ReplicaGet
	Delete
	ReplicaDelete
)

type Request struct {
	Command  Command
	Key      string
	Value    string
	Version  uint64
	Origin   string
	Topology string
}

func Parse(line string) (Request, error) {
	if len(line) > MaxCommandBytes {
		return Request{}, fmt.Errorf("command exceeds %d bytes", MaxCommandBytes)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return Request{}, fmt.Errorf("empty command")
	}
	name, rest, _ := strings.Cut(line, " ")
	switch strings.ToUpper(name) {
	case "PING":
		if strings.TrimSpace(rest) != "" {
			return Request{}, fmt.Errorf("usage: PING")
		}
		return Request{Command: Ping}, nil
	case "GET":
		key := strings.TrimSpace(rest)
		if err := validateKey(key); err != nil {
			return Request{}, fmt.Errorf("usage: GET <key>: %w", err)
		}
		if strings.ContainsAny(key, " \t\r\n") {
			return Request{}, fmt.Errorf("usage: GET <key>: key contains whitespace")
		}
		return Request{Command: Get, Key: key}, nil
	case "DELETE":
		key := strings.TrimSpace(rest)
		if err := validateKey(key); err != nil {
			return Request{}, fmt.Errorf("usage: DELETE <key>: %w", err)
		}
		if strings.ContainsAny(key, " \t\r\n") {
			return Request{}, fmt.Errorf("usage: DELETE <key>: key contains whitespace")
		}
		return Request{Command: Delete, Key: key}, nil
	case "SET":
		key, value, ok := cutField(rest)
		if !ok {
			return Request{}, fmt.Errorf("usage: SET <key> <value>")
		}
		if err := validateKey(key); err != nil {
			return Request{}, err
		}
		if err := validateValue(value); err != nil {
			return Request{}, err
		}
		return Request{Command: Set, Key: key, Value: value}, nil
	case "REPLICA_SET":
		fields := strings.SplitN(strings.TrimSpace(rest), " ", 5)
		if len(fields) != 5 {
			return Request{}, fmt.Errorf("usage: REPLICA_SET <topology> <key> <version> <origin> <value>")
		}
		if err := validateTopology(fields[0]); err != nil {
			return Request{}, err
		}
		version, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid version")
		}
		if err := validateKey(fields[1]); err != nil {
			return Request{}, err
		}
		if fields[3] == "" || !utf8.ValidString(fields[3]) || strings.ContainsAny(fields[3], " \t\r\n") {
			return Request{}, fmt.Errorf("invalid origin")
		}
		if err := validateValue(fields[4]); err != nil {
			return Request{}, err
		}
		return Request{Command: ReplicaSet, Topology: fields[0], Key: fields[1], Version: version, Origin: fields[3], Value: fields[4]}, nil
	case "REPLICA_GET":
		fields := strings.Fields(rest)
		if len(fields) != 2 || validateTopology(fields[0]) != nil {
			return Request{}, fmt.Errorf("usage: REPLICA_GET <topology> <key>")
		}
		key := fields[1]
		if err := validateKey(key); err != nil {
			return Request{}, fmt.Errorf("usage: REPLICA_GET <key>: %w", err)
		}
		if strings.ContainsAny(key, " \t\r\n") {
			return Request{}, fmt.Errorf("usage: REPLICA_GET <key>: key contains whitespace")
		}
		return Request{Command: ReplicaGet, Topology: fields[0], Key: key}, nil
	case "REPLICA_DELETE":
		fields := strings.Fields(rest)
		if len(fields) != 4 {
			return Request{}, fmt.Errorf("usage: REPLICA_DELETE <topology> <key> <version> <origin>")
		}
		if err := validateTopology(fields[0]); err != nil {
			return Request{}, err
		}
		version, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid version")
		}
		if err := validateKey(fields[1]); err != nil {
			return Request{}, err
		}
		if fields[3] == "" || !utf8.ValidString(fields[3]) || strings.ContainsAny(fields[3], " \t\r\n") {
			return Request{}, fmt.Errorf("invalid origin")
		}
		return Request{Command: ReplicaDelete, Topology: fields[0], Key: fields[1], Version: version, Origin: fields[3]}, nil
	default:
		return Request{}, fmt.Errorf("unknown command")
	}
}

func validateTopology(topology string) error {
	decoded, err := hex.DecodeString(topology)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("invalid topology fingerprint")
	}
	return nil
}

func cutField(input string) (string, string, bool) {
	input = strings.TrimSpace(input)
	for i, r := range input {
		if r == ' ' || r == '\t' {
			key := input[:i]
			value := strings.TrimSpace(input[i:])
			return key, value, key != "" && value != ""
		}
	}
	return "", "", false
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("key is not valid UTF-8")
	}
	if len(key) > MaxKeyBytes {
		return fmt.Errorf("key exceeds %d bytes", MaxKeyBytes)
	}
	if strings.ContainsAny(key, "\r\n") {
		return fmt.Errorf("key contains a newline")
	}
	return nil
}

func validateValue(value string) error {
	if value == "" {
		return fmt.Errorf("value is empty")
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("value is not valid UTF-8")
	}
	if len(value) > MaxValueBytes {
		return fmt.Errorf("value exceeds %d bytes", MaxValueBytes)
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("value contains a newline")
	}
	return nil
}
