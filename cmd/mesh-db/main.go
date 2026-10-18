package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anishs1207/mesh-db/internal/cluster"
	"github.com/anishs1207/mesh-db/internal/protocol"
	"github.com/anishs1207/mesh-db/internal/storage"
)

// Server represents one MeshDB node.
type Server struct {
	store             *storage.Store
	ring              *cluster.Ring
	self              string
	replicationFactor int
	readQuorum        int
	writeQuorum       int

	// Protects the logical clock shared by concurrent client handlers.
	clockMu sync.Mutex
	clock   uint64
	nodeID  string
}

// nextVersion generates a monotonically increasing local version.
func (s *Server) nextVersion() uint64 {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()

	s.clock++
	return s.clock
}

// observeVersion advances our clock when we receive a replicated write.
// This is similar to the receive step of a Lamport logical clock.
func (s *Server) observeVersion(version uint64) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()

	if version > s.clock {
		s.clock = version
	}
}

// replicate sends a versioned write to a peer.
func replicate(peer, key string, record storage.Value) error {
	conn, err := net.DialTimeout("tcp", peer, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}

	writer := bufio.NewWriter(conn)
	command := fmt.Sprintf(
		"REPLICA_SET %s %d %s %s\n",
		key,
		record.Version,
		record.Origin,
		record.Value,
	)

	if _, err := writer.WriteString(command); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}

	response = strings.TrimSpace(response)
	if response != "OK" {
		return fmt.Errorf("peer returned %q", response)
	}

	return nil
}

type replicaResult struct {
	owner string
	value storage.Value
	found bool
	err   error
}

func newer(incoming, current storage.Value) bool {
	return incoming.Version > current.Version ||
		(incoming.Version == current.Version && incoming.Origin > current.Origin)
}

func (s *Server) owners(key string) []string {
	if s.ring == nil {
		return []string{s.self}
	}
	return s.ring.Owners(key, s.replicationFactor)
}

func (s *Server) putReplica(owner, key string, record storage.Value) error {
	if owner == s.self {
		_, err := s.store.Set(key, record)
		return err
	}
	return replicate(owner, key, record)
}

func fetchReplica(peer, key string) (storage.Value, bool, error) {
	conn, err := net.DialTimeout("tcp", peer, 2*time.Second)
	if err != nil {
		return storage.Value{}, false, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return storage.Value{}, false, err
	}
	if _, err := fmt.Fprintf(conn, "REPLICA_GET %s\n", key); err != nil {
		return storage.Value{}, false, err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return storage.Value{}, false, err
	}
	line = strings.TrimSpace(line)
	if line == "NOT_FOUND" {
		return storage.Value{}, false, nil
	}
	parts := strings.Split(line, " ")
	if len(parts) != 4 || parts[0] != "VALUE" {
		return storage.Value{}, false, fmt.Errorf("invalid replica response %q", line)
	}
	version, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return storage.Value{}, false, fmt.Errorf("invalid replica version: %w", err)
	}
	origin, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return storage.Value{}, false, fmt.Errorf("invalid replica origin: %w", err)
	}
	value, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return storage.Value{}, false, fmt.Errorf("invalid replica value: %w", err)
	}
	return storage.Value{Version: version, Origin: string(origin), Value: string(value)}, true, nil
}

func (s *Server) getReplica(owner, key string) (storage.Value, bool, error) {
	if owner == s.self {
		value, ok := s.store.Get(key)
		return value, ok, nil
	}
	return fetchReplica(owner, key)
}

func (s *Server) quorumWrite(key string, record storage.Value) (int, []string) {
	owners := s.owners(key)
	results := make(chan replicaResult, len(owners))
	for _, owner := range owners {
		go func(owner string) {
			results <- replicaResult{owner: owner, err: s.putReplica(owner, key, record)}
		}(owner)
	}
	acks := 0
	var failed []string
	for range owners {
		result := <-results
		if result.err == nil {
			acks++
		} else {
			log.Printf("replication to %s failed: %v", result.owner, result.err)
			failed = append(failed, result.owner)
		}
	}
	return acks, failed
}

func (s *Server) quorumRead(key string) (storage.Value, bool, int, error) {
	owners := s.owners(key)
	results := make(chan replicaResult, len(owners))
	for _, owner := range owners {
		go func(owner string) {
			value, found, err := s.getReplica(owner, key)
			results <- replicaResult{owner: owner, value: value, found: found, err: err}
		}(owner)
	}

	successes := 0
	var winner storage.Value
	foundWinner := false
	completed := make([]replicaResult, 0, len(owners))
	for range owners {
		result := <-results
		completed = append(completed, result)
		if result.err != nil {
			continue
		}
		successes++
		if result.found && (!foundWinner || newer(result.value, winner)) {
			winner, foundWinner = result.value, true
		}
	}
	if successes < s.readQuorum {
		return storage.Value{}, false, successes, fmt.Errorf("read quorum unavailable")
	}
	if foundWinner {
		for _, result := range completed {
			if result.err == nil && (!result.found || newer(winner, result.value)) {
				if err := s.putReplica(result.owner, key, winner); err != nil {
					log.Printf("read repair to %s failed: %v", result.owner, err)
				}
			}
		}
	}
	return winner, foundWinner, successes, nil
}

// handleCommand executes a database command.
func (s *Server) handleCommand(line string) string {
	request, err := protocol.Parse(line)
	if err != nil {
		return "ERR " + err.Error()
	}

	switch request.Command {
	case protocol.Ping:
		return "PONG"

	case protocol.Set:
		record := storage.Value{
			Value:   request.Value,
			Version: s.nextVersion(),
			Origin:  s.nodeID,
		}

		acks, failed := s.quorumWrite(request.Key, record)
		if acks < s.writeQuorum {
			return fmt.Sprintf("ERR write_quorum_unavailable acknowledgements=%d required=%d failed=%s",
				acks, s.writeQuorum, strings.Join(failed, ","))
		}
		return fmt.Sprintf("OK version=%d acknowledgements=%d", record.Version, acks)

	case protocol.ReplicaSet:
		record := storage.Value{
			Version: request.Version,
			Origin:  request.Origin,
			Value:   request.Value,
		}

		s.observeVersion(request.Version)
		if _, err := s.store.Set(request.Key, record); err != nil {
			return "ERR persistence failure"
		}

		// A replica does not forward the write again.
		return "OK"

	case protocol.ReplicaGet:
		record, ok := s.store.Get(request.Key)
		if !ok {
			return "NOT_FOUND"
		}
		return fmt.Sprintf("VALUE %d %s %s", record.Version,
			base64.RawStdEncoding.EncodeToString([]byte(record.Origin)),
			base64.RawStdEncoding.EncodeToString([]byte(record.Value)))

	case protocol.Get:
		record, ok, responses, err := s.quorumRead(request.Key)
		if err != nil {
			return fmt.Sprintf("ERR read_quorum_unavailable responses=%d required=%d", responses, s.readQuorum)
		}
		if !ok {
			return "NOT_FOUND"
		}

		return fmt.Sprintf(
			"%s (version=%d origin=%s)",
			record.Value,
			record.Version,
			record.Origin,
		)

	default:
		return "ERR unsupported command"
	}
}

func handleConnection(conn net.Conn, server *Server) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64<<10), protocol.MaxCommandBytes)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		response := server.handleCommand(scanner.Text())

		if _, err := fmt.Fprintln(writer, response); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("client read error:", err)
	}
}

func main() {
	port := flag.String("port", "7000", "TCP port to listen on")
	peersFlag := flag.String(
		"peers",
		"",
		"comma-separated peer addresses",
	)
	dataDir := flag.String("data-dir", "./data", "directory for durable node data")
	compactOnStartup := flag.Bool("compact-on-startup", false, "rewrite the WAL with only current values before serving")
	advertise := flag.String("advertise", "", "address advertised to peers (default 127.0.0.1:<port>)")
	replicationFactor := flag.Int("replication-factor", 3, "number of owners for each key")
	readQuorum := flag.Int("read-quorum", 0, "successful replica reads required (default majority)")
	writeQuorum := flag.Int("write-quorum", 0, "successful replica writes required (default majority)")
	flag.Parse()

	var peers []string
	for _, peer := range strings.Split(*peersFlag, ",") {
		peer = strings.TrimSpace(peer)
		if peer != "" {
			peers = append(peers, peer)
		}
	}
	if *advertise == "" {
		*advertise = "127.0.0.1:" + *port
	}
	members := append([]string{*advertise}, peers...)
	ring := cluster.NewRing(members, cluster.DefaultVirtualNodes)
	effectiveReplication := *replicationFactor
	if effectiveReplication <= 0 {
		log.Fatal("replication-factor must be positive")
	}
	if effectiveReplication > len(ring.Nodes()) {
		effectiveReplication = len(ring.Nodes())
	}
	majority := effectiveReplication/2 + 1
	if *readQuorum == 0 {
		*readQuorum = majority
	}
	if *writeQuorum == 0 {
		*writeQuorum = majority
	}
	if *readQuorum < 1 || *writeQuorum < 1 || *readQuorum > effectiveReplication || *writeQuorum > effectiveReplication {
		log.Fatalf("quorums must be between 1 and effective replication factor %d", effectiveReplication)
	}
	if *readQuorum+*writeQuorum <= effectiveReplication {
		log.Fatalf("read-quorum + write-quorum must exceed replication factor (%d + %d <= %d)",
			*readQuorum, *writeQuorum, effectiveReplication)
	}

	store, err := storage.Open(*dataDir)
	if err != nil {
		log.Fatal("failed to open storage: ", err)
	}
	if *compactOnStartup {
		if err := store.Compact(); err != nil {
			_ = store.Close()
			log.Fatal("failed to compact storage: ", err)
		}
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("failed to close storage: %v", err)
		}
	}()

	server := &Server{
		store:             store,
		ring:              ring,
		self:              *advertise,
		replicationFactor: effectiveReplication,
		readQuorum:        *readQuorum,
		writeQuorum:       *writeQuorum,
		nodeID:            *advertise,
		clock:             store.MaxVersion(),
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal("failed to start MeshDB:", err)
	}
	defer listener.Close()

	log.Printf("MeshDB node %s listening on port %s", server.nodeID, *port)
	log.Printf("Configured peers: %v", peers)
	log.Printf("Replication N=%d R=%d W=%d", effectiveReplication, *readQuorum, *writeQuorum)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed to accept connection:", err)
			continue
		}

		go handleConnection(conn, server)
	}
}
