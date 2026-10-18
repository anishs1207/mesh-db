package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anishs1207/mesh-db/internal/storage"
)

// Server represents one MeshDB node.
type Server struct {
	store *storage.Store
	peers []string

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

// handleCommand executes a database command.
func (s *Server) handleCommand(line string) string {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "ERR empty command"
	}

	switch strings.ToUpper(parts[0]) {
	case "PING":
		if len(parts) != 1 {
			return "ERR usage: PING"
		}
		return "PONG"

	case "SET":
		if len(parts) != 3 {
			return "ERR usage: SET <key> <value>"
		}

		key, value := parts[1], parts[2]

		record := storage.Value{
			Value:   value,
			Version: s.nextVersion(),
			Origin:  s.nodeID,
		}

		if _, err := s.store.Set(key, record); err != nil {
			return "ERR persistence failure"
		}

		var failed []string
		for _, peer := range s.peers {
			if err := replicate(peer, key, record); err != nil {
				log.Printf("replication to %s failed: %v", peer, err)
				failed = append(failed, peer)
			}
		}

		if len(failed) > 0 {
			return "PARTIAL_OK version=" + strconv.FormatUint(record.Version, 10) +
				"; replication failed on: " + strings.Join(failed, ", ")
		}

		return "OK version=" + strconv.FormatUint(record.Version, 10)

	case "REPLICA_SET":
		if len(parts) != 5 {
			return "ERR usage: REPLICA_SET <key> <version> <origin> <value>"
		}

		version, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return "ERR invalid version"
		}

		record := storage.Value{
			Version: version,
			Origin:  parts[3],
			Value:   parts[4],
		}

		s.observeVersion(version)
		if _, err := s.store.Set(parts[1], record); err != nil {
			return "ERR persistence failure"
		}

		// A replica does not forward the write again.
		return "OK"

	case "GET":
		if len(parts) != 2 {
			return "ERR usage: GET <key>"
		}

		record, ok := s.store.Get(parts[1])
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
		return "ERR unknown command"
	}
}

func handleConnection(conn net.Conn, server *Server) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
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
	flag.Parse()

	var peers []string
	for _, peer := range strings.Split(*peersFlag, ",") {
		peer = strings.TrimSpace(peer)
		if peer != "" {
			peers = append(peers, peer)
		}
	}

	store, err := storage.Open(*dataDir)
	if err != nil {
		log.Fatal("failed to open storage: ", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("failed to close storage: %v", err)
		}
	}()

	server := &Server{
		store:  store,
		peers:  peers,
		nodeID: *port,
		clock:  store.MaxVersion(),
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal("failed to start MeshDB:", err)
	}
	defer listener.Close()

	log.Printf("MeshDB node %s listening on port %s", server.nodeID, *port)
	log.Printf("Configured peers: %v", peers)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed to accept connection:", err)
			continue
		}

		go handleConnection(conn, server)
	}
}
