package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/anishs1207/mesh-db/internal/cluster"
	hintstore "github.com/anishs1207/mesh-db/internal/hints"
	"github.com/anishs1207/mesh-db/internal/identity"
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
	readTimeout       time.Duration
	writeTimeout      time.Duration
	stats             serverStats
	clusterSecret     []byte
	tlsConfig         *tls.Config
	hints             *hintstore.Store
	hintBatchSize     int
	replicaPut        func(string, string, storage.Value) error
	replicaGet        func(string, string) (storage.Value, bool, error)
	backgroundMu      sync.Mutex
	background        sync.WaitGroup
	backgroundClosing bool

	// Protects the logical clock shared by concurrent client handlers.
	clockMu sync.Mutex
	clock   uint64
	nodeID  string
}

func (s *Server) startBackground(work func()) bool {
	s.backgroundMu.Lock()
	defer s.backgroundMu.Unlock()
	if s.backgroundClosing {
		return false
	}
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		work()
	}()
	return true
}

func (s *Server) stopBackground() {
	s.backgroundMu.Lock()
	s.backgroundClosing = true
	s.backgroundMu.Unlock()
	s.background.Wait()
}

type serverStats struct {
	commands           atomic.Uint64
	commandErrors      atomic.Uint64
	quorumFailures     atomic.Uint64
	activeConnections  atomic.Int64
	totalConnections   atomic.Uint64
	ready              atomic.Bool
	repairScans        atomic.Uint64
	repairKeys         atomic.Uint64
	repairFailures     atomic.Uint64
	hintsQueued        atomic.Uint64
	hintsDelivered     atomic.Uint64
	compactions        atomic.Uint64
	compactionFailures atomic.Uint64
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

func signInternal(command string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(command))
	return hex.EncodeToString(mac.Sum(nil))
}

func authenticatedCommand(command string, secret []byte) string {
	return "AUTH " + signInternal(command, secret) + " " + command
}

func verifyInternal(signature, command string, secret []byte) bool {
	provided, err := hex.DecodeString(signature)
	if err != nil || len(secret) == 0 {
		return false
	}
	expected, err := hex.DecodeString(signInternal(command, secret))
	return err == nil && hmac.Equal(provided, expected)
}

// replicate sends an authenticated versioned write to a peer.
func dialPeer(peer string, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	if tlsConfig == nil {
		return dialer.Dial("tcp", peer)
	}
	config := tlsConfig.Clone()
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		return nil, fmt.Errorf("invalid peer address: %w", err)
	}
	config.ServerName = host
	return tls.DialWithDialer(dialer, "tcp", peer, config)
}

func replicate(peer, key string, record storage.Value, secret []byte, tlsConfig *tls.Config) error {
	conn, err := dialPeer(peer, tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}

	writer := bufio.NewWriter(conn)
	var command string
	if record.Tombstone {
		command = fmt.Sprintf("REPLICA_DELETE %s %d %s", key, record.Version, record.Origin)
	} else {
		command = fmt.Sprintf("REPLICA_SET %s %d %s %s", key, record.Version, record.Origin, record.Value)
	}
	command = authenticatedCommand(command, secret) + "\n"

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
	if s.replicaPut != nil {
		return s.replicaPut(owner, key, record)
	}
	if owner == s.self {
		applied, err := s.store.Set(key, record)
		if err != nil {
			return err
		}
		if !applied {
			current, ok := s.store.Get(key)
			if !ok || current != record {
				return fmt.Errorf("replica rejected stale write")
			}
		}
		return nil
	}
	return replicate(owner, key, record, s.clusterSecret, s.tlsConfig)
}

func fetchReplica(peer, key string, secret []byte, tlsConfig *tls.Config) (storage.Value, bool, error) {
	conn, err := dialPeer(peer, tlsConfig)
	if err != nil {
		return storage.Value{}, false, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return storage.Value{}, false, err
	}
	command := "REPLICA_GET " + key
	if _, err := fmt.Fprintf(conn, "%s\n", authenticatedCommand(command, secret)); err != nil {
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
	if len(parts) != 5 || parts[0] != "VALUE" {
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
	tombstone := parts[3] == "1"
	if parts[3] != "0" && !tombstone {
		return storage.Value{}, false, fmt.Errorf("invalid replica tombstone %q", parts[3])
	}
	var value []byte
	if parts[4] != "-" {
		value, err = base64.RawStdEncoding.DecodeString(parts[4])
		if err != nil {
			return storage.Value{}, false, fmt.Errorf("invalid replica value: %w", err)
		}
	}
	return storage.Value{Version: version, Origin: string(origin), Value: string(value), Tombstone: tombstone}, true, nil
}

func (s *Server) getReplica(owner, key string) (storage.Value, bool, error) {
	if s.replicaGet != nil {
		return s.replicaGet(owner, key)
	}
	if owner == s.self {
		value, ok := s.store.Get(key)
		return value, ok, nil
	}
	return fetchReplica(owner, key, s.clusterSecret, s.tlsConfig)
}

func loadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("tls-cert, tls-key, and tls-ca must be configured together")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("TLS CA contains no valid certificates")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func (s *Server) quorumWrite(key string, record storage.Value) (int, []string) {
	owners := s.owners(key)
	results := make(chan replicaResult, len(owners))
	for _, owner := range owners {
		owner := owner
		if !s.startBackground(func() {
			results <- replicaResult{owner: owner, err: s.putReplica(owner, key, record)}
		}) {
			results <- replicaResult{owner: owner, err: fmt.Errorf("server shutting down")}
		}
	}
	acks := 0
	var failed []string
	for completed := 0; completed < len(owners); completed++ {
		result := <-results
		if result.err == nil {
			acks++
		} else {
			log.Printf("replication to %s failed: %v", result.owner, result.err)
			failed = append(failed, result.owner)
			if s.hints != nil {
				if err := s.hints.Put(result.owner, key, record); err != nil {
					log.Printf("persist hint for %s failed: %v", result.owner, err)
				} else {
					s.stats.hintsQueued.Add(1)
				}
			}
		}
		if acks >= s.writeQuorum {
			remaining := len(owners) - completed - 1
			if remaining > 0 {
				if !s.startBackground(func() { s.drainWriteResults(results, remaining, key, record) }) {
					s.drainWriteResults(results, remaining, key, record)
				}
			}
			return acks, failed
		}
	}
	return acks, failed
}

// drainWriteResults finishes deliveries after the client-visible quorum has
// completed. A buffered result channel prevents the replica goroutines from
// depending on this worker's scheduling, while durable hints preserve failed
// deliveries for retry.
func (s *Server) drainWriteResults(results <-chan replicaResult, remaining int, key string, record storage.Value) {
	for range remaining {
		result := <-results
		if result.err == nil {
			continue
		}
		log.Printf("replication to %s failed after quorum: %v", result.owner, result.err)
		if s.hints == nil {
			continue
		}
		if err := s.hints.Put(result.owner, key, record); err != nil {
			log.Printf("persist hint for %s failed: %v", result.owner, err)
		} else {
			s.stats.hintsQueued.Add(1)
		}
	}
}

func (s *Server) deliverHints() {
	s.deliverHintsContext(context.Background())
}

func (s *Server) deliverHintsContext(ctx context.Context) {
	if s.hints == nil {
		return
	}
	items, err := s.hints.ListLimit(s.hintBatchSize)
	if err != nil {
		log.Printf("some hints could not be loaded: %v", err)
	}
	for _, hint := range items {
		if ctx.Err() != nil {
			return
		}
		if err := s.putReplica(hint.Owner, hint.Key, hint.Value); err != nil {
			continue
		}
		if err := s.hints.Delete(hint.ID); err != nil {
			log.Printf("delete delivered hint failed: %v", err)
			continue
		}
		s.stats.hintsDelivered.Add(1)
	}
}

func (s *Server) quorumRead(key string) (storage.Value, bool, int, error) {
	owners := s.owners(key)
	results := make(chan replicaResult, len(owners))
	for _, owner := range owners {
		owner := owner
		if !s.startBackground(func() {
			value, found, err := s.getReplica(owner, key)
			results <- replicaResult{owner: owner, value: value, found: found, err: err}
		}) {
			results <- replicaResult{owner: owner, err: fmt.Errorf("server shutting down")}
		}
	}

	successes := 0
	var winner storage.Value
	foundWinner := false
	allAgree := true
	var firstFound bool
	var firstValue storage.Value
	haveSuccessfulResult := false
	completed := make([]replicaResult, 0, len(owners))
	for completedCount := 0; completedCount < len(owners); completedCount++ {
		result := <-results
		completed = append(completed, result)
		if result.err != nil {
			continue
		}
		successes++
		if !haveSuccessfulResult {
			firstFound, firstValue, haveSuccessfulResult = result.found, result.value, true
		} else if result.found != firstFound || (result.found && result.value != firstValue) {
			allAgree = false
		}
		if result.found && (!foundWinner || newer(result.value, winner)) {
			winner, foundWinner = result.value, true
		}
		// An agreeing quorum can return immediately. When replicas disagree, wait
		// for every reachable response so a minority carrying a newer committed
		// value or tombstone is not ignored during reconciliation.
		if successes >= s.readQuorum && allAgree {
			remaining := len(owners) - completedCount - 1
			if remaining == 0 {
				s.finishReadRepair(results, remaining, completed, key, winner, foundWinner)
			} else {
				if !s.startBackground(func() {
					s.finishReadRepair(results, remaining, completed, key, winner, foundWinner)
				}) {
					s.finishReadRepair(results, remaining, completed, key, winner, foundWinner)
				}
			}
			return winner, foundWinner, successes, nil
		}
	}
	if successes < s.readQuorum {
		return storage.Value{}, false, successes, fmt.Errorf("read quorum unavailable")
	}
	s.finishReadRepair(results, 0, completed, key, winner, foundWinner)
	return winner, foundWinner, successes, nil
}

func (s *Server) finishReadRepair(results <-chan replicaResult, remaining int, completed []replicaResult, key string, winner storage.Value, foundWinner bool) {
	for range remaining {
		result := <-results
		completed = append(completed, result)
		if result.err == nil && result.found && (!foundWinner || newer(result.value, winner)) {
			winner, foundWinner = result.value, true
		}
	}
	if !foundWinner {
		return
	}
	for _, result := range completed {
		if result.err == nil && (!result.found || newer(winner, result.value)) {
			if err := s.putReplica(result.owner, key, winner); err != nil {
				log.Printf("read repair to %s failed: %v", result.owner, err)
			}
		}
	}
}

func (s *Server) repairOnce() {
	s.repairOnceContext(context.Background())
}

func (s *Server) repairOnceContext(ctx context.Context) {
	s.stats.repairScans.Add(1)
	for key := range s.store.Entries() {
		if ctx.Err() != nil {
			return
		}
		if _, _, _, err := s.quorumRead(key); err != nil {
			s.stats.repairFailures.Add(1)
			continue
		}
		s.stats.repairKeys.Add(1)
	}
}

// handleCommand executes a database command.
func (s *Server) handleCommand(line string) string {
	s.stats.commands.Add(1)
	authorizedInternal := false
	if strings.HasPrefix(line, "AUTH ") {
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 || !verifyInternal(parts[1], parts[2], s.clusterSecret) {
			s.stats.commandErrors.Add(1)
			return "ERR unauthorized"
		}
		authorizedInternal = true
		line = parts[2]
	}
	request, err := protocol.Parse(line)
	if err != nil {
		s.stats.commandErrors.Add(1)
		return "ERR " + err.Error()
	}

	switch request.Command {
	case protocol.Ping:
		return "PONG"

	case protocol.Set:
		observed, _, responses, err := s.quorumRead(request.Key)
		if err != nil {
			s.stats.quorumFailures.Add(1)
			return fmt.Sprintf("ERR read_quorum_unavailable responses=%d required=%d", responses, s.readQuorum)
		}
		s.observeVersion(observed.Version)
		record := storage.Value{
			Value:   request.Value,
			Version: s.nextVersion(),
			Origin:  s.nodeID,
		}

		acks, failed := s.quorumWrite(request.Key, record)
		if acks < s.writeQuorum {
			s.stats.quorumFailures.Add(1)
			return fmt.Sprintf("ERR write_quorum_unavailable acknowledgements=%d required=%d failed=%s",
				acks, s.writeQuorum, strings.Join(failed, ","))
		}
		return fmt.Sprintf("OK version=%d acknowledgements=%d", record.Version, acks)

	case protocol.Delete:
		observed, _, responses, err := s.quorumRead(request.Key)
		if err != nil {
			s.stats.quorumFailures.Add(1)
			return fmt.Sprintf("ERR read_quorum_unavailable responses=%d required=%d", responses, s.readQuorum)
		}
		s.observeVersion(observed.Version)
		record := storage.Value{Version: s.nextVersion(), Origin: s.nodeID, Tombstone: true}
		acks, failed := s.quorumWrite(request.Key, record)
		if acks < s.writeQuorum {
			s.stats.quorumFailures.Add(1)
			return fmt.Sprintf("ERR write_quorum_unavailable acknowledgements=%d required=%d failed=%s",
				acks, s.writeQuorum, strings.Join(failed, ","))
		}
		return fmt.Sprintf("OK version=%d acknowledgements=%d", record.Version, acks)

	case protocol.ReplicaSet:
		if !authorizedInternal {
			s.stats.commandErrors.Add(1)
			return "ERR unauthorized"
		}
		record := storage.Value{
			Version: request.Version,
			Origin:  request.Origin,
			Value:   request.Value,
		}

		s.observeVersion(request.Version)
		applied, err := s.store.Set(request.Key, record)
		if err != nil {
			return "ERR persistence failure"
		}
		if !applied {
			current, ok := s.store.Get(request.Key)
			if !ok || current != record {
				return "STALE"
			}
		}

		// A replica does not forward the write again.
		return "OK"

	case protocol.ReplicaDelete:
		if !authorizedInternal {
			s.stats.commandErrors.Add(1)
			return "ERR unauthorized"
		}
		record := storage.Value{Version: request.Version, Origin: request.Origin, Tombstone: true}
		s.observeVersion(request.Version)
		applied, err := s.store.Set(request.Key, record)
		if err != nil {
			return "ERR persistence failure"
		}
		if !applied {
			current, ok := s.store.Get(request.Key)
			if !ok || current != record {
				return "STALE"
			}
		}
		return "OK"

	case protocol.ReplicaGet:
		if !authorizedInternal {
			s.stats.commandErrors.Add(1)
			return "ERR unauthorized"
		}
		record, ok := s.store.Get(request.Key)
		if !ok {
			return "NOT_FOUND"
		}
		encodedValue := base64.RawStdEncoding.EncodeToString([]byte(record.Value))
		if encodedValue == "" {
			encodedValue = "-"
		}
		tombstone := 0
		if record.Tombstone {
			tombstone = 1
		}
		return fmt.Sprintf("VALUE %d %s %d %s", record.Version,
			base64.RawStdEncoding.EncodeToString([]byte(record.Origin)),
			tombstone, encodedValue)

	case protocol.Get:
		record, ok, responses, err := s.quorumRead(request.Key)
		if err != nil {
			s.stats.quorumFailures.Add(1)
			return fmt.Sprintf("ERR read_quorum_unavailable responses=%d required=%d", responses, s.readQuorum)
		}
		if !ok || record.Tombstone {
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
	server.stats.activeConnections.Add(1)
	server.stats.totalConnections.Add(1)
	defer server.stats.activeConnections.Add(-1)

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64<<10), protocol.MaxCommandBytes)
	writer := bufio.NewWriter(conn)

	for {
		if server.readTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(server.readTimeout))
		}
		if !scanner.Scan() {
			break
		}
		response := server.handleCommand(scanner.Text())

		if server.writeTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(server.writeTimeout))
		}
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

func (s *Server) metrics() string {
	ready := 0
	if s.stats.ready.Load() {
		ready = 1
	}
	hintBytes, hintMaxBytes := int64(0), int64(0)
	if s.hints != nil {
		hintBytes, hintMaxBytes = s.hints.Usage()
	}
	return fmt.Sprintf(`# HELP meshdb_ready Whether this node is ready to serve traffic.
# TYPE meshdb_ready gauge
meshdb_ready %d
# HELP meshdb_active_connections Current client connections.
# TYPE meshdb_active_connections gauge
meshdb_active_connections %d
# HELP meshdb_connections_total Accepted client connections.
# TYPE meshdb_connections_total counter
meshdb_connections_total %d
# HELP meshdb_commands_total Commands processed.
# TYPE meshdb_commands_total counter
meshdb_commands_total %d
# HELP meshdb_command_errors_total Invalid or failed commands.
# TYPE meshdb_command_errors_total counter
meshdb_command_errors_total %d
# HELP meshdb_quorum_failures_total Operations that did not reach quorum.
# TYPE meshdb_quorum_failures_total counter
meshdb_quorum_failures_total %d
# HELP meshdb_repair_scans_total Anti-entropy scans started.
# TYPE meshdb_repair_scans_total counter
meshdb_repair_scans_total %d
# HELP meshdb_repair_keys_total Keys reconciled by anti-entropy.
# TYPE meshdb_repair_keys_total counter
meshdb_repair_keys_total %d
# HELP meshdb_repair_failures_total Keys that could not reach a read quorum during repair.
# TYPE meshdb_repair_failures_total counter
meshdb_repair_failures_total %d
# HELP meshdb_hints_queued_total Replica deliveries persisted for retry.
# TYPE meshdb_hints_queued_total counter
meshdb_hints_queued_total %d
# HELP meshdb_hints_delivered_total Persisted replica deliveries completed.
# TYPE meshdb_hints_delivered_total counter
meshdb_hints_delivered_total %d
# HELP meshdb_hint_bytes Bytes consumed by pending hints.
# TYPE meshdb_hint_bytes gauge
meshdb_hint_bytes %d
# HELP meshdb_hint_max_bytes Configured hint storage budget.
# TYPE meshdb_hint_max_bytes gauge
meshdb_hint_max_bytes %d
%s
`, ready, s.stats.activeConnections.Load(), s.stats.totalConnections.Load(),
		s.stats.commands.Load(), s.stats.commandErrors.Load(), s.stats.quorumFailures.Load(),
		s.stats.repairScans.Load(), s.stats.repairKeys.Load(), s.stats.repairFailures.Load(),
		s.stats.hintsQueued.Load(), s.stats.hintsDelivered.Load(), hintBytes, hintMaxBytes,
		s.storageMetrics())
}

func (s *Server) storageMetrics() string {
	walBytes, walRecords, liveKeys, err := s.store.WALStats()
	if err != nil {
		walBytes = -1
	}
	return fmt.Sprintf(`# HELP meshdb_wal_bytes Current write-ahead log size in bytes.
# TYPE meshdb_wal_bytes gauge
meshdb_wal_bytes %d
# HELP meshdb_wal_records Records currently stored in the write-ahead log.
# TYPE meshdb_wal_records gauge
meshdb_wal_records %d
# HELP meshdb_live_keys Keys retained by the local storage engine, including tombstones.
# TYPE meshdb_live_keys gauge
meshdb_live_keys %d
# HELP meshdb_compactions_total Successful online WAL compactions.
# TYPE meshdb_compactions_total counter
meshdb_compactions_total %d
# HELP meshdb_compaction_failures_total Failed online WAL compactions.
# TYPE meshdb_compaction_failures_total counter
meshdb_compaction_failures_total %d`, walBytes, walRecords, liveKeys,
		s.stats.compactions.Load(), s.stats.compactionFailures.Load())
}

func newOperationsServer(address string, server *Server) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !server.stats.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(server.metrics()))
	})
	return &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
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
	backupOnStartup := flag.String("backup-on-startup", "", "write a compact backup WAL to this new path before serving")
	advertise := flag.String("advertise", "", "address advertised to peers (default 127.0.0.1:<port>)")
	replicationFactor := flag.Int("replication-factor", 3, "number of owners for each key")
	readQuorum := flag.Int("read-quorum", 0, "successful replica reads required (default majority)")
	writeQuorum := flag.Int("write-quorum", 0, "successful replica writes required (default majority)")
	maxConnections := flag.Int("max-connections", 1024, "maximum simultaneous client connections")
	readTimeout := flag.Duration("read-timeout", 30*time.Second, "maximum idle time waiting for a command")
	writeTimeout := flag.Duration("write-timeout", 10*time.Second, "maximum time writing a response")
	shutdownTimeout := flag.Duration("shutdown-timeout", 15*time.Second, "graceful shutdown deadline")
	operationsAddress := flag.String("operations-address", "", "health and metrics HTTP address (for example 127.0.0.1:9090)")
	clusterSecretFile := flag.String("cluster-secret-file", "", "path to the shared inter-node authentication secret")
	repairInterval := flag.Duration("repair-interval", 30*time.Second, "interval between anti-entropy scans (0 disables)")
	hintInterval := flag.Duration("hint-interval", 5*time.Second, "interval between hinted-handoff retries (0 disables)")
	hintMaxBytes := flag.Int64("hint-max-bytes", 1<<30, "maximum disk bytes for pending hinted handoff")
	hintBatchSize := flag.Int("hint-batch-size", 256, "maximum hinted handoff records attempted per retry pass")
	compactionInterval := flag.Duration("compaction-interval", 5*time.Minute, "interval for checking WAL compaction eligibility (0 disables)")
	compactionMinBytes := flag.Int64("compaction-min-bytes", 256<<20, "minimum WAL size before automatic compaction")
	compactionStaleRatio := flag.Float64("compaction-stale-ratio", 2, "minimum WAL records per live key before automatic compaction")
	tlsCertFile := flag.String("tls-cert", "", "PEM certificate for mutually authenticated TLS")
	tlsKeyFile := flag.String("tls-key", "", "PEM private key for mutually authenticated TLS")
	tlsCAFile := flag.String("tls-ca", "", "PEM CA used to verify clients and peers")
	flag.Parse()
	if *maxConnections <= 0 || *readTimeout <= 0 || *writeTimeout <= 0 || *shutdownTimeout <= 0 || *repairInterval < 0 || *hintInterval < 0 || *hintMaxBytes <= 0 || *hintBatchSize <= 0 || *compactionInterval < 0 || *compactionMinBytes <= 0 || *compactionStaleRatio < 1 {
		log.Fatal("connection limit and timeouts must be positive")
	}

	var peers []string
	for _, peer := range strings.Split(*peersFlag, ",") {
		peer = strings.TrimSpace(peer)
		if peer != "" {
			peers = append(peers, peer)
		}
	}
	var clusterSecret []byte
	if *clusterSecretFile != "" {
		contents, err := os.ReadFile(*clusterSecretFile)
		if err != nil {
			log.Fatal("failed to read cluster secret: ", err)
		}
		clusterSecret = []byte(strings.TrimSpace(string(contents)))
		if len(clusterSecret) < 32 {
			log.Fatal("cluster secret must contain at least 32 bytes")
		}
	}
	if len(peers) > 0 && len(clusterSecret) == 0 {
		log.Fatal("cluster-secret-file is required when peers are configured")
	}
	tlsConfig, err := loadTLSConfig(*tlsCertFile, *tlsKeyFile, *tlsCAFile)
	if err != nil {
		log.Fatal("failed to configure TLS: ", err)
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
	hints, err := hintstore.Open(filepath.Join(*dataDir, "hints"))
	if err != nil {
		_ = store.Close()
		log.Fatal("failed to open hinted handoff: ", err)
	}
	hints.SetMaxBytes(*hintMaxBytes)
	nodeID, err := identity.LoadOrCreate(*dataDir)
	if err != nil {
		_ = store.Close()
		log.Fatal("failed to load node identity: ", err)
	}
	if *compactOnStartup {
		if err := store.Compact(); err != nil {
			_ = store.Close()
			log.Fatal("failed to compact storage: ", err)
		}
	}
	if *backupOnStartup != "" {
		if err := store.Backup(*backupOnStartup); err != nil {
			_ = store.Close()
			log.Fatal("failed to create backup: ", err)
		}
		log.Printf("Backup written to %s", *backupOnStartup)
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
		nodeID:            nodeID,
		clock:             store.MaxVersion(),
		readTimeout:       *readTimeout,
		writeTimeout:      *writeTimeout,
		clusterSecret:     clusterSecret,
		tlsConfig:         tlsConfig,
		hints:             hints,
		hintBatchSize:     *hintBatchSize,
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal("failed to start MeshDB:", err)
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	log.Printf("MeshDB node %s listening on port %s (advertise %s)", server.nodeID, *port, *advertise)
	log.Printf("Configured peers: %v", peers)
	log.Printf("Replication N=%d R=%d W=%d", effectiveReplication, *readQuorum, *writeQuorum)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var operations *http.Server
	if *operationsAddress != "" {
		operations = newOperationsServer(*operationsAddress, server)
		go func() {
			log.Printf("Operations endpoint listening on %s", *operationsAddress)
			if err := operations.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("operations endpoint failed: %v", err)
				stop()
			}
		}()
	}

	server.stats.ready.Store(true)
	var maintenance sync.WaitGroup
	if *repairInterval > 0 {
		maintenance.Add(1)
		go func() {
			defer maintenance.Done()
			ticker := time.NewTicker(*repairInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					server.repairOnceContext(ctx)
				}
			}
		}()
	}
	if *hintInterval > 0 {
		maintenance.Add(1)
		go func() {
			defer maintenance.Done()
			ticker := time.NewTicker(*hintInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					server.deliverHintsContext(ctx)
				}
			}
		}()
	}
	if *compactionInterval > 0 {
		maintenance.Add(1)
		go func() {
			defer maintenance.Done()
			ticker := time.NewTicker(*compactionInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					needed, err := store.NeedsCompaction(*compactionMinBytes, *compactionStaleRatio)
					if err != nil {
						server.stats.compactionFailures.Add(1)
						log.Printf("inspect WAL for compaction: %v", err)
						continue
					}
					if !needed {
						continue
					}
					if err := store.Compact(); err != nil {
						server.stats.compactionFailures.Add(1)
						log.Printf("automatic WAL compaction failed: %v", err)
						continue
					}
					server.stats.compactions.Add(1)
				}
			}
		}()
	}
	semaphore := make(chan struct{}, *maxConnections)
	var connections sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Println("failed to accept connection:", err)
			continue
		}
		select {
		case semaphore <- struct{}{}:
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer func() { <-semaphore }()
				handleConnection(conn, server)
			}()
		default:
			_, _ = fmt.Fprintln(conn, "ERR server_busy")
			_ = conn.Close()
		}
	}

	server.stats.ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if operations != nil {
		if err := operations.Shutdown(shutdownCtx); err != nil {
			log.Printf("operations shutdown: %v", err)
		}
	}
	done := make(chan struct{})
	go func() {
		connections.Wait()
		maintenance.Wait()
		server.stopBackground()
		close(done)
	}()
	select {
	case <-done:
		log.Print("MeshDB shutdown complete")
	case <-shutdownCtx.Done():
		log.Print("MeshDB shutdown deadline reached")
	}
}
