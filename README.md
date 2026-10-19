# MeshDB

MeshDB is an educational distributed key-value database built in deliberate,
runnable stages. Each level adds one production concern, tests it, documents
the trade-offs, and lands as a separate Git commit.

The current implementation is intentionally small. Do not use it for
production data until the production-readiness checklist in
[`prompts/README.md`](prompts/README.md) is complete.

## Current commands

```text
PING
SET <key> <value>
GET <key>
DELETE <key>
```

Start one node with durable local data:

```bash
go run ./cmd/mesh-db -port 7000 -data-dir ./data/node-7000
```

Each data directory is protected by an OS-backed exclusive lock for the full
storage lifetime. A second process fails startup instead of concurrently
appending to the same WAL. The operating system releases the lock after a crash;
the `.meshdb.lock` file itself is persistent and must not be used as a stale-lock
signal.
First-boot node identity is written and synced in a private temporary inode,
then atomically linked into place. Concurrent creators converge on the same
complete identity, and a crash cannot publish a partially written `node.id`.

Start a replicated three-node demonstration:

```bash
go run ./cmd/mesh-db -port 7000 -peers 127.0.0.1:7001,127.0.0.1:7002
go run ./cmd/mesh-db -port 7001 -peers 127.0.0.1:7000,127.0.0.1:7002
go run ./cmd/mesh-db -port 7002 -peers 127.0.0.1:7000,127.0.0.1:7001
```

Multi-node clusters require `-cluster-secret-file` on every node. The file must
contain the same randomly generated secret of at least 32 bytes. Never commit
that file; local secret files are covered by the `.env` ignore rules when named
accordingly.

For non-loopback deployments, enable mutually authenticated TLS 1.3 with
`-tls-cert`, `-tls-key`, and `-tls-ca`. The same CA must trust every client and
node certificate, and peer certificate DNS/IP names must match advertised
addresses. Partial TLS configuration is rejected. MeshDB binds to loopback by
default and rejects plaintext non-loopback `-listen-address` values unless the
operator deliberately supplies `-allow-insecure-listen` (intended only for
isolated development networks).

Each key is placed on a consistent-hash ring. Clusters with at least three
members default to replication factor 3 with majority read and write quorums. Override them with
`-replication-factor`, `-read-quorum`, and `-write-quorum`; MeshDB rejects
configurations where R + W does not intersect.
The replication-factor default of zero means “automatically use up to three”;
any positive value is strict and startup fails rather than silently reducing it
when membership is too small. Duplicate and malformed member addresses are
also rejected.

Authenticated replica traffic is fenced by a fingerprint covering membership,
virtual-node count, replication factor, and both quorums. Differently configured
nodes reject each other before storage mutation; compare startup logs or
`meshdb_topology_info` when diagnosing a cluster. The topology-fencing protocol
requires a coordinated restart when upgrading from older builds.
Every internal request also carries a signed timestamp and 128-bit random nonce.
Receivers enforce a 30-second clock-skew window and reject a nonce after its
first valid use, preventing captured replica commands from being replayed. Keep
node clocks synchronized; this authentication-envelope change also requires a
coordinated restart from older builds.
Receivers also recompute placement and reject replica operations for keys they
do not own, making routing mistakes visible instead of polluting non-owner
storage.

Writes return after the configured quorum acknowledges them; late failures are
captured by hinted handoff. Reads return immediately when a quorum agrees, but
wait for reconciliation when replicas disagree so newer values and tombstones
are not hidden by the fast path.
Logical versions are overflow-fenced: if the 64-bit Lamport space is exhausted,
new client mutations fail with `ERR version_space_exhausted` instead of wrapping
to zero and being silently treated as stale.

Use `-compact-on-startup` during a controlled restart to rewrite an oversized
WAL with only the latest value for each key.

WAL records are checksummed and explicitly format-versioned. Startup accepts
legacy records created before format markers, but rejects unknown fields and
future formats rather than silently corrupting data during an unsafe downgrade.
Checksummed records must also satisfy storage invariants: keys and origins are
bounded and whitespace-free, versions are nonzero, values are bounded, and
tombstones cannot carry a value. The same validation runs before live append
and during recovery, so a valid CRC cannot legitimize malformed state.
Replica responses and hinted-handoff records pass through that same validator
before quorum selection or retry, preventing malformed remote state from
winning reconciliation or occupying a permanent repair slot.

MeshDB also checks for online compaction every five minutes. By default it
compacts only after the WAL reaches 256 MiB and contains at least twice as many
records as retained keys. Tune this with `-compaction-interval`,
`-compaction-min-bytes`, and `-compaction-stale-ratio`; set the interval to zero
to keep compaction under external operational control.

The active WAL is capped at 10 GiB by default with `-wal-max-bytes`. Mutations
that would cross the exact framed-byte limit fail before append, while
compaction remains available to reclaim obsolete records. Current usage, the
limit, and capacity rejections are exported as metrics.

Use `-backup-on-startup /secure/path/backup.wal` during a controlled restart to
create a compact, checksummed backup. The destination must be new. To restore,
place the backup at `<new-data-dir>/meshdb.wal` before starting a node; always
restore into an empty data directory and retain an off-host copy.

Enable operational endpoints with `-operations-address 127.0.0.1:9090`:

- `/healthz` reports that the process is alive.
- `/readyz` reports whether the node is accepting database traffic.
- `/metrics` exports Prometheus-compatible counters and gauges.

The operations listener is loopback-only by default because it is plaintext and
exposes deployment metadata. External binding requires the explicit
`-allow-insecure-operations` acknowledgement and should be confined by network
policy. A configured operations address is bound synchronously before readiness;
port conflicts fail startup instead of leaving an unmonitored ready node.

Client concurrency and slow connections are bounded by `-max-connections`,
`-read-timeout`, and `-write-timeout`. SIGINT/SIGTERM stops admission and lets
in-flight connections drain up to `-shutdown-timeout`. Cancellation-aware
repair, hinted-handoff, and compaction workers are joined before durable
storage closes. Inter-node acknowledgement and value-response frames are also
size-bounded so a faulty peer cannot force unbounded coordinator allocation.
The command-frame limit includes syntax, a 4 KiB key, a 1 MiB value, and
authenticated replica metadata; parser and socket boundaries enforce the same
contract, including exact maximum-size values.
At the shutdown deadline, remaining client sockets are forcibly closed and all
handlers are still joined before the WAL is closed.

Nodes perform anti-entropy reconciliation every 30 seconds by default so a
replica that missed writes can converge without waiting for a client read. Use
`-repair-interval` to tune it or `-repair-interval=0` to disable it. Each tick
reconciles at most 1,024 keys and advances a stable cursor; tune the memory and
network-work ceiling with `-repair-batch-size`. Key selection scans the current
in-memory index, so this bounds allocation and replica traffic but not scan CPU.

Failed replica deliveries are persisted under the node data directory and
retried every five seconds. Tune this with `-hint-interval`; hints survive a
coordinator restart and are removed only after the target acknowledges them.
New hint files are format-fenced and content-addressed, so on-disk payload
tampering is rejected before it can become an authenticated replica mutation.
Pending hints are capped at 1 GiB by default with `-hint-max-bytes`; current and
maximum usage are exported as metrics. Capacity exhaustion is logged and
anti-entropy remains the fallback repair mechanism.
Retry scans decode at most 256 records per pass by default, bounded by
`-hint-batch-size`, so a large backlog cannot be loaded into memory at once.
Hints rejected because a replica already stores newer state are retired instead
of consuming retry capacity forever, and are counted separately in metrics.
Bounded scans rotate through the backlog so unavailable early entries cannot
starve healthy deliveries later in filename order.
Repeated failures for the same replica and key are coalesced to the newest
version, preventing hot keys from filling the repair budget with obsolete
history during an extended outage.

## Containerized three-node cluster

Create a local secret file outside the repository, point Compose at it, and
start the cluster:

```bash
openssl rand -hex 32 > ../meshdb-cluster.secret
MESHDB_CLUSTER_SECRET_FILE=../meshdb-cluster.secret docker compose up --build
```

Client ports are 7000-7002 and operations ports are 9090-9092. Each node uses a
separate persistent Docker volume and runs as a non-root user. This local demo
explicitly opts into plaintext client and operations listeners on its isolated
container network; add mounted certificates, the TLS flags, and appropriate
operations-network isolation before adapting it for production.

Follow the build history in [`prompts/`](prompts/README.md).

## Performance baselines

Run the reproducible microbenchmarks with:

```bash
go test -run '^$' -bench '.' -benchmem ./internal/storage ./internal/cluster
```

They cover durable 1 KiB WAL writes, parallel point reads, bounded page
selection over one million in-memory keys, and three-owner placement on a
100-node ring. CI smoke-runs every benchmark once so benchmark code cannot rot.
Results are machine and filesystem dependent; use repeated runs with `benchstat`
on the target deployment hardware before making capacity claims.
