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

Each key is placed on a consistent-hash ring. The defaults use replication
factor 3 with majority read and write quorums. Override them with
`-replication-factor`, `-read-quorum`, and `-write-quorum`; MeshDB rejects
configurations where R + W does not intersect.

Writes return after the configured quorum acknowledges them; late failures are
captured by hinted handoff. Reads return immediately when a quorum agrees, but
wait for reconciliation when replicas disagree so newer values and tombstones
are not hidden by the fast path.

Use `-compact-on-startup` during a controlled restart to rewrite an oversized
WAL with only the latest value for each key.

WAL records are checksummed and explicitly format-versioned. Startup accepts
legacy records created before format markers, but rejects unknown fields and
future formats rather than silently corrupting data during an unsafe downgrade.

MeshDB also checks for online compaction every five minutes. By default it
compacts only after the WAL reaches 256 MiB and contains at least twice as many
records as retained keys. Tune this with `-compaction-interval`,
`-compaction-min-bytes`, and `-compaction-stale-ratio`; set the interval to zero
to keep compaction under external operational control.

Use `-backup-on-startup /secure/path/backup.wal` during a controlled restart to
create a compact, checksummed backup. The destination must be new. To restore,
place the backup at `<new-data-dir>/meshdb.wal` before starting a node; always
restore into an empty data directory and retain an off-host copy.

Enable operational endpoints with `-operations-address 127.0.0.1:9090`:

- `/healthz` reports that the process is alive.
- `/readyz` reports whether the node is accepting database traffic.
- `/metrics` exports Prometheus-compatible counters and gauges.

Client concurrency and slow connections are bounded by `-max-connections`,
`-read-timeout`, and `-write-timeout`. SIGINT/SIGTERM stops admission and lets
in-flight connections drain up to `-shutdown-timeout`. Cancellation-aware
repair, hinted-handoff, and compaction workers are joined before durable
storage closes. Inter-node acknowledgement and value-response frames are also
size-bounded so a faulty peer cannot force unbounded coordinator allocation.

Nodes perform anti-entropy reconciliation every 30 seconds by default so a
replica that missed writes can converge without waiting for a client read. Use
`-repair-interval` to tune it or `-repair-interval=0` to disable it.

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

## Containerized three-node cluster

Create a local secret file outside the repository, point Compose at it, and
start the cluster:

```bash
openssl rand -hex 32 > ../meshdb-cluster.secret
MESHDB_CLUSTER_SECRET_FILE=../meshdb-cluster.secret docker compose up --build
```

Client ports are 7000-7002 and operations ports are 9090-9092. Each node uses a
separate persistent Docker volume and runs as a non-root user. This local demo
explicitly opts into plaintext on its isolated container network; add mounted
certificates and the TLS flags before adapting it for production.

Follow the build history in [`prompts/`](prompts/README.md).
