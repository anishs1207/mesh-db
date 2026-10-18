# MeshDB build journal

This directory is the versioned engineering journal for MeshDB. Every level
describes the capability added, its invariants, how to verify it, and what is
still unsafe. Git commits form executable checkpoints, not just snapshots.

## Roadmap

| Level | Capability | State |
| --- | --- | --- |
| 0 | TCP server and in-memory versioned values | Complete |
| 1 | Durable local storage and crash recovery | Planned |
| 2 | Stable protocol, idempotency, and batching | Planned |
| 3 | Partitioning, consistent hashing, and quorum replication | Planned |
| 4 | Membership, health checks, and topology changes | Planned |
| 5 | Anti-entropy, hinted handoff, and repair | Planned |
| 6 | Raft-backed strongly consistent ranges | Planned |
| 7 | LSM storage, snapshots, and compaction | Planned |
| 8 | Authentication, encryption, quotas, and auditability | Planned |
| 9 | Metrics, tracing, backups, deployment, and chaos testing | Planned |

“State of the art” is not a finish line or a claim made by a feature list.
MeshDB will only call a capability production-ready when it has explicit
correctness invariants, automated tests, failure tests, observability, upgrade
and recovery procedures, and measured performance.
