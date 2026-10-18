# Level 0: runnable in-memory node

## Goal

Prove the smallest end-to-end database loop: accept a TCP connection, parse a
command, mutate state, and return a response.

## What exists

- A newline-delimited TCP protocol with `PING`, `SET`, and `GET`.
- A mutex-protected in-memory map.
- Lamport-style versions with a node-origin tie breaker.
- Best-effort synchronous fan-out to statically configured peers.
- A shell client for interactive experiments.

## Run it

```bash
go run ./cmd/mesh-db -port 7000
./meshdb-cli.sh 7000
```

## Correctness model

Each node accepts writes independently. A record with a larger logical version
wins; equal versions are ordered by origin. Replicas do not forward replicated
writes, which prevents loops.

## Known limitations

- State disappears on restart.
- Versions and node identity are not durable.
- Acknowledgements are not quorum based.
- Static peer lists have no failure detector or membership protocol.
- Concurrent writes can be ordered differently from real-world intent.
- The text protocol cannot safely represent whitespace in keys or values.
- There is no authentication, encryption, request limit, or observability.

These limitations are documented rather than hidden. Later levels remove them
one at a time while retaining regression tests.
