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
```

Start one node:

```bash
go run ./cmd/mesh-db -port 7000
```

Start a replicated three-node demonstration:

```bash
go run ./cmd/mesh-db -port 7000 -peers 127.0.0.1:7001,127.0.0.1:7002
go run ./cmd/mesh-db -port 7001 -peers 127.0.0.1:7000,127.0.0.1:7002
go run ./cmd/mesh-db -port 7002 -peers 127.0.0.1:7000,127.0.0.1:7001
```

Follow the build history in [`prompts/`](prompts/README.md).
