# mini-etcd

A distributed key-value store built from scratch in Go, inspired by etcd.
Implements the Raft consensus algorithm for leader election and log replication
across a 3-node cluster, with a gRPC + HTTP API and WAL-backed crash recovery.

Built to understand distributed systems internals — every subsystem including
Raft, WAL, snapshotting, and gRPC transport is written from scratch without
any consensus library.

---

## Architecture

```text
                    Client
                       │
           ┌───────────┴───────────┐
       HTTP REST               gRPC API
       /v1/kv/*             KVService proto
           └───────────┬───────────┘
                       │
                Raft Consensus
          ┌────────────┼────────────┐
       node1         node2        node3
     (leader)     (follower)   (follower)
          └────────────┼────────────┘
                       │
                State Machine
              map[string]string
              + watch registry
                       │
              Persistence Layer
           WAL (fsync) + Snapshots
```

---

## Features

- **Raft consensus** — leader election, log replication, and heartbeats across a 3-node cluster, implemented directly from the Ongaro & Ousterhout paper
- **Fault tolerance** — cluster survives any single node failure; new leader elected within 300ms
- **gRPC API** — `Get`, `Put`, `Delete`, and server-streaming `Watch` RPCs defined in protobuf
- **HTTP REST API** — `/v1/kv/get`, `/v1/kv/put`, `/v1/kv/delete`, `/v1/kv/watch`
- **Write-ahead log** — every committed entry is fsync'd to disk before applying to the store
- **Snapshotting** — full state machine snapshot every 10 commits to bound WAL growth
- **Crash recovery** — on restart, state is restored from snapshot then WAL delta is replayed
- **Watch** — real-time key change notifications via SSE (HTTP) and server-streaming gRPC
- **Observability** — `/status` endpoint exposes node ID, Raft state, term, and leader status

---

## Project Structure

```text
mini-etcd/
├── cmd/
│   └── server/
│       └── main.go       # Entry point — boots HTTP + gRPC servers and apply loop
├── raft/
│   ├── node.go           # Raft node, leader election, replication loop
│   ├── log.go            # In-memory Raft log
│   ├── rpc.go            # RequestVote and AppendEntries messages
│   ├── transport.go      # Inter-node transport
│   └── server.go         # Raft RPC handlers
├── store/
│   └── store.go          # Thread-safe KV store + watch registry
├── server/
│   ├── http.go           # REST API
│   └── grpc.go           # gRPC server
├── wal/
│   └── wal.go            # Write-ahead log
├── snapshot/
│   └── snapshot.go       # Snapshot persistence
├── proto/
│   ├── kv.proto
│   ├── kv.pb.go
│   └── kv_grpc.pb.go
├── api/
│   └── types.go
├── data/
├── go.mod
├── go.sum
└── README.md
```

---

## Getting Started

### Prerequisites

- Go 1.21+
- `grpcurl` for gRPC testing

```bash
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

---

### Run a 3-node cluster

Open three terminals and run one command in each.

#### Terminal 1

```bash
go run cmd/server/main.go \
-id=node1 \
-addr=localhost:2379 \
-grpc-addr=localhost:50051 \
-data-dir=data
```

#### Terminal 2

```bash
go run cmd/server/main.go \
-id=node2 \
-addr=localhost:2380 \
-grpc-addr=localhost:50052 \
-data-dir=data
```

#### Terminal 3

```bash
go run cmd/server/main.go \
-id=node3 \
-addr=localhost:2381 \
-grpc-addr=localhost:50053 \
-data-dir=data
```

Within approximately **300ms**, one node becomes the leader.

---

# API Reference

## Check cluster status

```bash
curl http://localhost:2379/status
curl http://localhost:2380/status
curl http://localhost:2381/status
```

Example response:

```json
{"id":"node1","state":"leader","term":1,"is_leader":true}
{"id":"node2","state":"follower","term":1,"is_leader":false}
{"id":"node3","state":"follower","term":1,"is_leader":false}
```

---

## HTTP REST API

### Put

```bash
curl -X POST http://localhost:2379/v1/kv/put \
-H "Content-Type: application/json" \
-d '{"key":"hello","value":"world"}'
```

### Get

```bash
curl http://localhost:2379/v1/kv/get?key=hello
curl http://localhost:2380/v1/kv/get?key=hello
curl http://localhost:2381/v1/kv/get?key=hello
```

### Delete

```bash
curl -X DELETE \
http://localhost:2379/v1/kv/delete?key=hello
```

### Watch

```bash
curl http://localhost:2379/v1/kv/watch?key=hello
```

---

## gRPC API

### Put

```bash
grpcurl -plaintext \
-d '{"key":"hello","value":"world"}' \
localhost:50051 kv.KVService/Put
```

### Get

```bash
grpcurl -plaintext \
-d '{"key":"hello"}' \
localhost:50051 kv.KVService/Get
```

### Delete

```bash
grpcurl -plaintext \
-d '{"key":"hello"}' \
localhost:50051 kv.KVService/Delete
```

### Watch

```bash
grpcurl -plaintext \
-d '{"key":"hello"}' \
localhost:50051 kv.KVService/Watch
```

---

## Crash Recovery

Kill the leader and watch the cluster self-heal.

```bash
# Stop node1 (Ctrl+C)

# Restart node1

go run cmd/server/main.go \
-id=node1 \
-addr=localhost:2379 \
-grpc-addr=localhost:50051 \
-data-dir=data
```

Example startup logs:

```text
[node1] restoring snapshot at index=10
[node1] replayed 2 WAL records
[node1] HTTP listening on localhost:2379
[node1] gRPC listening on localhost:50051
```

The remaining nodes elect a new leader automatically while the restarted node restores its state and rejoins the cluster.

---

## Design Notes

### Why no Raft library?

The objective of this project is to understand the Raft algorithm by implementing it from scratch. Every component—including leader election, heartbeats, log consistency checks, log replication, and commit advancement—is written directly from the Raft paper by Ongaro & Ousterhout (2014).

### WAL format

Records are stored as newline-delimited JSON and are fsync'd after every commit.

Example:

```text
{"term":2,"index":15,"command":"PUT hello world"}
```

This makes the WAL simple to inspect:

```bash
cat data/node1/raft.wal
```

### Snapshot trigger

A snapshot is created every **10 committed entries**.

Snapshots reduce recovery time by avoiding replaying the complete WAL.

In this implementation, the WAL is **not truncated** after snapshotting. WAL compaction is listed as future work.

---

## Roadmap

- [x] Single-node concurrent KV store
- [x] HTTP REST API with SSE watch
- [x] Raft leader election
- [x] Heartbeats and follower log replication
- [x] Writes routed through Raft consensus
- [x] gRPC API with Protocol Buffers
- [x] Write-ahead log with fsync
- [x] Atomic snapshotting
- [x] Crash recovery (snapshot + WAL replay)
- [x] Cluster observability via `/status`
- [ ] WAL truncation after snapshotting
- [ ] Linearizable reads (ReadIndex)
- [ ] MVCC revisions
- [ ] Lease management
- [ ] Prometheus metrics
- [ ] Integration tests

---

## License

MIT
