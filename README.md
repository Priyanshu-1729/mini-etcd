# mini-etcd

A lightweight implementation of an **etcd-inspired distributed key-value store** written in Go. The project is being built from scratch to understand the internals of distributed systems, including Raft consensus, log replication, write-ahead logging, snapshots, and linearizable reads.

> **Current Status:** Phase 1 — Single-node in-memory key-value store

---

## Features

### Implemented

* Thread-safe in-memory key-value store
* HTTP REST API
* CRUD operations (Get, Put, Delete)
* Server-Sent Events (SSE) based watch API
* Concurrent access using `sync.RWMutex`
* Modular project structure

### Planned

* Raft leader election
* Log replication
* gRPC node-to-node communication
* Persistent Write-Ahead Log (WAL)
* Snapshotting
* Multi-node cluster
* Linearizable reads (ReadIndex)
* MVCC revisions
* Lease management
* Prometheus metrics

---

## Architecture

```
                HTTP Client
                     │
             REST API Handlers
                     │
          ┌──────────┴──────────┐
          │                     │
      KV Store             Watch Registry
          │                     │
     sync.RWMutex         Event Channels
```

Current implementation is a **single-node service**. Future phases will introduce Raft to replicate all state changes across multiple nodes.

---

## Project Structure

```
mini-etcd/
├── cmd/
│   └── server/
│       └── main.go
├── api/
│   └── types.go
├── server/
│   └── http.go
├── store/
│   └── store.go
├── raft/          # Planned
├── wal/           # Planned
├── snapshot/      # Planned
├── proto/         # Planned
└── README.md
```

---

## REST API

### Put

```http
POST /v1/kv/put
```

Request

```json
{
  "key": "name",
  "value": "priyanshu"
}
```

---

### Get

```http
GET /v1/kv/get?key=name
```

---

### Delete

```http
DELETE /v1/kv/delete?key=name
```

---

### Watch

```http
GET /v1/kv/watch?key=name
```

Returns a Server-Sent Events (SSE) stream whenever the specified key changes.

---

## Running

```bash
go run ./cmd/server
```

The server starts on

```
localhost:2379
```

---

## Example

Store a value

```bash
curl -X POST http://localhost:2379/v1/kv/put \
-H "Content-Type: application/json" \
-d '{"key":"name","value":"priyanshu"}'
```

Retrieve it

```bash
curl "http://localhost:2379/v1/kv/get?key=name"
```

Watch for changes

```bash
curl "http://localhost:2379/v1/kv/watch?key=name"
```

---

## Roadmap

* [x] Single-node concurrent key-value store
* [x] HTTP API
* [x] Watch API using SSE
* [ ] Command abstraction
* [ ] Raft leader election
* [ ] Heartbeats
* [ ] Log replication
* [ ] State machine application
* [ ] gRPC transport
* [ ] Write-Ahead Log
* [ ] Snapshotting
* [ ] Multi-node cluster
* [ ] Linearizable reads
* [ ] MVCC
* [ ] Lease management
* [ ] Metrics
* [ ] Integration tests

---

## Learning Goals

This project focuses on understanding the implementation of distributed systems concepts rather than using existing libraries. Every major subsystem—including Raft, WAL, snapshots, and replication—is implemented from scratch in Go to gain a deeper understanding of consensus algorithms and replicated state machines.

---

## License

MIT
