# QuantaFs

### Fault-tolerant storage. Explicit guarantees. Reproducible failure tests.

QuantaFs is a distributed key-value storage engine being built in Java with Raft consensus, gRPC, and RocksDB. The goal is to keep acknowledged writes durable and the cluster consistent through server crashes and network partitions.

**Status: milestone 1, a durable single-node key-value service (the first two roadmap items), is implemented and tested. Raft, replication, and every distributed guarantee below are not implemented yet.**

## Implemented today vs. planned

| Implemented (milestone 1) | Planned (not implemented) |
| --- | --- |
| gRPC + Protocol Buffers API: `Put`, `Get`, `Delete` on byte keys/values | Raft leader election and log replication |
| Explicit missing-key result (`found = false`), input validation, documented gRPC status codes | Multi-node cluster, leader-aware routing |
| RocksDB storage with a configurable data directory | Linearizable reads across replicas |
| Writes acknowledged only after the RocksDB write-ahead log is synced to disk | Request IDs and retry deduplication |
| Graceful shutdown on SIGTERM/SIGINT with ordered resource cleanup | Snapshots, lagging-node catch-up |
| Command-line client for demonstrations | Docker images, Kubernetes manifests |
| Tests through gRPC, restart persistence tests, process-level SIGKILL recovery test | Network-partition and power-loss fault injection, benchmarks |
| GitHub Actions CI running all tests | |

Design, exact semantics, durability policy, and test limits: [docs/single-node.md](docs/single-node.md).

## Quickstart (single node)

Prerequisites: JDK 21 (for example, Eclipse Temurin 21) on macOS or Linux. Maven is **not** required; `./mvnw` downloads a pinned Maven 3.9.16 and verifies its checksum.

```bash
git clone https://github.com/richardeyeboah/QuantaFs.git
cd QuantaFs

# Build, run unit tests, package target/quantafs.jar, run process-level crash/shutdown tests
./mvnw verify

# Unit and in-process gRPC tests only (faster)
./mvnw test
```

Start a server. It listens on `127.0.0.1:50051` by default and stores data in the directory you give it (`./data` is git-ignored):

```bash
java -jar target/quantafs.jar server --data-dir ./data
```

In a second terminal, use the demo client:

```bash
java -jar target/quantafs.jar client put greeting "hello, durable world"   # OK
java -jar target/quantafs.jar client get greeting                          # hello, durable world
java -jar target/quantafs.jar client put greeting "overwritten"            # OK
java -jar target/quantafs.jar client get missing-key                       # NOT FOUND (exit code 1)
java -jar target/quantafs.jar client delete greeting                       # OK
java -jar target/quantafs.jar client put "" value                          # error: INVALID_ARGUMENT: key must not be empty (exit code 3)
```

Client exit codes: `0` success, `1` key not found, `2` usage error, `3` RPC error. Use `--target host:port` to reach a different server. Run `server --help` or `client --help` for all options.

### Demonstration: data survives a restart and a hard kill

```bash
java -jar target/quantafs.jar server --data-dir ./data &
SERVER_PID=$!; sleep 2
java -jar target/quantafs.jar client put city Accra

kill -TERM $SERVER_PID; wait $SERVER_PID       # graceful: logs "gRPC server stopped" and "Storage closed"

java -jar target/quantafs.jar server --data-dir ./data &
SERVER_PID=$!; sleep 2
java -jar target/quantafs.jar client get city  # Accra
java -jar target/quantafs.jar client put survivor "written right before kill -9"

kill -KILL $SERVER_PID; wait $SERVER_PID       # abrupt: no shutdown hook runs

java -jar target/quantafs.jar server --data-dir ./data &
SERVER_PID=$!; sleep 2
java -jar target/quantafs.jar client get survivor   # written right before kill -9
kill -TERM $SERVER_PID
```

A process kill does **not** simulate power loss: the operating system's page cache survives it. See [what the crash test proves and does not prove](docs/single-node.md#limits-of-this-test).

## The engineering problem

Saving data on one server is straightforward. Keeping several servers in agreement when messages arrive late, a leader crashes, or a client retries is the hard part. QuantaFs explores that boundary between storage, networking, and distributed coordination.

The target demonstration: write data to a five-node cluster, interrupt its leader and another node, then verify that acknowledged writes remain readable after recovery. Availability requires a communicating majority and eventual stable leadership.

## Planned architecture

```text
Application / test client
          | gRPC: Put / Get / Delete
          v
   Leader-aware routing
          |
          v
   Raft replication group
   [A] [B] [C] [D] [E]
    |   |   |   |   |
   Each node owns durable Raft state
   and a local RocksDB state machine
```

- **Java:** server implementation, concurrency control, and lifecycle management.
- **gRPC + Protocol Buffers:** typed client APIs and inter-node communication.
- **Raft:** leader election, ordered log replication, and safe commitment.
- **RocksDB:** persistent local key-value state; durable Raft metadata and log storage will have an explicit recovery contract.
- **Docker + Kubernetes:** reproducible deployment with stable node identities and persistent volumes.

## Target guarantees

These are implementation goals, not verified capabilities.

- Acknowledged writes survive supported crash-and-recovery scenarios under the documented disk durability assumptions.
- Linearizable reads: completed writes are reflected in subsequent reads unless superseded.
- A five-voter cluster continues committing with two failed nodes when the remaining majority can communicate and elect a leader.
- A minority partition cannot acknowledge new writes.
- Request identities and replicated deduplication state prevent repeated execution of retried mutations within a defined retention policy.
- Restarted nodes recover safely and catch up before serving reads that require current state.

Scope starts with one replication group and fixed membership. Sharding, multi-key transactions, dynamic membership, and Byzantine fault tolerance are outside the initial implementation. Replication is not a substitute for backups.

## Implementation roadmap

- [x] Single-node Java service with gRPC Put, Get, and Delete.
- [x] RocksDB persistence with restart and durability tests (process-crash level; power-loss testing is part of the fault-injection item).
- [ ] Deterministic Raft core with election and replication tests.
- [ ] Three-node cluster with durable consensus state and recovery.
- [ ] Correct reads, client retries, and request deduplication.
- [ ] Snapshotting and lagging-node recovery.
- [ ] Five-node deployment with persistent storage.
- [ ] Fault-injection suite and reproducible benchmark reports.

## Validation strategy

Correctness comes before throughput. Tests will exercise leader termination, follower restart, network partitions, concurrent clients, and ambiguous client timeouts. A recorded operation history will support checks that observed behavior respects the promised consistency model.

Benchmark reports will include hardware, software versions, payload sizes, concurrency, read/write mix, disk synchronization settings, throughput, latency percentiles, error rates, and recovery-time definitions.

**No performance or failover numbers are claimed yet. Results will be published with commands and raw evidence once measured.**

## First milestone (complete)

Save a value through gRPC, stop the server, restart it with the same data directory, and retrieve the original value. Covered by `KeyValueGrpcServiceTest.dataPersistsAcrossServerRestart`, `ServerProcessIT`, and the demonstration above.

## Technical references

- [Raft paper](https://raft.github.io/raft.pdf)
- [gRPC concepts](https://grpc.io/docs/what-is-grpc/core-concepts/)
- [RocksDB](https://rocksdb.org/)
