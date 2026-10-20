# QuantaFs

### Fault-tolerant storage. Explicit guarantees. Reproducible failure tests.

QuantaFs is a distributed key-value storage engine being built in Java with Raft consensus, gRPC, and RocksDB. The goal is to keep acknowledged writes durable and the cluster consistent through server crashes and network partitions.

**Status: milestone 1, a durable single-node key-value service, is implemented and tested. Milestone 2A, a deterministic Raft leader-election core, is implemented and tested in simulation only; it is not connected to the server. Log replication and every distributed guarantee below are not implemented yet.**

## Implemented today vs. planned

| Implemented | Planned (not implemented) |
| --- | --- |
| **M1:** gRPC + Protocol Buffers API: `Put`, `Get`, `Delete` on byte keys/values | **M2B:** durable Raft log, log replication, commitment |
| **M1:** Explicit missing-key result (`found = false`), input validation, documented gRPC status codes | **M2B:** ordered application of committed entries to RocksDB |
| **M1:** RocksDB storage with a configurable data directory | Raft between processes over gRPC; multi-node cluster, leader-aware routing |
| **M1:** Writes acknowledged only after the RocksDB write-ahead log is synced to disk | Linearizable reads across replicas |
| **M1:** Graceful shutdown, demo CLI, gRPC tests, restart and process-level SIGKILL recovery tests | Request IDs and retry deduplication |
| **M2A:** Raft leader election: roles, terms, randomized timeouts, `RequestVote`, majority voting, heartbeats | Snapshots, lagging-node catch-up |
| **M2A:** `currentTerm`/`votedFor` persisted atomically and durably before dependent messages; fail-closed | PreVote, leader leases / read index, membership changes |
| **M2A:** Deterministic simulator (delay, reorder, duplicate, drop, partition, crash, restart) with election-safety checks | Docker images, Kubernetes manifests |
| GitHub Actions workflow running all tests | Power-loss fault injection, benchmarks |

Design, exact semantics, durability policy, and test limits:
- Milestone 1: [docs/single-node.md](docs/single-node.md)
- Milestone 2A: [docs/raft-election.md](docs/raft-election.md)

## Quickstart (single node)

Prerequisites: JDK 21 (for example, Eclipse Temurin 21) on macOS or Linux. Maven is **not** required; `./mvnw` downloads a pinned Maven 3.9.16 and verifies its checksum.

```bash
git clone https://github.com/richardeyeboah/QuantaFs.git
cd QuantaFs

# Build, run unit tests, package target/quantafs.jar, run process-level crash/shutdown tests
./mvnw verify

# Unit, in-process gRPC, and Raft simulation tests only (faster)
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

### Demonstration: Raft leader election (milestone 2A, simulation)

```bash
java -jar target/quantafs.jar election-demo                       # 5 voters, seed 7
java -jar target/quantafs.jar election-demo --voters 3 --seed 3
```

This runs the election core inside one process with a deterministic simulated network. It prints each role change, term, and vote count as a leader is elected, crashed, and replaced, then restarts the old leader. The same seed always prints the same output. It stores no client data and is not connected to the key-value server. Expected output and a line-by-line explanation: [docs/raft-election.md](docs/raft-election.md#demonstration).

### Working in an IDE

IDEs that import Maven projects through Eclipse m2e (including Cursor and VS Code with the Red Hat Java extension) compile into `target/ide/classes`, separate from Maven's `target/classes`. The `ide-separate-output` profile in `pom.xml` does this automatically. Run `./mvnw generate-sources` (or any build) once after cloning, so the generated protobuf and gRPC sources exist for the editor.

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
  - [x] 2A: leader election, heartbeats, and durable term/vote, with deterministic simulation tests ([docs/raft-election.md](docs/raft-election.md)).
  - [ ] 2B: durable log replication, commitment, and ordered state-machine application.
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
