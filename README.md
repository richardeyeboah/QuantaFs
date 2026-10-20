# QuantaFs

### Fault-tolerant storage. Explicit guarantees. Reproducible failure tests.

QuantaFs is a distributed key-value storage engine being built in Java with Raft consensus, gRPC, and RocksDB. The goal is to keep acknowledged writes durable and the cluster consistent through server crashes and network partitions.

**Status: the local cluster now includes automatic leader discovery, journaled retry-safe mutations,
and snapshots with log-prefix cleanup and lagging-node recovery. These work on top of the durable
Raft core and real gRPC transport. Stage 1 packages the loopback single-node service; Stage 2 runs
three nodes on an isolated local Compose network. Stage 3 now provides local Docker Desktop
Kubernetes StatefulSet deployment, verified on a local Docker Desktop cluster.
TLS/authentication and remote-host setup are not included.**

## Review the project

- **See recovery:** [interview demo](docs/demo.md), including the repeatable Kubernetes failure script.
- **Inspect measurements:** [benchmark method and results](docs/benchmarks.md), with raw latency samples.
- **Understand the guarantees:** [retry sessions and snapshots](docs/retries-and-snapshots.md).
- **Run it yourself:** [Docker Compose](docs/compose-walkthrough.md) or [local Kubernetes](docs/kubernetes-walkthrough.md).

The system uses fixed membership and a single replication group. The local Kubernetes deployment
runs three voters on one machine; five-voter behavior is covered by process tests. TLS, authentication,
dynamic membership, and multi-host production validation remain unfinished.

## Implemented today vs. planned

| Implemented | Planned (not implemented) |
| --- | --- |
| **M1:** gRPC + Protocol Buffers API: `Put`, `Get`, `Delete` on byte keys/values | Deployment security and remote-host support |
| **M1:** Explicit missing-key result (`found = false`), input validation, documented gRPC status codes | Remote hosts with authenticated, encrypted transport |
| **M1:** RocksDB storage with a configurable data directory; writes acknowledged only after the WAL is synced | Read-index optimization (current cluster reads use a replicated barrier) |
| **M1:** Graceful shutdown, demo CLI, gRPC tests, restart and process-level SIGKILL recovery tests | Chunked snapshots for images larger than 8 MiB |
| **M2A:** Raft leader election: roles, terms, randomized timeouts, `RequestVote`, majority voting, heartbeats | Streaming snapshots and large-dataset tuning |
| **M2B:** Durable term-tagged log (`PUT`, `DELETE`, `NO_OP`); full `AppendEntries` with log matching, `nextIndex`/`matchIndex`, catch-up, conflict repair | PreVote, leadership transfer, membership changes |
| **M2B:** Majority commitment restricted to current-term entries (leader `NO_OP`; Figure 8 tested) | Multi-host deployment validation |
| **M2B:** Term, vote, and log in one atomic synced RocksDB write; OS-locked node directories; corruption refuses startup | Power-loss fault injection, sustained capacity benchmarks |
| **M2B:** RocksDB state machine with atomic data + `lastApplied` marker; strict in-order apply; `commitIndex` relearned after restart | |
| **M2B:** Internal proposal API: success only after majority commit and local apply; `UNKNOWN` if interrupted | |
| **M2:** Deterministic simulator (delay, reorder, duplicate, drop, partition, crash, restart, injected storage/apply failures) checking election safety, log matching, committed agreement, ordered apply, and convergence | |
| **M3:** Real gRPC peer transport; public Put/Delete confirm after Raft commitment and local application | |
| **M3:** Get uses a fresh majority-committed read barrier; followers return leader hints | |
| **M3:** Three-/five-process failure and recovery tests; fixed identities and node directory ownership | |
| **M4:** Automatic leader discovery, durable client journals, and replicated sequential-session deduplication | |
| **M5:** Checksummed snapshots include data and sessions; compacted-prefix recovery and snapshot transfer | |
| **Deployment Stage 1:** Multi-stage Java 21 image for the loopback single-node service; non-root runtime and explicit persistent data mount | |
| **Deployment Stage 2:** Three-node Compose cluster; stable DNS membership, private internal network, separate node volumes, non-root client failover demo | |
| **Deployment Stage 3:** Local-only Namespace, headless Service, three-voter StatefulSet/PVCs, probes, non-root client tool, and strict context guard; local runtime validated | |
| GitHub Actions workflow configured for all tests (hosted execution was blocked by an account billing lock) | |

Design, exact semantics, durability policy, and test limits:
- Milestone 1: [docs/single-node.md](docs/single-node.md)
- Milestone 2A: [docs/raft-election.md](docs/raft-election.md)
- Milestone 2B: [docs/raft-replication.md](docs/raft-replication.md)
- Milestone 3: [docs/network-cluster.md](docs/network-cluster.md)
- Milestones 4–5: [docs/retries-and-snapshots.md](docs/retries-and-snapshots.md)
- Deployment Stage 1: [docs/container-walkthrough.md](docs/container-walkthrough.md)
- Deployment Stage 2: [docs/compose-walkthrough.md](docs/compose-walkthrough.md)
- Deployment Stage 3: [docs/kubernetes-walkthrough.md](docs/kubernetes-walkthrough.md)

## Try the real cluster

```bash
./mvnw verify
python3 scripts/cluster-demo.py --advanced
```

The demo starts three separate Java processes, saves an order, forcibly terminates the leader,
reads the confirmed order from the replacement, and restarts the old node to demonstrate catch-up.
Use `--nodes 5` for five processes. It owns only temporary storage and cleans up its processes.
See [the network cluster walkthrough](docs/network-cluster.md) for manual commands, architecture,
read guarantees, test evidence, and limitations.

To run the same three-voter runtime in isolated local containers:

```bash
docker compose build
docker compose up --detach --wait --wait-timeout 60
```

No ports are published to the host. See the [Compose walkthrough](docs/compose-walkthrough.md) for
leader discovery, retry-client, failover, recovery, security-boundary, and safe cleanup commands.

Local Kubernetes resources are under `deploy/kubernetes`. Three voters and their separate PVCs
have been started on Docker Desktop Kubernetes. Follow the
[Kubernetes walkthrough](docs/kubernetes-walkthrough.md), including its strict local-context check.

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

This runs simulated nodes inside one process with a deterministic network and in-memory storage. It prints each role change, term, and vote count as a leader is elected, crashed, and replaced, then restarts the old leader. The same seed always prints the same output. It is not connected to the key-value server. Expected output and a line-by-line explanation: [docs/raft-election.md](docs/raft-election.md#demonstration).

### Demonstration: Raft log replication (milestone 2B, simulation with real storage)

```bash
java -jar target/quantafs.jar replication-demo                    # 3 voters, seed 7
java -jar target/quantafs.jar replication-demo --voters 5 --seed 2
java -jar target/quantafs.jar replication-demo --keep             # keep the per-node RocksDB files
```

This elects a leader, replicates and applies client writes, rejects a write sent to a follower, cuts the leader off so that a write it accepts cannot commit, crashes it, elects a replacement, shows that committed data survived, and restarts the old leader from its RocksDB files so it catches up and its uncommitted entry is replaced. Each node gets its own directory under a new temporary directory, deleted afterwards unless `--keep` is given; `./data` is never touched. Nodes still run in one process with a simulated network. Expected output and a phase-by-phase explanation: [docs/raft-replication.md](docs/raft-replication.md#demonstration).

### Working in an IDE

IDEs that import Maven projects through Eclipse m2e (including Cursor and VS Code with the Red Hat Java extension) compile into `.ide-build/`, separate from Maven's `target/classes`. The `ide-separate-output` profile in `pom.xml` does this automatically. The folder is outside `target/` so that `./mvnw clean` cannot delete the IDE's incremental-build state, which made the editor report same-package types as unresolved. Run `./mvnw generate-sources` (or any build) once after cloning, so the generated protobuf and gRPC sources exist for the editor.

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
- **RocksDB:** persistent local key-value state, and (since 2B) a separate per-node database for durable Raft metadata and log with an explicit recovery contract.
- **Docker:** local single-node and three-node Compose packaging with stable identities and persistent volumes.
- **Kubernetes:** local Docker Desktop StatefulSet, stable peer DNS, separate persistent claims,
  bounded probes, and non-root client workflow; local runtime validated.

## Target guarantees

The local implementation and tests support these under the documented crash/storage and sequential
session assumptions. Remote deployment and production-scale evidence remain outstanding.

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
- [x] Deterministic Raft core with election and replication tests (the core is independently tested in simulation).
  - [x] 2A: leader election, heartbeats, and durable term/vote, with deterministic simulation tests ([docs/raft-election.md](docs/raft-election.md)).
  - [x] 2B: durable log replication, commitment, and ordered state-machine application ([docs/raft-replication.md](docs/raft-replication.md)).
- [x] Local three-/five-process gRPC cluster with durable consensus state, recovery, and barrier reads.
- [x] Correct reads, automatic leader discovery, and retry deduplication (sequential client-session contract).
- [x] Snapshotting, logical log-prefix cleanup, and lagging-node recovery (8 MiB single-image limit).
- [x] Stage 1 single-node Docker image with a non-root runtime and persistent data mount.
- [x] Stage 2 local-only network boundary and three-node Docker Compose deployment.
- [x] Stage 3 local Kubernetes manifests, strict context guard, probes, and persistent claims
  (server validated and run locally).
- [x] Stage 4 local Kubernetes Pod replacement, PVC reuse, majority-loss refusal, and recovery demo.
- [ ] Five-node deployment with persistent storage.
- [x] Initial reproducible local benchmark report with raw samples and explicit workload limits.
- [ ] Broader fault-injection and sustained capacity characterization.

## Validation strategy

Correctness comes before throughput. Tests will exercise leader termination, follower restart, network partitions, concurrent clients, and ambiguous client timeouts. A recorded operation history will support checks that observed behavior respects the promised consistency model.

Benchmark reports will include hardware, software versions, payload sizes, concurrency, read/write mix, disk synchronization settings, throughput, latency percentiles, error rates, and recovery-time definitions.

**Initial local measurements:** 6,000 operations completed across six trials with zero observed errors.
Median trial throughput was 135.7 ops/s with one client and 130.1 ops/s with four clients in the
resource-limited local Kubernetes setup. See the [full method, latency results, variation, and raw evidence](docs/benchmarks.md).
These are workload-specific observations; no production capacity or failover-time guarantee is claimed.

## First milestone (complete)

Save a value through gRPC, stop the server, restart it with the same data directory, and retrieve the original value. Covered by `KeyValueGrpcServiceTest.dataPersistsAcrossServerRestart`, `ServerProcessIT`, and the demonstration above.

## Technical references

- [Raft paper](https://raft.github.io/raft.pdf)
- [gRPC concepts](https://grpc.io/docs/what-is-grpc/core-concepts/)
- [RocksDB](https://rocksdb.org/)
