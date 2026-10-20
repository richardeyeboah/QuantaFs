# QuantaFs

### Fault-tolerant storage. Explicit guarantees. Reproducible failure tests.

QuantaFs is a distributed key-value storage engine being built in Java with Raft consensus, gRPC, and RocksDB. The goal is to keep acknowledged writes durable and the cluster consistent through server crashes and network partitions.

**Status: early development. This repository currently documents the design and implementation roadmap; the distributed system is not implemented yet.**

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

- [ ] Single-node Java service with gRPC Put, Get, and Delete.
- [ ] RocksDB persistence with restart and durability tests.
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

## First milestone

Save a value through gRPC, stop the server, restart it with the same data directory, and retrieve the original value.

## Technical references

- [Raft paper](https://raft.github.io/raft.pdf)
- [gRPC concepts](https://grpc.io/docs/what-is-grpc/core-concepts/)
- [RocksDB](https://rocksdb.org/)
