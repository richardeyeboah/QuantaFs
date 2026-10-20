# QuantaFs resume entry

**QuantaFs | Java, gRPC, Protocol Buffers, RocksDB, Docker, Kubernetes, Raft**

- Implemented a Raft-based key-value store with durable log replication, majority-committed writes,
  and barrier reads; validated leader failure and recovery in three- and five-process clusters.
- Built retry-safe sequential client sessions and checksummed snapshots that preserve deduplication
  state, compact committed logs, and recover lagging followers; passed 526 automated tests.
- Deployed three voters on local Kubernetes with stable peer DNS and separate persistent volumes;
  verified leader Pod replacement, storage reuse, majority-loss refusal, and recovery through a
  repeatable demo.

These bullets describe verified functionality. Performance measurements belong with their workload
and environment in [the benchmark report](benchmarks.md); avoid presenting a laptop result as a
production service guarantee. No sub-400 ms failover, 180 MB/s, or 25K ops/s claim is supported by
this project's current evidence.
