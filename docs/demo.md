# Demonstrating QuantaFs in an interview

[Watch the captioned 70-second demo](https://github.com/user-attachments/assets/09bb0814-8f3d-41e4-a314-b02a347fb36b).
It is an edited replay of a real captured local run; waits are shortened.

## Start with the problem

“I built a distributed key-value store in Java. The challenge is keeping acknowledged data
consistent when a server crashes or a client retries an operation whose reply was lost.”

## The architecture in 30 seconds

- gRPC and Protocol Buffers carry client and peer messages.
- Raft elects a leader and orders writes; a majority must durably append before commitment.
- Each node owns separate RocksDB databases for its Raft history and applied application state.
- Reads commit a fresh barrier rather than trusting an isolated node's local copy.
- Sequential client identities and replicated deduplication state protect retries.
- Snapshots include both application data and deduplication records so lagging nodes can recover.
- A Kubernetes StatefulSet preserves node identities and reconnects replacement Pods to their PVCs.

## Run the recovery demonstration

Prerequisite: the local cluster is running as described in [the walkthrough](kubernetes-walkthrough.md).
From the repository root:

```bash
python3 scripts/kubernetes-demo.py
```

Explain these checkpoints as they appear. The command includes a deliberate 20-second client
deadline under majority loss; allow several minutes rather than promising a fixed demo duration.

1. A client writes and reads a value through leader discovery.
2. The script records the leader's Pod UID and its PVC/volume identities.
3. It deletes the leader Pod. Kubernetes supplies a replacement; Raft supplies the consensus recovery.
4. A new journaled write completes and the replacement reuses the same persistent claim.
5. Two server processes stop. The remaining voter cannot form a majority and refuses the read.
6. Three voters return, catch up, and the original value is still readable.

All checks must print `PASS`. The script leaves the recovered cluster running and never deletes PVCs.
It uses its own journal and serializes against the benchmark runner, so do not run them together.
The workload uses dedicated demonstration keys; it is intended for the local project namespace.

## Show the code behind the behavior

- `src/main/java/.../raft/RaftNode.java`: election, log matching, commitment, and snapshot messages.
- `src/main/java/.../raft/storage/RocksDbRaftStorage.java`: durable consensus records.
- `src/main/java/.../cluster/ClusterClient.java`: leader discovery and bounded retries.
- `src/main/java/.../cluster/ClientJournal.java`: durable pending mutation before sending.
- `deploy/kubernetes/statefulset.yaml`: identity, storage, resource bounds, and probes.

The ellipsis above abbreviates the common package `io/github/richardeyeboah/quantafs`.

## Discuss the tradeoffs honestly

**Why not read directly from any follower?** It might be behind or isolated. The current implementation
uses an extra majority-committed no-op for reads; a read-index implementation could reduce overhead.

**Does a retry always mean exactly-once execution?** The guarantee has a specific sequential-session
contract: consecutive sequence numbers and one outstanding mutation per client. It is not arbitrary
transaction processing or unlimited historical reply reconstruction.

**Does Kubernetes make this durable?** RocksDB, synced writes, Raft, and persistent volumes each
provide different parts of recovery. Kubernetes recreates processes. One laptop still means one
host and one disk failure domain; replication is not backup.

**What is unfinished?** TLS/authentication, dynamic membership, independent backups, larger/chunked
snapshots, remote/multi-host validation, and broad performance characterization. Snapshot images
currently have an 8 MiB encoded limit; oversized images retain the log instead of deleting it.

## Close with evidence

“526 local automated tests passed, including real three- and five-process crash/restart scenarios.
The Kubernetes demo checks leader Pod replacement, storage reuse, minority refusal, and recovery.
My benchmark report includes raw samples and the local resource constraints; those measurements
are not a production capacity claim.”
