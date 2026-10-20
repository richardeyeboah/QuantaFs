# Leader discovery, safe retries, snapshots, and log cleanup

These are the last two implementation phases before deployment. They run locally; no containers,
Kubernetes resources, remote servers, or paid infrastructure are created.

## Watch the new behavior

```bash
./mvnw verify
python3 scripts/cluster-demo.py --advanced
```

The advanced demo uses the automatic client to save an order, terminates its leader, and finds a
replacement without a manually selected target. It then takes a follower offline, performs 40
journaled writes plus verified reads, and shows the leader's snapshot index and retained log count.
When the follower returns, it installs the snapshot and catches up. The demo owns temporary ports,
processes, databases, and its client journal. It leaves your existing data alone.

## What safe retries mean

A successful write might lose its reply on the network. Retrying an ordinary `Put(key, oldValue)`
after another application wrote `newValue` would restore old data. Reusing a request identity avoids
that: the replicated state machine recognizes the operation and changes no user data a second time.

Each mutation carries a client ID and a consecutive sequence number. The state machine saves the
latest sequence and SHA-256 fingerprint of its mutation atomically with the data and applied marker.
Every replica makes the same decision. A retried current request gets the same successful empty
response; a changed payload with that identity is rejected. Older sequence numbers are rejected
without execution. Gaps are rejected. Invalid/repeated entries still advance the applied marker.

This is a **sequential session contract**: one outstanding mutation per client ID, starting at 1.
Use different IDs/journals for independent clients. Only the latest result per client is retained;
older retries receive `OUT_OF_RANGE`, not a reconstruction of the earlier response. Sessions are
not evicted, so old sequence numbers cannot silently become executable again. This is effect
deduplication under that contract, not a universal exactly-once network guarantee.

The legacy `client` command still makes one attempt, and unidentified cluster mutations are not
safe for automatic replay. Single-node `server` mode explicitly rejects identified session writes.

## The retry client

With the three manual nodes from `network-cluster.md` running:

```bash
java -jar target/quantafs.jar cluster-client --seeds 127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103 --session ./checkout.session put order:1042 paid
java -jar target/quantafs.jar cluster-client --seeds 127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103 --session ./checkout.session get order:1042
```

The client probes seeds and rotates after failures, even if an isolated former leader still claims
leadership. A single monotonic 20-second CLI request budget bounds discovery and retries. Every
mutation retry uses identical bytes and the same ID/sequence. Reads may retry safely through fresh
consensus barriers. Permanent validation/session errors do not trigger a retry loop.

All seeds must belong to the same cluster. The client checks fixed membership keys from node
status, but this is configuration checking, not authentication. Loopback-only restrictions remain.

The session file is a checksummed journal protected by an OS ownership lock. It saves a pending
operation using a forced temporary file, atomic rename, and parent-directory sync **before sending**.
It advances the sequence only after confirmation. If it crashes after the server applied a write,
the pending record survives so reopening can retry that same operation safely. Corrupt journals
fail closed rather than generating a new identity.

After an ambiguous failure, retry the exact command or run:

```bash
java -jar target/quantafs.jar cluster-client --seeds 127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103 --session ./checkout.session resume
```

A different write is refused while a pending mutation exists. Do not delete, copy, roll back, or
share a session journal to bypass uncertainty. A journal belongs to one sequential client and one
cluster. Sessions and pending payloads are local private state and are ignored by Git.

## What a snapshot does

Instead of retaining every operation since startup, a node periodically stores a complete image of
its committed, applied state. This image includes application keys/values, retry-session records,
and the included log index and term. A versioned format and CRC detect corruption and boundary
mismatches. A snapshot of index 128 means its state already incorporates operations through 128.
The log can retain index 129 onward, rather than replaying 1–128 again.

Every 64 newly applied entries, the local runtime attempts compaction. The storage atomically saves
the image and removes the covered log keys in one synced RocksDB batch. User data is not deleted.
Index/term metadata at the boundary remains available for log matching and elections. RocksDB's
background compaction reclaims physical obsolete records; logical deletion does not promise an
immediate filesystem-size reduction.

Nodes can compact at different indexes. A follower's verified snapshot boundary can advance a
leader's obsolete probe; otherwise the leader sends its own snapshot when a follower needs entries
that no longer exist. Responses from older terms and acknowledgements beyond the leader's log
cannot advance replication unsafely.

## Restart and installation ordering

1. Validate the image and its boundary.
2. Save it durably in Raft storage, atomically replacing the covered prefix. Retain a suffix only
   when its boundary term matches; conflicting uncommitted suffixes are discarded.
3. Restore data, sessions, and the applied marker atomically in the application database.
4. Only then acknowledge installation and resume suffix replication.

If a crash interrupts steps 2–3, startup sees that the application is behind the durable snapshot
and restores it before serving requests. Applied state and retry records therefore cannot be
replaced independently. Older snapshots never roll back committed application state. Snapshots
are internal replication checkpoints, not independently managed backups.

## Upgrade procedure

Stop all old local node processes, build the new jar, then restart every node using its original
identity, membership, and directory. Existing unsnapshotted storage is readable. Older jars cannot
read the new request/snapshot formats; mixed-version rolling upgrades are unsupported. The automatic
client rejects nodes without the new retry-safe protocol metadata.

## Deliberate limits

Snapshots currently transfer as a single message with an **8 MiB encoded-image limit**. Larger
images leave the old log intact and set `snapshotLimited=true` in node status. The runtime retries
snapshot creation after a 30-second cooldown, allowing a reduced dataset to fit later. It does not
silently discard data to satisfy the limit. Chunked/streaming snapshots and multi-gigabyte datasets
are outside this version. Dataset/session metadata growth still needs operational monitoring.

Transport is loopback-only and unauthenticated. No deployment, TLS, Kubernetes, external fault
infrastructure, disk-power-loss proof, or benchmark claim is added in these phases.

## Local validation

On October 20, 2026, the final `./mvnw verify` run passed **521 tests**: 515 unit,
in-process, codec, storage, and simulation checks plus six process-level integration checks.
There were zero failures, errors, or skipped tests. Both three- and five-process scenarios passed.
The final advanced demonstration also passed: it completed leader replacement, 40 journaled writes
with read checks, log-prefix compaction, and snapshot recovery of an offline follower. All three
nodes finished with the same applied index. These are local results; hosted GitHub checks are not
required for this verification.

## Evidence

Tests cover retrying an acknowledged mutation after a newer overwrite, conflicting identities,
stale/gapped sequences, journal restart/locking/corruption, snapshot preservation of sessions,
compacted-log reopen, corruption rejection, interrupted snapshot application, and an offline
follower installing an image after the missing prefix has been deleted. Real three- and five-process
checks combine automatic client failover, snapshot catch-up, majority loss, and full-cluster restart.
These are local tests. Deployment is the next user-controlled phase.
