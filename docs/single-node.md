# Milestone 1: durable single-node service

This document describes what is implemented in milestone 1, the exact guarantees it is designed
to provide, how those guarantees are tested, and what the tests do **not** prove. Nothing here is
distributed: there is one process, one RocksDB instance, and no replication.

## Request path

```text
client (QuantaCli, tests, any gRPC client)
   |  gRPC over HTTP/2: Put / Get / Delete   (proto: src/main/proto/quantafs/v1/kv.proto)
   v
KeyValueGrpcService      validate input, call the store, map failures to gRPC status codes
   |
   v
KeyValueStore            protocol-independent interface (get / put / delete / close)
   |
   v
RocksDbKeyValueStore     embedded RocksDB, synced write-ahead log, lifecycle locking
   |
   v
data directory           WAL (*.log), sorted tables (*.sst), MANIFEST, LOCK
```

| Package | Responsibility |
| --- | --- |
| `storage` | `KeyValueStore` interface, RocksDB implementation, storage exceptions. No gRPC imports. |
| `server` | gRPC handler, request validation, configuration, server lifecycle. No RocksDB imports. |
| `client` | Command-line demonstration client. |
| root | `Main` entry point (`server` / `client` subcommands), shutdown-safe logging. |

The handler never touches RocksDB directly. That boundary is deliberate; see
[Where Raft will fit](#where-raft-will-fit).

## API semantics

| Operation | Success | Notes |
| --- | --- | --- |
| `Put(key, value)` | empty `PutResponse` | Creates or overwrites. Last completed write wins. |
| `Get(key)` | `found = true` plus `value` | A missing key returns `found = false` with an empty `value`; it is **not** an error. |
| `Delete(key)` | empty `DeleteResponse` | Idempotent: deleting a missing key succeeds. Does not report whether the key existed. |

- **Bytes, not strings.** Keys and values are arbitrary byte sequences, including zero bytes and
  invalid UTF-8. The CLI is text-only for convenience; the tests exercise binary data through gRPC.
- **Why `found` exists.** In proto3, an unset `bytes` field and an empty one are indistinguishable.
  Without `found`, a stored empty value would look identical to a missing key.
- **Why Delete does not report existence.** Reporting it would need a read before the delete and
  locking so the answer stays correct under concurrent writers. A blind delete is simpler, and its
  outcome is the same however many times it is retried.

### Limits and errors

| Condition | gRPC status |
| --- | --- |
| Empty key, key > 1024 bytes, or value > 1 MiB (1,048,576 bytes) | `INVALID_ARGUMENT` |
| Whole request message > 2 MiB | `RESOURCE_EXHAUSTED` (rejected by gRPC before buffering) |
| Store already closed (shutdown in progress) | `UNAVAILABLE` |
| Storage engine error | `INTERNAL` with the generic description `storage failure`; details are logged server-side only |
| Any other unexpected exception | `INTERNAL` with description `internal error` |

Validation happens before the store is called, so a rejected request never modifies data (tested).

**Ambiguous outcomes.** If a client sees `INTERNAL`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, or a broken
connection for a write, the write may or may not have been applied. Retrying a Put or Delete gives
the same final state *if no other client wrote the key in between*. Exactly-once retries need
request IDs and deduplication, which are planned for a later milestone and not implemented.

## Disk synchronization policy

**Rule: a Put or Delete is acknowledged only after RocksDB has appended it to its write-ahead log
(WAL) and synced the WAL file to the storage device.**

How it works:

1. `RocksDbKeyValueStore` performs every write with `WriteOptions.setSync(true)`.
2. RocksDB appends the record to the WAL, asks the OS to sync the file to the device, then inserts
   the change into its in-memory table (memtable). Only after all of that does `db.put`/`db.delete` return.
3. The gRPC handler sends the response only after the store call returns. A success response
   therefore implies step 2 completed.
4. On startup, RocksDB replays the WAL to rebuild any memtable contents that were not yet written
   to `.sst` files. RocksDB's default WAL recovery mode (point-in-time recovery) stops at the first
   incomplete or corrupt record. Under the sync assumption below, an incomplete record at the tail
   can only belong to a write that was never acknowledged.

What this covers:

| Failure | Acknowledged writes | Evidence |
| --- | --- | --- |
| Clean shutdown (SIGTERM / Ctrl-C) | Preserved | Tested (`ServerProcessIT`, gRPC restart tests) |
| Process killed abruptly (SIGKILL, JVM crash) | Preserved | Tested (`ServerProcessIT`, see limits below) |
| OS crash or power loss | Preserved **only if** the OS, filesystem, and device honor sync requests | **Not tested** |

Assumptions and caveats:

- **Sync must reach stable storage.** Drives with volatile write caches that ignore flush commands,
  some virtualized disks, and network filesystems can break this assumption.
- **macOS.** On macOS, `fsync` hands data to the drive but does not force the drive to flush its own
  cache; Apple documents `F_FULLFSYNC` for that. QuantaFs does not configure or verify the
  syscall RocksDB uses on macOS, so treat power-loss durability on macOS as unverified. The CI
  runs on Linux.
- **Cost.** Every write waits for a disk sync, which is the slowest part of a write. RocksDB can
  share one WAL write and sync across concurrent writers (group commit). No throughput or latency
  numbers have been measured.
- **Not a backup.** A deleted or corrupted data directory is not recoverable.

The policy is fixed, not configurable, so the server cannot be started in a mode that
acknowledges writes before they are synced.

## Concurrency and ordering

gRPC runs each request on a thread from its executor; RocksDB is thread-safe and gives each write
an internal sequence number. Each Put or Delete on a single key is atomic. For concurrent writes to the
same key, the one RocksDB orders last wins. A Get that starts after a write's response is received
is intended to observe that write or a later one. This single-node behavior has not yet been
checked with a recorded-history consistency checker.

## Lifecycle

**Startup.** Parse configuration, open RocksDB (creating the directory if needed), bind the gRPC
server, then log `QuantaFs listening on <host>:<port>`. If the store cannot be opened or the port
cannot be bound, the process logs the reason, releases what it opened, and exits with code 1.

**Single owner per data directory.** RocksDB holds an OS file lock (`LOCK`) on the data directory.
A second server pointed at the same directory fails to start instead of corrupting data (tested).
The OS releases this lock when a process dies, including on SIGKILL, so restart after a crash
does not need manual cleanup.

**Graceful shutdown** runs from a JVM shutdown hook on SIGTERM or SIGINT:

1. Stop accepting new RPCs (`server.shutdown()`).
2. Wait up to `--shutdown-grace-seconds` (default 10) for in-flight RPCs to finish.
3. Cancel any that remain (`server.shutdownNow()`) and wait up to 5 more seconds.
4. Close the store. Store operations hold a shared lock and `close()` takes it exclusively, so
   close waits for any operation still running and later operations fail with `UNAVAILABLE`
   instead of touching freed native memory (which could crash the JVM).

The exit code after SIGTERM is 143 (128 + signal 15), which is normal for the JVM.

**Shutdown logging.** The JDK's logging system registers its own shutdown hook that closes all log
handlers, and JVM shutdown hooks run concurrently. Without intervention the shutdown messages
above were silently lost (an integration test caught this). `ShutdownSafeLogManager` defers that
reset until the server's shutdown completes.

**Not handled by design:** SIGKILL and power loss skip every step above; durability then relies
entirely on the synced WAL.

## Configuration

| Flag | Environment variable | Default |
| --- | --- | --- |
| `--data-dir` | `QUANTAFS_DATA_DIR` | none (required) |
| `--host` | `QUANTAFS_HOST` | `127.0.0.1` |
| `--port` | `QUANTAFS_PORT` | `50051` (`0` picks a free port) |
| `--shutdown-grace-seconds` | `QUANTAFS_SHUTDOWN_GRACE_SECONDS` | `10` |

Flags override environment variables. The default bind address is loopback because the API has
no TLS and no authentication. Binding to `0.0.0.0` exposes it to anyone who can reach the port.

## Tests and what they prove

Run everything with `./mvnw verify`. Unit and in-process tests (`*Test`) run in Maven Surefire;
process-level tests (`*IT`) run afterwards in Maven Failsafe against the packaged
`target/quantafs.jar`.

| Test class | Level | What it checks |
| --- | --- | --- |
| `RocksDbKeyValueStoreTest` | Storage | Put/get, overwrite, missing key, empty value, delete idempotence, binary data, persistence across close/reopen, use-after-close rejection, second open of a locked directory fails |
| `KeyValueGrpcServiceTest` | Real gRPC server and client over loopback TCP | Overwrite, missing key `found = false`, empty value, delete, binary data, maximum-size key/value accepted, `INVALID_ARGUMENT` cases (rejected put not applied), `RESOURCE_EXHAUSTED` above the transport limit, persistence across server restart, `INTERNAL` without leaking details, `UNAVAILABLE` when the store is closed |
| `QuantaCliTest` | CLI against a real server | Output and exit codes for put/get/delete/not-found, invalid input, usage errors, unreachable server |
| `ServerConfigTest` | Unit | Defaults, flag/environment precedence, invalid input |
| `ServerProcessIT` | Separate OS process | SIGKILL recovery under concurrent writes; SIGTERM graceful shutdown then restart; data-directory lock between processes |

### The abrupt-termination test

`ServerProcessIT.acknowledgedWritesSurviveSigkillDuringConcurrentWrites`:

1. Starts `java -jar target/quantafs.jar server` as a child process with a fresh data directory.
2. Runs 4 writer threads. Each owns 50 keys and loops over them, overwriting values and deleting
   every 7th operation. After each acknowledged operation it records the key's expected state.
3. After at least 400 acknowledged operations, sends SIGKILL (`Process.destroyForcibly()`) while
   writers are still issuing requests. Each writer stops at its first failed request and records it
   as *in flight*.
4. Asserts the killed process never logged `Storage closed`, proving no graceful path ran.
5. Restarts a new server process on the same data directory and checks, through gRPC:
   - every key whose last operation was acknowledged has exactly that state;
   - each writer's in-flight key holds either its previous acknowledged state or the in-flight
     state. Both are legal, because the client never learned the outcome.

The test prints a summary line such as
`SIGKILL recovery: <n> acknowledged operations, <k> keys verified exactly, <m> in-flight operations with unknown outcome`.
Counts vary per run.

### Limits of this test

- **SIGKILL is not power loss.** Killing a process destroys its memory but not the operating
  system's page cache. Data RocksDB had written to the WAL file but not yet synced still reaches the disk
  later through the OS. This test therefore **also passes with `setSync(false)`**; this was
  confirmed by temporarily disabling sync and rerunning it during development. What it
  verifies is that acknowledgement happens after the write reaches the WAL, that WAL replay
  recovers acknowledged writes after an unclean exit, and that in-flight writes never produce a
  state other than "before" or "after".
- **It does not test the sync itself.** Verifying the policy against power loss needs a tool that
  discards unsynced data, for example [LazyFS](https://github.com/dsrhaslab/lazyfs), Linux
  `dm-log-writes`, or hard-resetting a VM. None of these are part of this milestone.
- **No disk-fault injection.** I/O errors, a full disk, and on-disk corruption are exercised only
  through a fake store that throws `StorageException`, not through RocksDB.
- **Timing-dependent.** The kill point depends on scheduling, so each run exercises a different
  interleaving. It is not exhaustive.

## Where Raft will fit

Planned, not implemented. The current boundaries are chosen so the change is additive:

```text
today:   handler -> validate -> KeyValueStore.put -> respond
planned: handler -> validate -> append command to Raft log -> wait for commit
                 -> state machine applies committed commands in log order -> KeyValueStore.put
                 -> respond
```

- `KeyValueStore` would become the storage the state machine writes to, applied strictly in log
  order. It already has no knowledge of gRPC.
- Validation stays in front of the log, so every committed command is guaranteed to apply.
- The acknowledgement point moves from "synced to local WAL" to "committed by a majority and
  applied". The durability of the Raft log itself will need its own documented sync policy.
- Reads will need a leader check (for example, a read index) before they can be called linearizable.

## Glossary

- **gRPC:** an RPC framework over HTTP/2. The client calls generated methods (`stub.put(...)`); the
  server implements a generated base class. Failures are reported as status codes such as
  `INVALID_ARGUMENT`.
- **Protocol Buffers (protobuf):** a language-neutral schema and binary encoding. `kv.proto`
  defines the messages and the service; `protoc` generates the Java classes at build time.
- **RocksDB:** an embedded key-value storage engine (a library inside our process, not a separate
  server) based on a log-structured merge tree.
- **Write-ahead log (WAL):** an append-only file where each change is recorded before it is applied
  in memory, so it can be replayed after a crash.
- **Memtable / SST:** RocksDB keeps recent writes in an in-memory table and periodically writes
  them to immutable sorted files (`.sst`) on disk.
- **fsync / sync:** a system call asking the OS to push a file's buffered data to the storage device.
  Without it, written data may sit in the OS page cache.
- **Page cache:** OS memory holding recently written file data. It survives a process crash but not
  a power loss or kernel crash.
- **SIGTERM / SIGKILL:** SIGTERM asks a process to exit and lets it run cleanup (shutdown hooks).
  SIGKILL ends it immediately with no cleanup.
- **Graceful shutdown:** stop taking new work, finish or cancel in-flight work, release resources.
- **Idempotent:** applying the operation twice has the same effect as applying it once.
- **Linearizable:** every operation appears to take effect at a single instant between its request
  and its response, consistent with real-time order.
- **Shaded (fat) jar:** a single jar containing the application and all its dependencies.
- **Maven Wrapper (`mvnw`):** a script that downloads a pinned Maven version, so contributors do
  not need Maven installed. The download is verified against a SHA-256 checksum.
- **Surefire / Failsafe:** Maven plugins that run unit tests (`test` phase) and integration tests
  (`integration-test` / `verify` phases, after packaging).
