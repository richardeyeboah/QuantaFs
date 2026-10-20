# Milestone 2B: durable log replication, safe commitment, ordered application

This is a historical milestone checkpoint. The current automatic retry client and snapshot/log
cleanup implementation are documented in [milestones 4–5](retries-and-snapshots.md).


Milestone 2B extends the deterministic Raft core from [milestone 2A](raft-election.md) with the
replication half of Raft:
- a durable, term-tagged log;
- `AppendEntries` with the log-matching check;
- per-follower progress tracking;
- commitment by majority, restricted to current-term entries;
- strict in-order application of committed entries to a RocksDB key-value state machine;
- a proposal API that reports success only after commitment and local application.

**What 2B is not:** the nodes still run inside one process, connected by the deterministic simulator.
There is no network transport between machines. The public single-node gRPC server from milestone 1
is unchanged and is **not** connected to Raft. Nothing here should be read as a working distributed
database, or as a claim of multi-process availability.

Reference: Ongaro and Ousterhout, *In Search of an Understandable Consensus Algorithm* (2014), the
[Raft paper](https://raft.github.io/raft.pdf). Rule numbers refer to its Figure 2.

## Implemented vs. deferred

| Implemented in 2B | Deferred |
| --- | --- |
| Immutable `LogEntry(index, term, command)` with `PUT`, `DELETE`, and internal `NO_OP` commands; binary keys/values; limits checked when a command is created | Inter-node gRPC transport, multi-process cluster |
| Full `AppendEntries` (`prevLogIndex`, `prevLogTerm`, entries, `leaderCommit`); heartbeats obey the same check | Routing the public gRPC API through Raft |
| `nextIndex`/`matchIndex`, retries, missing-prefix catch-up, replacement of conflicting uncommitted entries | Linearizable reads (read index or leases) |
| Responses matched to their requests; stale responses cannot regress progress | Client request IDs and deduplication (exactly-once) |
| Majority commitment restricted to current-term entries; leader `NO_OP` on election (Figure 8 tested) | Snapshots and log compaction |
| One atomic, synced storage write for term, vote, and log changes (`RocksDbRaftStorage`) | Dynamic membership |
| OS-locked node directory; corruption and inconsistency refuse startup | Docker, Kubernetes |
| RocksDB state machine; data and `lastApplied` marker in one atomic write; strict index order | Power-loss testing |
| `commitIndex` relearned after restart (starts at `lastApplied`) | PreVote, leadership transfer |
| Proposal API: success only after majority commit and local apply; `UNKNOWN` when interrupted | |
| Simulator: replication invariants, client tracking, RocksDB-backed nodes; `replication-demo` | |

## Key terms

- **Log entry:** one slot in the replicated log: an **index** (its position, starting at 1), the
  **term** of the leader that created it, and a **command**. Raft identifies an entry by
  (index, term).
- **Log Matching property:** if two logs contain an entry with the same index and term, they hold
  the same command there and are identical in every earlier entry. `AppendEntries` keeps this true
  by refusing to append unless the entry just before the new ones (`prevLogIndex`, `prevLogTerm`)
  matches.
- **Committed:** an entry is committed once it is safe to apply. It will never be removed or
  changed on any node that later becomes leader. The leader decides this by counting replicas (with
  the restriction below). Followers learn it from `leaderCommit`.
- **`commitIndex`:** the highest index a node knows to be committed. **`lastApplied`:** the highest
  index it has applied to its state machine. Always `lastApplied <= commitIndex <= last log index`.
- **`nextIndex[f]`:** the next entry the leader will send follower `f`. **`matchIndex[f]`:** the
  highest index the leader knows `f` has stored. A guess and a fact, respectively.
- **State machine:** the key-value data that committed commands are applied to, in log order.
  Every node applies the same commands in the same order, so all reach the same state.
- **`NO_OP`:** an entry with no effect. A new leader appends one from its own term, so it has a
  current-term entry to commit (see Figure 8 below).
- **Leader Completeness:** a leader's log contains every committed entry. **State Machine Safety:**
  no two nodes apply different commands at the same index.
- **Acknowledgement (here):** the leader reporting `COMMITTED` for a proposal. That means a majority
  stored the entry durably and the leader applied it.

## Request flow

```text
client ──propose(cmd)──> leader
                           1. append LogEntry(index, term, cmd) to its log: atomic + fsync
                           2. AppendEntries(prev, entries, leaderCommit) ──> each follower
follower                   3. consistency check at prevLogIndex/prevLogTerm
                           4. persist new entries (and any newer term) in ONE atomic write + fsync
                           5. reply success(matchIndex = prev + entries.size()) ──> leader
leader                     6. matchIndex[f] = max(matchIndex[f], reply.matchIndex)
                           7. commit N if a majority has N AND log[N].term == currentTerm
                           8. apply entries lastApplied+1..commitIndex in order (atomic with marker)
                           9. report COMMITTED to the client
followers                 10. learn commitIndex from a later AppendEntries.leaderCommit and apply
```

Only after step 8 does the client hear success. If the leader loses leadership before step 7, the
client is told `UNKNOWN` (see [client outcomes](#client-outcomes)).

## AppendEntries in detail

### Follower side (`RaftNode.onAppendEntries`)

1. **Term check.** A request from a lower term is refused, and the timer is not reset. A higher
   term is adopted.
2. **Consistency check (rule 2).** The local log must have an entry at `prevLogIndex` with term
   `prevLogTerm`. If not, the reply is a failure carrying a hint: the follower's last index. A
   heartbeat (an AppendEntries with no entries) is checked the same way.
3. **Merge (rules 3-4).** Walk the incoming entries:
   - an entry already present with the same term is skipped;
   - at the first index whose term differs, that entry and everything after it is removed, and the
     rest is appended;
   - entries past the end of the log are appended.

   An identical duplicate, or a delayed, shorter request that arrives late, therefore changes
   nothing. A matching request never removes anything. Removing an entry at or below
   `commitIndex` would mean the leader conflicts with committed data. The node throws instead of
   obeying (fail stop).
4. **One atomic write.** A newer term, the removal, and the appended entries are one `StorageWrite`.
   It is synced before the reply is built. A failure halts the node, and it sends no reply.
5. **Commit (rule 5).** `commitIndex = max(commitIndex, min(leaderCommit, prevLogIndex + entries.size()))`.
   Only the prefix this request verified counts. Entries beyond it in the local log may still be
   replaced, even if `leaderCommit` is larger.
6. Apply newly committed entries, then reply success with `matchIndex = prevLogIndex + entries.size()`.

### Leader side (`RaftNode.onAppendEntriesResponse`)

- On election, `nextIndex[f] = last index + 1` and `matchIndex[f] = 0`. The leader appends its
  `NO_OP` and sends it immediately.
- Every heartbeat is a real AppendEntries from `nextIndex[f]`, carrying at most 16 entries. So
  unacknowledged entries are retried on every heartbeat, and a lagging follower catches up in
  batches.
- **Responses are matched to their requests.** Each response echoes the request's term and
  `prevLogIndex`. The leader ignores:
  - responses to requests from an earlier term (`requestTerm != currentTerm`);
  - responses from a lower term.
- **Success:** `matchIndex[f]` only ever increases (`max`). A delayed or duplicated success can
  never lower it. Any current-term success proves that `f` durably held that prefix in this term,
  and nothing in this term can remove it. If the success advanced `matchIndex` and more entries
  remain, the next batch is sent at once.
- **Failure:** acted on only if it answers the probe the leader would send now
  (`requestPrevLogIndex == nextIndex[f] - 1`). Then
  `nextIndex[f] = max(matchIndex[f] + 1, min(requestPrevLogIndex, followerLastIndex + 1))` and the
  request is resent. Older or duplicated failures are ignored, so they cannot push `nextIndex` back
  repeatedly. And it never drops to or below a known match.
- A higher term in any response makes the leader step down. Its unfinished proposals become `UNKNOWN`.

## Commitment and Figure 8

**Rule:** the leader commits the highest `N` such that a majority (itself included) has
`matchIndex >= N` **and** `log[N].term == currentTerm`. Entries from older terms are never committed
by counting replicas. They become committed as the prefix of a committed current-term entry.

**Why (paper, section 5.4.2, Figure 8).** Suppose index 2 holds a term-2 entry on servers s1, s2,
and s3 of five. That's a majority. Meanwhile s5 holds a different index-2 entry from term 3. If s1
crashes, s5 can win term 5 with votes from s2, s3, and s4, because its last entry (term 3) is more
up to date than theirs (term 2). s5 then overwrites index 2 on s2 and s3. So "stored on a majority"
was not enough for an old-term entry. A leader that had reported it committed would have lost
committed data.

**The NO_OP.** A new leader appends a `NO_OP` from its own term. Once that reaches a majority, it
commits, and every earlier entry commits with it. After that, a stale candidate like s5 cannot
win: a majority now has a last entry from the newer term.

`Figure8Test` plays this out with five real nodes:
- `leaderDoesNotCommitAnOldTermEntryEvenWhenAMajorityStoresIt`: the term-4 leader keeps
  `commitIndex` at 0 while index 2 (term 2) sits on 3 of 5. It commits 1-3 only when its `NO_OP`
  reaches a majority.
- `whyItMatters_theOldTermEntryOnAMajorityCanStillBeOverwritten`: s5 wins the votes of s2, s3, and
  s4 and replaces the term-2 entry.
- `onceTheNoOpIsOnAMajorityTheStaleCandidateCannotWin`: after s2 stores the `NO_OP`, it refuses s5.

Removing the current-term check makes the first test and
`RaftReplicationTest.inheritedEntriesCommitOnlyThroughACurrentTermEntry` fail (see
[mutation evidence](#evidence-the-tests-can-fail)).

## Durable storage

### Contract (`RaftStorage`)

- `write(StorageWrite)` applies an optional new `(currentTerm, votedFor)`, an optional removal of
  every entry from some index on, and appended entries. It is **all or nothing**, including across
  a crash, and **durable when it returns**.
- Every write is checked against Raft's rules before anything changes:
  - the term never goes back;
  - a vote never changes within a term;
  - entries are contiguous;
  - entry terms never decrease and never exceed the current term.

  A rejected write throws and leaves storage untouched.
- After an I/O failure the instance refuses further use. The node halts, and only a reopen (a
  restart) trusts the disk again.

**Why term, vote, and log share one atomic write.** A follower that learns a higher term from a
leader and accepts its entries records both in one step. If they were separate writes, a crash in
between could leave entries from term 7 next to a stored term of 6. Or it could leave a newer term
next to an older log. That is the "mismatched independently updated state" that can cause unsafe
votes or lost acknowledged entries. With one write, recovery always sees a state that existed. The
2A design kept term and vote in their own file, and that file was removed in 2B for exactly this reason.

### Why a separate RocksDB database (and not files)

- **Atomic multi-key writes.** A RocksDB `WriteBatch` commits several keys through a single
  write-ahead-log record. With `sync=true`, that record is fsynced before `write` returns.
- **Checksums and recovery.** RocksDB checksums its files and recovers its WAL on open.
- **Separation.** A dedicated database keeps consensus state apart from application data, so a bug
  or a key collision in one cannot corrupt the other.
- **The alternative** would be a hand-written append-only file with its own framing, fsync and
  rename discipline, truncation, and recovery. That's more code to get subtly wrong. The 2A file
  store was a reasonable approach for one small record, but not for a growing log that also needs
  truncation.

### Layout on disk

```text
<node-dir>/
  node.lock              exclusive OS lock (FileChannel.tryLock) while the node is open
  raft-log/              RocksDB: RocksDbRaftStorage
     key 0x01                         -> TermAndVote record
     key 0x02 + 8-byte BE index       -> LogEntry record (big-endian, so key order = log order)
  state-machine/         RocksDB: RocksDbStateMachine
     column family "default"          -> user keys and values (arbitrary bytes)
     column family "raft-meta"        -> "last-applied" -> (term, index) record
```

Every record is `[type][format version][payload][CRC32]` (`RecordCodec`).

### Recovery checks (refuse to start, never reset)

Opening `RocksDbRaftStorage` scans the whole log. It throws `PersistenceException` ("refusing to
start") on any of these:
- a checksum mismatch, or a truncated record;
- the wrong record type under a key;
- a gap in indexes, or an entry stored under the wrong index;
- decreasing terms;
- log entries without term/vote metadata;
- a last entry from a term later than the stored term.

A damaged directory is therefore never mistaken for a fresh node. Treating it as fresh could
mean voting twice or forgetting acknowledged entries.

Both databases use RocksDB's `TolerateCorruptedTailRecords` WAL recovery mode. Per RocksDB's
documentation, it tolerates an incomplete record at the end of the WAL (a write that never
finished, so it was never acknowledged) and reports other corruption as an error. The default mode
(point-in-time recovery) would instead silently stop replaying at the first damaged record,
dropping acknowledged writes after it.

`RaftNode` adds cross-checks at startup. It refuses to start if:
- the state machine has applied an index beyond the end of the log, which means applied entries are
  missing;
- the applied entry's term differs from the log's term at that index.

### Single ownership

`NodeStorage.open` takes an exclusive OS lock on `node.lock` before opening either database. A
second open fails:
- from another process, because the lock is held;
- from the same JVM, because Java detects the overlapping lock.

The OS releases the lock when a process dies, so restart after a crash needs no cleanup. Each
RocksDB database also holds its own `LOCK`. These are advisory locks: a program that ignores them is
not stopped.

## State machine and the apply boundary

`RocksDbStateMachine.apply(entry)`:
1. rejects anything but `lastApplied + 1` (no skips, no repeats);
2. writes the command's effect into the `default` column family. That's a put, a delete, or
   nothing for a `NO_OP`;
3. writes the new `(term, index)` marker into `raft-meta`;
4. commits steps 2 and 3 as **one synced `WriteBatch`**.

Column families are separate key spaces in one database. A user key named `last-applied` lands in
`default` and cannot touch the marker (`userKeysCannotCollideWithTheInternalMarker`).

Crash points around an apply, and what recovery does:

| Crash point | On disk after restart | What happens | Test |
| --- | --- | --- | --- |
| Before the apply batch is written | Data and marker both old | Entry is applied again after commitment is relearned | `RaftReplicationTest.applyFailingBeforeTheWriteIsAppliedAfterRestartWithoutSkipping`; simulation `followerWhoseApplyFails...[FAIL_BEFORE]` |
| After the batch is written, before the node records it | Data and marker both new | Marker says applied, so it is not applied again | `applyFailingAfterTheWriteIsNotReappliedAfterRestart`; simulation `[FAIL_AFTER]` |
| In the middle of the batch | Impossible: the batch is atomic | none | `RaftStorageCrashIT` (process kill) |

This is **atomic local apply tracking**: each committed index is applied exactly once per node.
It is **not** exactly-once client semantics. If a client retries a command after an `UNKNOWN`
outcome, the retry is a new log entry and is applied too. Deduplication by request ID is a later
milestone.

## commitIndex after a restart

`commitIndex` is **not persisted**. On restart, `commitIndex = lastApplied` taken from the state
machine's durable marker. Applied entries are known to be committed, because only committed entries
are ever applied. Everything after that is relearned:
- a follower learns it from the leader's `leaderCommit`, limited to the prefix that leader verified;
- a node that becomes leader commits its `NO_OP`.

A log entry's mere presence never proves commitment. A restarted former leader may hold uncommitted
entries that the new leader replaces. That is exactly what the demo shows for `PUT user:4=dave`.
Mutating the restart to `commitIndex = last log index` is caught by the tests.

## Client outcomes

`RaftNode.propose(command)` returns immediately:

- **`Accepted(index, term)`**: the leader durably appended the entry and started replicating it.
  This is **not** success.
- **`NotLeader(hint)`**: nothing was appended. The hint is the leader this node knows of, if any.
  It may be empty: a follower learns the leader only from its AppendEntries.
- **`Unavailable`**: the node is halted; nothing was appended.
- **`OutcomeUnknown`**: persisting the entry failed and the node halted. The entry may or may not
  be in its log.

The final result arrives later through `drainOutcomes()`:

- **`COMMITTED`**: a majority stored it, it is committed, and this leader applied it.
- **`UNKNOWN`**: this node stepped down or halted first. A later leader may still commit the entry,
  or replace it. It is deliberately not reported as "failed". The simulator also reports `UNKNOWN`
  to a client whose node crashed.

**Reads.** There is no client read path in 2B. `StateMachine.diagnosticContents()` and the demo's
"diagnostic local reads" inspect one node's state, with no guarantee of being current. The demo
shows a follower lagging behind. Linearizable reads (a read index or leader leases) are deferred.

## Invariants checked in every simulated history

`Simulation` checks these after every single node step, and throws with the seed and tick if one
breaks:

1. **Election Safety:** at most one leader per term. Terms never decrease, including across
   restarts. No node votes twice in a term. In-memory term and vote equal storage.
2. **Log Matching:** a node's log is rechecked whenever it changes. Every (index, term) must
   identify the same prefix everywhere.
3. **Committed agreement:** every node that considers index i committed has the same entry there.
   This checks Leader Completeness and State Machine Safety from the outside.
4. `commitIndex` never decreases within a node incarnation. `lastApplied` never decreases even
   across restarts. `lastApplied` equals the state machine's durable marker.
5. **Only committed entries are applied, in order:** each applied entry equals the committed entry
   at its index, and indexes are consecutive.
6. **Client successes:** every `COMMITTED` outcome is in the committed history at its index, with
   the same term and command.
7. No node halts unless a failure was injected into it.

After faults heal, `assertConverged` requires:
- every node is up and has applied the same, highest commit index;
- every node's state equals the state obtained by replaying the committed history.

This accounts for later overwrites and deletes. A value written by an acknowledged `PUT` is not
expected to remain current if a later committed command changed it. What must hold is that the
`PUT` is in the committed history and the state equals the replay.

## Tests and what they prove

`./mvnw verify` runs everything. These are the counts from the final run during development:
- **501 unit and simulation tests** (Surefire);
- **4 process-level tests** (Failsafe).

All passed. That includes all original milestone 1 tests (28 unit and 3 process-level) and the 2A
election tests. The 2A election tests were adapted only where the protocol changed: heartbeats now
carry log fields, a new leader's first message carries its `NO_OP`, a leader holding a `NO_OP`
rightly refuses a candidate with an empty log, and fixtures became real logs.

| Test class | Count | What it checks |
| --- | --- | --- |
| `RaftReplicationTest` | 24 | Each follower and leader rule above; proposal outcomes; storage and apply failures before and after the write; restart relearning commitment; refusal to start on inconsistent applied state |
| `Figure8Test` | 3 | The current-term commitment restriction and why it is needed |
| `RaftNodeTest` | 31 | 2A election rules on the 2B message format |
| `RaftStorageContractTest` (in-memory and RocksDB) | 6 + 6 | Atomic term+truncate+append, rule checks, binary round trip, reopen |
| `RocksDbRaftStorageTest` | 9 | Flipped bit, truncated record, gap, wrong index, wrong record type, missing metadata: all refuse to start; second open refused; closed storage refuses use |
| `RocksDbStateMachineTest` | 5 | Atomic data+marker, strict order, key-space separation, corrupt marker, second open |
| `NodeStorageTest` | 2 | Lock exclusion; a single-voter node's log and state survive a real reopen |
| `CommandTest` | 4 | Limits equal the single-node API's; binary value semantics; `NO_OP` is internal |
| `ReplicationSimulationTest` | 38 | Pipelined writes in order (3 and 5 voters × 10 seeds); follower rejection; isolated former leader cannot commit and its entries are replaced; catch-up across many batches; lossy/duplicating/reordering network; RocksDB-backed cluster with real reopens; injected storage and apply failures; leader append failure |
| `ReplicationChaosTest` | 80 | Seeded histories (3 and 5 voters × 40 seeds) of writes, crashes, restarts, partitions, network changes, and injected failures, then repair, progress, and convergence |
| `ReplicationDemoTest` | 3 | The demo succeeds, is deterministic apart from its temp path, and deletes its temp directory |
| `RaftStorageCrashIT` | 1 | Process-level SIGKILL during atomic writes, three rounds (below) |

**Activity in the chaos histories.** These counts come from the deterministic seeds, measured once
during development by printing per-history statistics. They are counts, not performance figures.
On average per history:
- about 1,800 messages delivered, 1,300 dropped, and 500 duplicated;
- about 13 leader elections and 97 proposals;
- about 26 acknowledged writes (at least 3 in every history), about 10 `UNKNOWN` outcomes, and
  about 46 committed entries.

### The process-crash test, and its limits

`RaftStorageCrashIT` starts a separate JVM (`StorageCrashWorker`) on a real node directory. The
worker writes batch after batch: each batch is a new term plus three entries in one `StorageWrite`,
and every fourth batch also replaces the two newest unapplied entries. Applies lag one batch
behind. The worker prints a line after each completed write and apply.

The test kills the worker with SIGKILL at an arbitrary moment, reopens the directory, and checks:
- the recovered log is exactly the log the worker had written after the recovered term's batch.
  Term and entries changed together; there is no half-applied batch;
- every write that printed its acknowledgement survived;
- the applied marker matches the log, and the applied data is exactly the replay of entries up to
  the marker. Nothing was skipped or applied twice;
- the dead process's lock is gone.

It repeats this three times on the same directory.

**What it does not prove:** SIGKILL destroys the process, not the operating system's page cache.
This would pass even if fsync did nothing. Power loss, kernel crashes, and drives that ignore flush
requests are not tested. As in milestone 1, macOS `fsync` does not flush the drive's own cache.

### Evidence the tests can fail

During development, eleven 2B rules in `RaftNode` were deliberately broken one at a time, and the
Raft tests rerun. Every mutation was caught:

| Mutation | Caught by (examples) |
| --- | --- |
| Commit old-term entries by counting (remove the current-term check) | `Figure8Test.leaderDoesNotCommit...`, `inheritedEntriesCommitOnlyThroughACurrentTermEntry` |
| Follower commit up to its whole log instead of the verified prefix | `followerCommitIsLimitedToThePrefixTheLeaderVerified` |
| Blindly truncate after `prevLogIndex` on every append | `olderShorterAppendArrivingLateNeverTruncates`, `identicalDuplicateAppendIsHarmless`, `committedEntriesAreNeverTruncated`, demo |
| Let `matchIndex` move backwards | `staleOrDuplicatedResponsesCannotMoveProgressBackwards` |
| Act on uncorrelated failures | `rejectionMovesNextIndexBackOnceAndRetriesWhileDuplicatesAreIgnored` |
| Ignore the request term echoed in responses | `responsesToRequestsFromAnEarlierTermAreIgnored` |
| On restart, infer `commitIndex` from log length | Figure 8, apply-boundary tests, simulation and chaos invariants |
| No `NO_OP` on election | `RaftNodeTest`, `Figure8Test`, `inheritedEntries...` |
| Allow truncating committed entries | `committedEntriesAreNeverTruncated` |
| Acknowledge appends without persisting them | Simulation and chaos invariants (memory vs storage, log matching) |
| Report success for the wrong entry before applying it | Simulation, chaos, demo (acknowledged-request checks) |

This was a manual check. There is no mutation-testing tool in CI.

## Demonstration

```bash
./mvnw -q -DskipTests package
java -jar target/quantafs.jar replication-demo                    # 3 voters, seed 7
java -jar target/quantafs.jar replication-demo --voters 5 --seed 2
java -jar target/quantafs.jar replication-demo --keep             # keep the RocksDB files to inspect
java -jar target/quantafs.jar replication-demo --help
```

Each run creates a fresh temporary directory, with one sub-directory per node. It deletes the
directory at the end unless `--keep` is given. It never touches `./data`. Apart from the temporary
path, the output is identical on every run:

```text
QuantaFs Raft log-replication simulation (milestone 2B)
seed 7 | voters [n1, n2, n3] | quorum 2 | election timeout 10-19 ticks | heartbeat every 3 ticks | network delay 1-3 ticks
Per-node RocksDB storage (raft-log/ and state-machine/ per node) under:
  <temporary directory>/quantafs-replication-demo-<random>
Network and time are simulated in one process; no ports are opened.

== Phase 1: elect a leader ==
[tick   14] n3 FOLLOWER -> CANDIDATE  term 1  (election timeout after 14 ticks; votes for itself)
[tick   16] n1 term 0 -> 1  (higher term from n3)
[tick   16] n1 votes for n3 in term 1
[tick   16] n2 term 0 -> 1  (higher term from n3)
[tick   16] n2 votes for n3 in term 1
[tick   17] n3 CANDIDATE -> LEADER  term 1  votes 2/3 [n1, n3], needed 2
[tick   17] n3 log: appended [1:NO_OP(t1)]; now ends at index 1
[tick   19] n1 follows leader n3 in term 1
[tick   19] n1 log: appended [1:NO_OP(t1)]; now ends at index 1
[tick   19] n2 follows leader n3 in term 1
[tick   19] n2 log: appended [1:NO_OP(t1)]; now ends at index 1
[tick   19] => n3 leads term 1. Its first entry is a NO_OP of term 1: only entries from the leader's own term can be committed by counting replicas.
[tick   20] n3 commitIndex 0 -> 1
[tick   20] n3 applied [1:NO_OP(t1)] to its state machine
  node role      term  lastLogIndex  commitIndex  lastApplied
  n1   FOLLOWER  1     1             0            0
  n2   FOLLOWER  1     1             0            0
  n3   LEADER    1     1             1            1

== Phase 2: client writes go to the leader ==
[tick   20] client -> n3: PUT user:1=alice accepted at index 2 (term 1); waiting for commit
[tick   20] n3 log: appended [2:PUT user:1=alice(t1)]; now ends at index 2
[tick   21] n1 log: appended [2:PUT user:1=alice(t1)]; now ends at index 2
[tick   21] n1 commitIndex 0 -> 1
[tick   21] n1 applied [1:NO_OP(t1)] to its state machine
[tick   21] n2 log: appended [2:PUT user:1=alice(t1)]; now ends at index 2
[tick   21] n2 commitIndex 0 -> 1
[tick   21] n2 applied [1:NO_OP(t1)] to its state machine
[tick   23] n3 commitIndex 1 -> 2
[tick   23] n3 applied [2:PUT user:1=alice(t1)] to its state machine
[tick   23] n3 -> client: index 2 committed and applied; PUT user:1=alice OK
[tick   23] client -> n3: PUT user:2=bob accepted at index 3 (term 1); waiting for commit
[tick   23] n3 log: appended [3:PUT user:2=bob(t1)]; now ends at index 3
[tick   24] n1 log: appended [3:PUT user:2=bob(t1)]; now ends at index 3
[tick   24] n1 commitIndex 1 -> 2
[tick   24] n1 applied [2:PUT user:1=alice(t1)] to its state machine
[tick   25] n2 log: appended [3:PUT user:2=bob(t1)]; now ends at index 3
[tick   25] n2 commitIndex 1 -> 2
[tick   25] n2 applied [2:PUT user:1=alice(t1)] to its state machine
[tick   26] n3 commitIndex 2 -> 3
[tick   26] n3 applied [3:PUT user:2=bob(t1)] to its state machine
[tick   26] n3 -> client: index 3 committed and applied; PUT user:2=bob OK
[tick   26] client -> n3: PUT user:1=alice-v2 accepted at index 4 (term 1); waiting for commit
[tick   26] n3 log: appended [4:PUT user:1=alice-v2(t1)]; now ends at index 4
[tick   27] n1 log: appended [4:PUT user:1=alice-v2(t1)]; now ends at index 4
[tick   27] n1 commitIndex 2 -> 3
[tick   27] n1 applied [3:PUT user:2=bob(t1)] to its state machine
[tick   28] n2 log: appended [4:PUT user:1=alice-v2(t1)]; now ends at index 4
[tick   28] n2 commitIndex 2 -> 3
[tick   28] n2 applied [3:PUT user:2=bob(t1)] to its state machine
[tick   29] n3 commitIndex 3 -> 4
[tick   29] n3 applied [4:PUT user:1=alice-v2(t1)] to its state machine
[tick   29] n3 -> client: index 4 committed and applied; PUT user:1=alice-v2 OK
[tick   29] client -> n3: DELETE user:2 accepted at index 5 (term 1); waiting for commit
[tick   29] n3 log: appended [5:DELETE user:2(t1)]; now ends at index 5
[tick   30] n1 log: appended [5:DELETE user:2(t1)]; now ends at index 5
[tick   30] n1 commitIndex 3 -> 4
[tick   30] n1 applied [4:PUT user:1=alice-v2(t1)] to its state machine
[tick   31] n3 commitIndex 4 -> 5
[tick   31] n3 applied [5:DELETE user:2(t1)] to its state machine
[tick   31] n3 -> client: index 5 committed and applied; DELETE user:2 OK
  node role      term  lastLogIndex  commitIndex  lastApplied
  n1   FOLLOWER  1     5             4            4
  n2   FOLLOWER  1     4             3            3
  n3   LEADER    1     5             5            5

== Phase 3: a follower refuses client writes ==
[tick   31] client -> n1: PUT user:9=ignored REJECTED: n1 is not the leader (hint: n3)

== Phase 4: the leader is cut off and accepts a write it cannot commit ==
[tick   31] PARTITION [n3] | [n1, n2] (in-flight messages across groups are lost)
[tick   31] client -> n3: PUT user:4=dave accepted at index 6 (term 1); waiting for commit
[tick   31] n3 log: appended [6:PUT user:4=dave(t1)]; now ends at index 6
[tick   44] n1 FOLLOWER -> CANDIDATE  term 2  (election timeout after 14 ticks; votes for itself)
[tick   46] n2 FOLLOWER -> CANDIDATE  term 2  (election timeout after 16 ticks; votes for itself)
[tick   46] => n3 still believes it leads term 1, but index 6 exists only in its own log (1 of 3 nodes), so it is not committed and the client has no answer

== Phase 5: crash the cut-off leader; the others elect a new one ==
[tick   46] CRASH n3 (was LEADER in term 1): volatile state discarded; its RocksDB files are closed and stay on disk
[tick   46] n3 -/-> client: lost connection; outcome of PUT user:4=dave is UNKNOWN
[tick   46] HEAL network: all nodes can reach each other again
[tick   60] n2 election for term 2 failed: 1/3 votes [n2], needed 2
[tick   60] n2 CANDIDATE -> CANDIDATE  term 3  (election timeout after 14 ticks; votes for itself)
[tick   61] n1 CANDIDATE -> FOLLOWER  term 3  (saw higher term 3 from n2)
[tick   70] n1 FOLLOWER -> CANDIDATE  term 4  (election timeout after 10 ticks; votes for itself)
[tick   73] n2 CANDIDATE -> FOLLOWER  term 4  (saw higher term 4 from n1)
[tick   73] n2 votes for n1 in term 4
[tick   76] n1 CANDIDATE -> LEADER  term 4  votes 2/3 [n1, n2], needed 2
[tick   76] n1 log: appended [6:NO_OP(t4)]; now ends at index 6
[tick   77] n2 follows leader n1 in term 4
[tick   77] => n1 leads term 4 and appended its own NO_OP
[tick   77] client -> n1: PUT user:3=carol accepted at index 7 (term 4); waiting for commit
[tick   77] n1 log: appended [7:PUT user:3=carol(t4)]; now ends at index 7
[tick   81] n2 log: appended [5:DELETE user:2(t1), 6:NO_OP(t4), 7:PUT user:3=carol(t4)]; now ends at index 7
[tick   81] n2 commitIndex 3 -> 4
[tick   81] n2 applied [4:PUT user:1=alice-v2(t1)] to its state machine
[tick   84] n1 commitIndex 4 -> 7
[tick   84] n1 applied [5:DELETE user:2(t1), 6:NO_OP(t4), 7:PUT user:3=carol(t4)] to its state machine
[tick   84] n1 -> client: index 7 committed and applied; PUT user:3=carol OK
  node role      term  lastLogIndex  commitIndex  lastApplied
  n1   LEADER    4     7             7            7
  n2   FOLLOWER  4     7             4            4
  n3   DOWN (files closed on disk)

== Phase 6: committed data survived the crash (diagnostic local reads) ==
  n1  lastApplied 7  {user:1=alice-v2, user:3=carol}
  n2  lastApplied 4  {user:1=alice-v2, user:2=bob}
  A follower can lag behind the leader: it applies an entry only after a later message tells it the leader's commitIndex covers that entry.

== Phase 7: restart the old leader from its files; its uncommitted entry is replaced ==
[tick   84] RESTART n3: reloaded term 1, votedFor n3, log ends at index 6, lastApplied 5 by reopening its RocksDB files; commitIndex restarts at lastApplied; starts as FOLLOWER
[tick   85] n3 term 1 -> 4  (higher term from n1)
[tick   85] n3 follows leader n1 in term 4
[tick   85] n3 log: replaced conflicting uncommitted entries; now ends at index 7 (term 4)
[tick   85] n3 commitIndex 5 -> 7
[tick   85] n3 applied [6:NO_OP(t4), 7:PUT user:3=carol(t4)] to its state machine
[tick   86] n2 commitIndex 4 -> 7
[tick   86] n2 applied [5:DELETE user:2(t1), 6:NO_OP(t4), 7:PUT user:3=carol(t4)] to its state machine
[tick   95] => n3 caught up: applied through index 7

== Final state ==
  node role      term  lastLogIndex  commitIndex  lastApplied
  n1   LEADER    4     7             7            7
  n2   FOLLOWER  4     7             7            7
  n3   FOLLOWER  4     7             7            7
Committed log (identical on every node):
  1  term 1  NO_OP
  2  term 1  PUT user:1=alice
  3  term 1  PUT user:2=bob
  4  term 1  PUT user:1=alice-v2
  5  term 1  DELETE user:2
  6  term 4  NO_OP
  7  term 4  PUT user:3=carol
Key-value state per node (diagnostic local reads):
  n1  {user:1=alice-v2, user:3=carol}
  n2  {user:1=alice-v2, user:3=carol}
  n3  {user:1=alice-v2, user:3=carol}
Client view:
  PUT user:1=alice         via n3: COMMITTED
  PUT user:2=bob           via n3: COMMITTED
  PUT user:1=alice-v2      via n3: COMMITTED
  DELETE user:2            via n3: COMMITTED
  PUT user:9=ignored       via n1: rejected (not leader)
  PUT user:4=dave          via n3: UNKNOWN
  PUT user:3=carol         via n1: COMMITTED
  PUT user:4=dave was reported UNKNOWN, not failed; here it was never committed and was replaced on n3
Checks: at most one leader per term; logs match; committed entries agree; every node applied the committed log in order; 5 acknowledged writes are in the committed history
```

### How to read it

- **Phase 1.**
  - `n3` wins term 1 and immediately appends a `NO_OP` at index 1.
  - It commits that entry (tick 20) when `n1`'s acknowledgement makes 2 of 3.
  - The followers still show `commitIndex 0`. They learn about the commit from the next
    AppendEntries.
- **Phase 2.**
  - Each write follows the [request flow](#request-flow): the leader appends it, the followers
    append it, a majority acknowledges, the leader commits and applies it, then reports `OK`.
  - The followers' commit and apply lines trail by one round, because they learn `leaderCommit`
    from the next message.
  - At the end, `n2` happens to be one entry behind on its log. That's normal; it catches up later.
- **Phase 3.** `n1` refuses the write and points the client to `n3`. Nothing is appended.
- **Phase 4.**
  - `n3` is cut off and accepts `PUT user:4=dave` at index 6, but only it stores the entry.
  - It never reaches a majority, so `n3` neither commits it nor answers the client, even though
    `n3` still believes it leads term 1.
- **Phase 5.**
  - **The crash.** `n3` crashes, and its client gets `UNKNOWN`, not "failed".
  - **The election.** `n1` and `n2` split their votes in term 2, try again, and `n1` wins term 4.
    `n1`'s log (index 5) was more up to date than `n2`'s (index 4).
  - **Catch-up.** `n2` receives entries 5, 6, and 7 in one batch.
  - **Commitment.** Index 5 is a term-1 entry. It is committed only together with index 7, a
    term-4 entry (`commitIndex 4 -> 7`). That's the current-term rule.
- **Phase 6.**
  - The committed writes survived the leader crash.
  - `n2` still shows the older state, because it has not yet learned that 5-7 are committed.
    These are diagnostic local reads, not consistent reads.
- **Phase 7.**
  - **Reopen.** `n3` reopens its RocksDB files. It has term 1, a log ending at 6, and
    `lastApplied` 5. Its `commitIndex` restarts at 5, not 6: the log entry at 6 proves nothing.
  - **Conflict repair.** `n1`'s AppendEntries conflicts at index 6 (term 1 vs term 4). Index 6 is
    above `n3`'s commit index, so `n3` replaces it with the new leader's entries. `dave` is gone.
- **Final state.**
  - All three nodes hold the identical committed log and identical data.
  - `user:2` is absent because it was deleted at index 5. Correctness here means the state equals
    a replay of the committed log, not that every acknowledged value is still current.

## Limitations

- **One process, simulated network.** No inter-node transport, no real timers, no multi-machine
  availability claims.
- **No client read path.** Only diagnostic local reads. Linearizable reads are deferred.
- **No exactly-once semantics.** A retry after `UNKNOWN` can apply a command twice.
- **No snapshots or compaction.** The log grows without bound. Opening storage scans the whole log,
  and the node keeps every entry's term in memory.
- **Each storage write is fsynced individually.** There is no batching of syncs across messages. No
  throughput or latency has been measured, and none is claimed.
- **No PreVote.** A node that churned through terms while partitioned can depose a healthy leader
  when it returns. The chaos test therefore retries the final write like a real client would.
- **Durability assumptions** as in milestone 1: fsync must reach stable storage, and macOS fsync
  does not flush the drive cache. Power loss is not tested.
- **Fixed membership.**

## Where real networking fits next

The core needs only a driver. A node process would:
1. open `NodeStorage` for its directory (this takes the lock);
2. run one thread that owns the `RaftNode`:
   - call `tick()` on a timer (for example every few milliseconds);
   - pass each received message to `receive()`;
   - send whatever comes back. `RaftNode` is single-threaded, so all calls go through this thread;
3. expose a gRPC service for `RequestVote` and `AppendEntries`, with protobuf messages mirroring
   the records in `raft/`, and call peers through gRPC stubs. Persistence already completes inside
   `receive()` before replies are returned, so the transport needs no extra durability logic;
4. route the public `KeyValueService.Put/Delete`:
   - on the leader, call `propose` and wait for `COMMITTED`;
   - on a follower, answer with the leader hint;
   - map `UNKNOWN` to an "outcome unknown" error.

Reads would then need a read index or leases, request IDs would add deduplication, and snapshots
would bound the log. After that come containers and Kubernetes with persistent volumes.
