# Milestone 2A: deterministic Raft leader election

Milestone 2A implements the **leader-election half of Raft** as a deterministic, testable core,
plus a simulator that drives several copies of it through crashes, partitions, and message faults.
It is the first part of milestone 2, not a complete Raft implementation.

**What 2A is not:** there is no log replication, no commitment, and no state machine. The election
core is **not connected to the key-value server**. Client `Put`/`Get`/`Delete` still go straight
to the single-node RocksDB store from milestone 1, unchanged. Simulated nodes exchange messages
in memory; nothing here is a working distributed database.

Reference: Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm*
(2014), the [Raft paper](https://raft.github.io/raft.pdf). Rule names below follow its Figure 2.

## Implemented vs. deferred

| Implemented in 2A | Deferred |
| --- | --- |
| Follower / candidate / leader roles, monotonically increasing terms | Log entries, `AppendEntries` consistency check, commit index |
| Randomized election timeouts, candidate retries after split votes | Applying committed commands to RocksDB (state machine) |
| `RequestVote` with majority voting (including the candidate's own vote) | Networked transport between processes (gRPC) |
| Heartbeats as `AppendEntries` with no entries | Routing client writes through Raft |
| Durable `currentTerm` + `votedFor`, written atomically before dependent messages | Snapshots, log compaction |
| Election restriction (last-log term/index comparison) against a log-metadata interface | A real log behind that interface |
| Sender membership validation, stale/duplicate message handling | PreVote, check-quorum / leader leases, leadership transfer |
| Fail-closed halt on persistence failure | Membership changes |
| Deterministic simulator: delay, reorder, duplicate, drop, partition, crash, restart | Multi-process fault injection, power-loss testing |

## Key terms

- **Term:** a logical epoch number. Each election starts a new, higher term, and at most one leader
  can win a given term. Every message carries the sender's term, so a server that sees a higher term
  knows it is out of date.
- **Roles:** a **follower** passively answers requests. A **candidate** is campaigning to become
  leader. The **leader** sends heartbeats (and, from 2B, replicates the log).
- **Election timeout:** how long a follower waits without hearing from a leader before it becomes a
  candidate. It is **randomized** per node so that nodes rarely time out at the same moment.
- **Split vote:** several candidates divide the votes so nobody reaches a majority. Each candidate
  times out again (at a new random moment) and retries in a higher term.
- **Quorum (majority):** more than half of *all* voters, counting the candidate's vote for itself.
  3 voters need 2 votes, and 5 voters need 3. Two different majorities always share at least one voter,
  and each voter votes at most once per term, so two candidates can never both win the same term.
- **Heartbeat:** a periodic, empty `AppendEntries` message from the leader that tells followers
  "I am still here" and restarts their election timers.
- **`votedFor`:** whom this server voted for in `currentTerm`. Together with `currentTerm` it must be
  on stable storage before the server answers, or a crash and restart could let it vote twice.
- **Election restriction (section 5.4.1):** a voter refuses candidates whose log is less up to date
  than its own: a higher last-entry term wins, and with equal last terms the longer log wins. This is
  what will later guarantee that a new leader already has every committed entry.
- **Logical tick:** an abstract unit of time. The core counts ticks and never reads a clock.

## Architecture

```text
             tick()  ------------------>  +-------------------+  ---> List<RaftMessage> to send
   receive(RaftMessage) -------------->   |     RaftNode      |
                                          |  (pure decisions) |
                                          +---------+---------+
                                                    | save(term, votedFor) BEFORE returning
                                                    v           any message that depends on it
                                          ElectionStateStore  (FileElectionStateStore | InMemory...)
      RandomSource (seeded)  --> election timeouts
      LogMetadata (fixture)  --> last log (term, index) for the election restriction
```

| Package / class | Responsibility |
| --- | --- |
| `raft.RaftNode` | All election rules. Inputs: `tick()`, `receive(message)`. Output: messages to send. No threads, clocks, I/O, gRPC, or RocksDB. |
| `raft.RequestVote*`, `raft.AppendEntries*` | Immutable message records, validated on construction (term >= 1, not self-addressed). `RaftMessage` is a sealed interface, so the compiler checks every message type is handled. |
| `raft.ElectionStateStore` | Contract for durable `TermAndVote`. |
| `raft.FileElectionStateStore` | Durable implementation (one small checksummed file). |
| `raft.InMemoryElectionStateStore` | Test/simulation store with failure injection and a write history. |
| `raft.LogMetadata`, `raft.LogPosition` | The narrow boundary to the future log: last entry's term and index. |
| `raft.RandomSource`, `raft.SeededRandomSource` | Injected randomness; same seed, same timeouts. |
| `raft.sim.Simulation` | Deterministic cluster simulator and invariant checker. |
| `raft.sim.ElectionSafety` | Checks "at most one leader per term" over a recorded history. |
| `raft.sim.ElectionDemo` | The printable demonstration (`election-demo` subcommand). |

**Why this design:** consensus bugs typically appear only under specific interleavings of messages,
crashes, and timeouts. Because the core is a pure function of its inputs (plus its store), a test
can reproduce an exact interleaving, and a seed reproduces a whole randomized history. Tests never
sleep or race the wall clock. A future gRPC transport only has to call `tick()` on a timer, pass
received messages to `receive()`, and send what comes back.

## Rules and where they are tested

| Rule (paper) | Behavior | Tests |
| --- | --- | --- |
| Followers: election timeout | No heartbeat or granted vote within the timeout => become candidate | `RaftNodeTest.electionTimeoutPersistsNewTermAndSelfVoteThenRequestsVotes` |
| Candidates: on conversion | `currentTerm + 1`, vote for self, persist, reset timer, send `RequestVote` to every peer | same test; checks the store write precedes the returned messages |
| Randomized timeouts (5.2) | Timeout drawn from `[min, max)` via the injected `RandomSource` on every reset | `electionTimeoutIsDrawnFromInjectedRandomnessOverTheConfiguredRange` |
| Candidates: majority wins | Quorum counts the self-vote; a set counts each voter once | `majorityIncludesTheCandidatesOwnVote`, `duplicateGrantsFromOneVoterAreCountedOnce` |
| Candidates: timeout => new election | A candidate without a majority retries in a higher term | `candidateWhoseElectionTimesOutRetriesInANewTerm`; simulation `simultaneousCandidatesSplitTheVoteAndRetryInALaterTerm` (3 and 5 voters) |
| Stale responses | Responses from older terms, or after the election is decided, are ignored | `staleAndRejectingResponsesDoNotCount` |
| RequestVote receiver 1 | `term < currentTerm` => refuse, reply with own term | `rejectsRequestVoteFromLowerTermWithoutChangingState` |
| RequestVote receiver 2 | Vote only if `votedFor` is empty or the same candidate, and the log is up to date | `votesAtMostOncePerTermAndPersistsBeforeReplying`, `grantsOnlyToCandidatesWhoseLogIsAtLeastAsUpToDate` (5 cases), `refusingAStaleLogDoesNotUseUpTheVote`; simulation `onlyCandidatesWithUpToDateLogsCanWin` |
| All servers: higher term | Adopt the term, clear the vote, become follower; term and vote saved in one atomic write | `higherTermRequestVoteMakesLeaderStepDownAndVoteInOneAtomicSave`, `leaderStepsDownWhenAResponseRevealsAHigherTerm`, `candidateStepsDownWhenAVoteResponseRevealsAHigherTerm`; simulation `rejoiningNodeWithAHigherTermForcesTheLeaderToStepDown` |
| AppendEntries receiver 1 | Heartbeat with `term < currentTerm` is refused and does **not** reset the timer | `staleHeartbeatIsRejectedAndDoesNotResetTheTimer` |
| Candidates: AppendEntries from new leader | Same-term heartbeat turns a candidate into a follower | `candidateBecomesFollowerOnHeartbeatFromSameTermLeader` |
| Leaders: heartbeats | Immediately on election, then every `heartbeatIntervalTicks` | `leaderSendsHeartbeatsEveryIntervalAndNeverTimesOut`, `majorityIncludesTheCandidatesOwnVote` |
| Legitimate timer resets | Only a current-term heartbeat or a granted vote restarts a follower's timer | `heartbeatsFromTheCurrentLeaderPreventElections`, `grantingAVoteResetsTheTimerButRefusingDoesNot` |
| Persistent state | `currentTerm` and `votedFor` durable before replying; restart cannot double vote | `restartReloadsTheVoteAndRefusesASecondCandidateInTheSameTerm`, `FileElectionStateStoreTest.raftNodeRestartedOnTheSameDirectoryDoesNotVoteTwice`; simulation `nodeCrashedRightAfterVotingReloadsThatVote` |
| Membership (QuantaFs rule) | Messages from non-members or addressed to another node are dropped before they can affect any state | `messagesFromNonMembersOrForOtherNodesCannotAffectState` |
| Election Safety (Figure 3) | At most one leader per term | Every simulation run (checked after every node step), `ElectionChaosTest` (120 seeded fault histories), `ElectionSafetyTest` (the checker detects violations) |

## Durable election state

**When:** `currentTerm` and `votedFor` are written when starting an election (new term, vote for
self), when granting a vote, and when adopting a higher term seen in any request or response.
In every case the write completes **before** the node returns any message that depends on it. A
`RequestVoteResponse` granting a vote cannot exist unless that vote is durable.

**How (`FileElectionStateStore`):**

1. Encode `(magic, format version, term, votedFor)` followed by a CRC32 checksum.
2. Write it to `election-state.tmp` and `fsync` that file.
3. Atomically rename it over `election-state` (`ATOMIC_MOVE`). A crash leaves the old or the new
   file, never a mix.
4. `fsync` the directory, so the rename itself survives a crash.

Term and vote live in one file, so they change together in one atomic step. A leftover temp file
at startup is an interrupted write and is discarded.

**Fail closed:**

- A corrupt or truncated state file stops startup with an error. It is never treated as
  "term 0, no vote", because forgetting a vote could let the node vote twice in one term.
- If a save fails, the node **halts**. It returns no message for that input and ignores all later
  input. A halted leader stops sending heartbeats.
- The failed store instance also refuses further use, because its view of the disk is no longer
  trustworthy. Recovery is a restart, which re-reads whatever actually reached the disk.
- Both stores also refuse transitions Raft never makes: a lower term, or a different vote within the
  same term. That's a second line of defense behind the node's own logic.

**Assumptions and limits:**

- Durability assumes the OS and device honor `fsync`. As in milestone 1, macOS `fsync` does not
  flush the drive's own cache, and power loss is not tested.
- The store keeps one directory per node and takes no OS lock on it. Running two processes on one
  directory is unsupported. A future server process should hold a lock, as RocksDB does in milestone 1.
- The election state is deliberately separate from the RocksDB application data.

**`InMemoryElectionStateStore`** implements the same contract (verified by the shared
`ElectionStateStoreContractTest`). In the simulator, a crashed node's store object survives the
crash, which models its disk. The store can also be told to fail every save.

## Timer semantics in detail

- Followers and candidates count ticks. Reaching the timeout starts an election.
- The timer restarts, with a fresh random timeout, on exactly these events: a granted vote, a
  heartbeat from the current term's leader, starting an election, and leaving the leader or
  candidate role.
- A refused vote, a stale heartbeat, or a follower merely adopting a higher term does **not**
  restart it. Otherwise traffic from deposed leaders or hopeless candidates could postpone elections
  forever.
- The leader has no election timer, only a heartbeat counter.

## The log-metadata boundary

Voting needs only the last log entry's term and index, so the core depends on
`LogMetadata.lastEntry()` and nothing else. In 2A, callers supply fixed values: empty logs by
default, and chosen fixtures in tests (for example, a node whose log is behind cannot win).
`LogPosition` rejects impossible positions, and `RaftNode` refuses to start if the last log term
exceeds the persisted term. In 2B, a durable log will implement this interface. The comparison
code and its tests stay as they are.

## The simulator

`Simulation` owns logical time and a model network:

- **Delivery and delay:** each message is delivered after a random 1 to N ticks. Independent delays
  reorder messages.
- **Duplication and drops:** each message may be delivered twice or lost (configurable probabilities).
- **Partitions:** the network can be split into groups. Messages crossing groups are lost, including
  ones already in flight. `heal()` reconnects everything.
- **Crash and restart:** a crash discards the `RaftNode` and all its volatile state. A restart builds
  a new node over the same store.
- **Determinism:** one seed drives every random choice. The same seed and the same calls reproduce
  the same history, event for event (tested). There are no threads or sleeps.

After every single node step it throws if any of these breaks:
- at most one leader per term;
- a node's term never decreases, including across restarts;
- no node votes for two candidates in one term;
- a node's in-memory term and vote equal what is in its store, so memory never runs ahead of disk.

### Simulation scenarios (3 and 5 voters)

`ElectionSimulationTest`:
- initial election over 25 seeds per cluster size, and stable heartbeats over 2,000 ticks;
- leader crash, re-election, and the old leader rejoining as a follower (25 seeds per size);
- five voters losing two nodes, and split votes followed by a retry;
- an isolated old leader and healing; a minority partition and two survivors of five never
  electing a leader;
- higher-term step-down by a returning node, and long delays with duplication and drops;
- log freshness, a node crashing right after it voted, and a node whose disk fails going silent;
- determinism.

`ElectionChaosTest` runs 60 seeded histories per cluster size, with random crashes, restarts,
partitions, heals, and network changes. It then requires a leader once everything is repaired, and
checks every node's sequence of durable writes.

### Evidence the tests can fail

During development, nine rules were deliberately broken one at a time in `RaftNode` or
`ClusterConfig`, and the Raft tests rerun. Every mutation was caught:

| Mutation | Caught by (examples) |
| --- | --- |
| quorum computed as `n / 2` instead of `n / 2 + 1` | simulation and chaos (two leaders in one term) |
| vote granted regardless of an earlier vote | simulation, chaos, demo |
| log freshness check ignored | `onlyCandidatesWithUpToDateLogsCanWin` |
| stale vote responses counted | `staleAndRejectingResponsesDoNotCount`, chaos |
| persistence failure ignored (fail open) | the three `persistenceFailure...` unit tests, disk-failure simulation |
| timer reset on a refused vote | `grantingAVoteResetsTheTimerButRefusingDoesNot` |
| no step-down on a higher term | simulation and chaos |
| stale heartbeats accepted | isolation, delay, and chaos tests |
| sender membership not checked | `messagesFromNonMembersOrForOtherNodesCannotAffectState` |

This was a manual check, not an automated mutation-testing tool in CI.

## Nuances and limits

- **An isolated old leader does not know it was replaced.** If a leader is partitioned away, the
  majority elects a new leader in a higher term, while the old one keeps believing it leads its
  older term until a message with the higher term reaches it. The test
  `isolatedOldLeaderKeepsBelievingItLeadsUntilItHearsTheHigherTerm` asserts exactly this. Two nodes
  can think they are leader at the same moment, but never in the same term. Once replication exists
  (2B), the old leader cannot commit anything, because commitment needs a majority. Serving reads
  from it would still be unsafe without a read-index or lease mechanism, which is a later milestone.
- **Disruptive rejoin without PreVote.** A node that was cut off keeps raising its term. When it
  returns, its higher term forces the healthy leader to step down and causes a new election. This is
  correct Raft behavior but costs availability. The PreVote extension (Ongaro's thesis, section 9.6)
  would prevent it and is not implemented. The behavior is tested, not hidden.
- **Liveness is probabilistic.** Randomized timeouts make repeated split votes unlikely, not
  impossible. The tests show leaders emerging within fixed tick budgets for the tested seeds. They
  make no real-time claims; ticks are not milliseconds.
- **Defensive checks.** If a node ever observes two leaders in its current term, `RaftNode` throws
  `IllegalStateException` rather than continuing, because that indicates a bug or a misconfigured
  cluster.
- **Single-threaded.** `RaftNode` is not thread-safe; a future driver must call it from one thread.
- **No performance numbers** are claimed or measured.

## Demonstration

Build once, then run the deterministic demo:

```bash
./mvnw -q -DskipTests package
java -jar target/quantafs.jar election-demo              # 5 voters, seed 7
java -jar target/quantafs.jar election-demo --voters 3 --seed 3
java -jar target/quantafs.jar election-demo --help
```

Expected output for the default run (identical on every run):

```text
QuantaFs Raft leader-election simulation (milestone 2A)
seed 7 | voters [n1, n2, n3, n4, n5] | quorum 3 | election timeout 10-19 ticks | heartbeat every 3 ticks | network delay 1-3 ticks
Simulation of the election core only: no client data, no log replication.

== Phase 1: initial election ==
[tick   13] n4 FOLLOWER -> CANDIDATE  term 1  (election timeout after 13 ticks; votes for itself)
[tick   14] n3 term 0 -> 1  (higher term from n4)
[tick   14] n3 votes for n4 in term 1
[tick   14] n5 term 0 -> 1  (higher term from n4)
[tick   14] n5 votes for n4 in term 1
[tick   15] n1 term 0 -> 1  (higher term from n4)
[tick   15] n1 votes for n4 in term 1
[tick   15] n2 term 0 -> 1  (higher term from n4)
[tick   15] n2 votes for n4 in term 1
[tick   16] n4 CANDIDATE -> LEADER  term 1  votes 3/5 [n3, n4, n5], needed 3
[tick   17] n3 follows leader n4 in term 1
[tick   17] n5 follows leader n4 in term 1
[tick   17] => n4 leads term 1, acknowledged by a quorum

== Phase 2: 30 ticks of steady heartbeats ==
[tick   18] n1 follows leader n4 in term 1
[tick   19] n2 follows leader n4 in term 1
[tick   47] n4 is still leader of term 1; sent 40 heartbeats (AppendEntries with no entries) to 4 followers; no new elections

== Phase 3: crash the leader ==
[tick   47] CRASH n4 (was LEADER in term 1); its volatile state is gone, its durable store survives
[tick   56] n5 FOLLOWER -> CANDIDATE  term 2  (election timeout after 11 ticks; votes for itself)
[tick   57] n3 term 1 -> 2  (higher term from n5)
[tick   57] n3 votes for n5 in term 2
[tick   57] n1 FOLLOWER -> CANDIDATE  term 2  (election timeout after 10 ticks; votes for itself)
[tick   58] n2 term 1 -> 2  (higher term from n5)
[tick   58] n2 votes for n5 in term 2
[tick   61] n5 CANDIDATE -> LEADER  term 2  votes 3/5 [n2, n3, n5], needed 3; rejected by [n1]
[tick   62] n1 CANDIDATE -> FOLLOWER  term 2  (heartbeat from leader n5 of the same term)
[tick   62] n1 follows leader n5 in term 2
[tick   62] n2 follows leader n5 in term 2
[tick   62] => n5 leads term 2, acknowledged by a quorum

== Phase 4: restart the old leader ==
[tick   62] RESTART n4: reloaded term 1, votedFor n4 from its durable store; starts as FOLLOWER
[tick   63] n3 follows leader n5 in term 2
[tick   64] n4 term 1 -> 2  (higher term from n5)
[tick   64] n4 follows leader n5 in term 2

== Final state ==
node  role      term  votedFor  knownLeader
n1    FOLLOWER  2     n1        n5
n2    FOLLOWER  2     n5        n5
n3    FOLLOWER  2     n5        n5
n4    FOLLOWER  2     -         n5
n5    LEADER    2     n5        n5
Leaders by term: {1=n4, 2=n5}
Election safety: OK (no term had more than one leader)
```

How to read it:

- **Ticks 13 to 16:** `n4` times out first, votes for itself, and asks for votes. It becomes leader
  when its third vote arrives (`n3`, `n5`, and itself), which is a majority of 5. `n1` and `n2` also voted for
  it, but their votes arrived after it had already won.
- **Phase 2:** 30 ticks at one heartbeat round every 3 ticks gives 10 rounds to 4 followers, so 40
  heartbeats. No follower times out.
- **Phase 3:** after the crash, `n5` and then `n1` both become candidates for term 2. `n1` loses,
  because `n2` and `n3` had already voted for `n5`, and `n1`'s request to `n5` was refused. When
  `n5`'s heartbeat arrives, `n1` steps down.
- **Phase 4:** `n4` restarts with the term and vote it had saved (term 1, voted for itself). It learns
  term 2 from `n5`'s heartbeat and follows. In the final table its `votedFor` is `-` because it has
  not voted in term 2.

## Next: milestone 2B (log replication and ordered application)

Planned, not implemented:

- **Durable Raft log.** Append-only entries `(index, term, command)` with the same documented
  sync-before-acknowledge discipline as the election state, and a recovery contract. It will
  implement `LogMetadata`, replacing the fixtures.
- **Full AppendEntries.** Add `prevLogIndex`, `prevLogTerm`, `entries[]`, and `leaderCommit`.
  Followers reject entries that don't match their log at `prevLogIndex` and truncate conflicting
  suffixes. The leader tracks `nextIndex` and `matchIndex` per follower and backs off on rejection.
- **Commitment.** The leader marks an entry committed once a majority stores it, but only for
  entries from its own current term (section 5.4.2). Followers learn the commit index from
  `leaderCommit`.
- **Ordered state-machine application.** Committed entries are applied to the milestone-1
  `KeyValueStore` strictly in log order, exactly once per node, tracking `lastApplied` durably.
- **Testing.** The same deterministic simulator, extended with Log Matching, Leader Completeness,
  and State Machine Safety checks over generated histories.
- **Still later:** gRPC transport between processes, routing client writes through the leader,
  linearizable reads, client request deduplication, snapshots, and Kubernetes deployment.
