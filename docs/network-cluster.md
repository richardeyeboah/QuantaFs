# Milestone 3: real local processes over gRPC

This documents the network foundation. For the newer automatic retry client, snapshots,
log cleanup, and current limits, see [milestones 4–5](retries-and-snapshots.md).

QuantaFs now runs three or five independent Java server processes, with a separate RocksDB
consensus log and state machine per node. They communicate through TCP/gRPC on loopback.
The public Put/Get/Delete API is connected to Raft in `cluster-server` mode.
The original `server` command remains a single-node service.

## Run the complete demonstration

Prerequisites: JDK 21 and Python 3. From the project directory:

```bash
./mvnw verify
python3 scripts/cluster-demo.py
# Optional: five independent processes, keeping their temporary databases and logs
python3 scripts/cluster-demo.py --nodes 5 --keep
```

The script chooses unused local ports, creates temporary storage, starts each server, discovers a
leader, saves `order:1042=paid`, and reads it through a consensus barrier. It forcibly kills that
leader, discovers the replacement, reads the confirmed order, saves `city=Accra`, and restarts
the old leader from its disk. It waits for catch-up and prints each node's status. Leader IDs,
ports, terms, and indexes vary with scheduling; the checks do not assume a specific winner.

This is real networking and independent processes, not the deterministic simulator. All processes
still share one physical machine. It demonstrates process-failure tolerance, not survival of that
machine, its disk, or a power outage. The script cleans up only its own temporary directory unless
`--keep` is used; it never touches `./data` or manually created node directories.

## Run nodes manually

Open three terminals, enter the project directory in each, and use the same membership list:

```bash
# Terminal 1
java -jar target/quantafs.jar cluster-server --id n1 --peers n1=127.0.0.1:5101,n2=127.0.0.1:5102,n3=127.0.0.1:5103 --data-dir ./n1-data
# Terminal 2
java -jar target/quantafs.jar cluster-server --id n2 --peers n1=127.0.0.1:5101,n2=127.0.0.1:5102,n3=127.0.0.1:5103 --data-dir ./n2-data
# Terminal 3
java -jar target/quantafs.jar cluster-server --id n3 --peers n1=127.0.0.1:5101,n2=127.0.0.1:5102,n3=127.0.0.1:5103 --data-dir ./n3-data
```

In a fourth terminal, inspect nodes:

```bash
java -jar target/quantafs.jar cluster-status --target 127.0.0.1:5101
java -jar target/quantafs.jar cluster-status --target 127.0.0.1:5102
java -jar target/quantafs.jar cluster-status --target 127.0.0.1:5103
```

Find `role=LEADER` and use its port (5101 is just an example):

```bash
java -jar target/quantafs.jar client --target 127.0.0.1:5101 put order:1042 paid
java -jar target/quantafs.jar client --target 127.0.0.1:5101 get order:1042
```

A follower returns `UNAVAILABLE: not leader; leader=nX`. Check status again and choose the new
leader after a failure. The client does not automatically retry writes: an interrupted write may
already have committed. Request deduplication and automatic discovery are deferred.

Stop a manual node with Ctrl-C, then restart it with precisely the same command and data directory.
Do not share directories or change identities/ports in place. A persisted identity binds a node's
storage to the configured node ID and complete fixed membership. Incompatible configurations or
non-empty unidentified directories fail startup. This is not a membership-change mechanism.

## Request flow and guarantees

A network worker decodes and validates a request. All consensus access is then serialized on one
bounded event queue so networking threads never race the single-threaded Raft core.

- **Put/Delete:** validate command, append durably, replicate over gRPC, wait for majority
  commitment, apply the command locally, then respond. Admission is not confirmation.
- **Get:** append a fresh internal no-op and wait for its majority commitment and local application,
  then read the requested key on the same event loop. A no-op changes no user data. This provides
  a conservative linearizable read path: it is more expensive than read-index and grows the log.
  Followers never serve public local reads. An isolated old leader cannot complete the barrier.
- **Peer traffic:** `raft.proto` explicitly represents votes, log entries, append requests, and
  responses. A `Deliver` receipt confirms queue admission only; the actual Raft acknowledgement is
  a separately delivered protocol message, produced after the core's persistence/application steps.
- **Status:** node-local diagnostic observation, not proof of current majority leadership. A
  partitioned old leader can still report LEADER. Client requests must go through Raft.

The runtime has 128 queued events, at most 128 core proposals awaiting resolution, and at most two
in-flight transport calls per peer. Excess admission is rejected and dropped peer transmissions are
retried by normal heartbeats. Peer calls have 500 ms deadlines. Heartbeats normally run every
150 ms, with election timeouts drawn from 500–950 ms; these are settings, not measured failover
claims. Slow synchronous disk operations can delay the loop and trigger elections.

Client requests have a five-second server confirmation budget, in addition to the client's own
configured deadline. Deadline/cancellation/leadership loss means **unknown outcome**, not a
promise that the write failed. Timed-out client callbacks are discarded, but the core keeps tracking
unresolved proposals, counting them against admission, until commitment or leadership loss.

## Disk layout and shutdown

```text
n1-data/
  node.lock       exclusive OS lock while the process owns the directory
  identity        node ID and canonical fixed membership
  raft-log/       RocksDB: term, vote, durable replicated log
  state-machine/ RocksDB: application values and atomic lastApplied marker
```

Shutdown stops the network and clock, drains the event queue, then closes the databases. Native
storage handles are never closed while an event is still using them. An unresponsive event loop
leaves storage open rather than closing handles concurrently. The OS releases ownership locks
on process termination. Existing durability assumptions and lack of power-loss evidence still apply.

## Verification

`ClusterProcessIT` launches actual packaged Java processes for both three- and five-node groups.
It checks binary values, empty values, deletion/missing keys, follower read rejection, input
validation, membership mismatch rejection, leader SIGKILL and replacement, restarted follower
catch-up, reads/writes refused without a majority, recovery after a majority returns, full-cluster
SIGKILL/reopen, and directory ownership exclusion. These complement the deterministic fault
histories and storage crash tests from milestone 2.

Codec tests round-trip all four protocol message kinds and reject malformed commands. Identity
tests cover same-process ownership, incompatible identities, and refusing unidentified databases.

## Milestone 3 local validation (historical checkpoint)

On October 20, 2026, `./mvnw verify` passed 506 unit/in-process/simulation tests and six
process-level integration tests: **512 total, zero failures, zero errors, zero skipped**.
Both three-process and five-process scenarios completed leader termination, majority loss,
catch-up, and full-cluster restart checks. `python3 scripts/cluster-demo.py` also passed its
end-to-end order-survival demonstration. These are local results, not hosted CI results or
performance benchmarks.

## Remaining scope

Only loopback addresses are accepted: there is no TLS, authenticated peer identity, or client access
control. The membership key prevents accidental configuration mixing; it is not an authentication
secret. Do not expose these ports through forwarding or deploy this mode on an untrusted machine.

At the original milestone-3 checkpoint there were no automatic client retries or snapshots; those
are now implemented as described in `retries-and-snapshots.md`. Live membership changes,
remote hosts, Kubernetes deployment, systematic real-network partition injection, disk/power-loss
fault tests, or throughput/latency claims are included. Snapshot limits and resource admission bounds still require monitoring; this is not a production-ready service.
