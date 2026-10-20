# Local Kubernetes benchmark: October 20, 2026

This is an initial reproducible measurement of the current implementation on one laptop.
It is not a production capacity estimate, a maximum-throughput test, or a multi-host result.

## Workload and measurement

- Three Raft voters on Docker Desktop kubeadm Kubernetes v1.36.1, Linux ARM64 containers.
- macOS 26.6.2 ARM64 host; Docker allocated six CPUs and 4,106,870,784 bytes of memory.
- Each server limited to one CPU / 1 GiB; the client Pod limited to 250 millicores / 256 MiB.
- Persistent local hostpath volumes; existing state retained across trials. QuantaFs syncs its WAL
  on durable writes. Snapshots and regular health probes remain enabled.
- Closed-loop workload: each worker submits one operation at a time using persistent gRPC channels.
  Each worker has its own client ID and consecutive mutation sequence numbers.
- 50% PUT and 50% GET, alternating; 512-byte values. Keys contain a run UUID, worker ID, and a
  rotating slot. Each GET checks the exact value from that worker's preceding PUT.
- One and four concurrent workers, three trials each, 1,000 measured operations per trial.
  Each worker first performs 100 unmeasured warm-up operations.
- Latency uses the client's monotonic clock around the complete logical call, including leader
  probing and any internal retries. It excludes Java startup, channel/client construction, warm-up,
  class copying, and CSV output. GET includes the majority-committed read barrier.
- Throughput is completed measured operations divided by the span from the earliest worker start
  to the latest worker completion. Percentiles use nearest rank on raw per-call latency samples.
- The harness calls `ClusterClient` directly. It does **not** include the CLI's durable client-journal
  fsync or prove recovery of a benchmark client's own pending mutations after its crash.

## Results

All 6,000 measured operations completed successfully and all sampled reads matched their expected
values. This is an observed error count for this workload, not a general zero-error guarantee.

**One concurrent client:** trial throughput was 123.1, 135.7, and 172.9 operations/second
(median 135.7). Corresponding p95 latency was 27.74, 27.37, and 18.26 ms; p99 was
61.37, 64.17, and 55.06 ms.

**Four concurrent clients:** trial throughput was 95.2, 135.7, and 130.1 operations/second
(median 130.1). Corresponding p95 latency was 109.65, 83.95, and 91.57 ms; p99 was
302.05, 121.21, and 101.93 ms.

The additional clients did not consistently increase throughput in this setup and had higher tail
latency. CPU limits, client-side work, synced storage, background snapshots/probes, and shared-host
scheduling are possible contributors; this experiment does not isolate a bottleneck. The short trials
(about 6-11 measured seconds each) show substantial variation. Longer runs, stronger warm-up, profiling,
and controlled resource changes are needed before making scaling or capacity claims.

## Raw evidence and reproduction

[Raw samples and environment](../benchmarks/20261020T073222Z/) include every measured latency in
nanoseconds, per-trial stderr/timing, per-operation PUT/GET percentiles, Pod resource/image metadata,
and source/JAR hashes. The recorded Git parent is `9ca5296` with the Kubernetes work present as local
changes; the Pod image IDs and manifest fields identify the deployed workload. The report and those
changes are published together in the resulting milestone commit.

The final measurement ran without intentional fault injection. An earlier exploratory attempt overlapped
the recovery exercise and failed; it was excluded in full and is not used for these results. Both runners
now take the same local exclusive lock to prevent accidental overlap on this workstation.

With the local deployment already running:

```bash
./mvnw verify
python3 scripts/benchmark-kubernetes.py
```

The runner compiles `scripts/QuantaBenchmark.java` against the built JAR, copies temporary classes
into the client Pod, performs all six trials, and writes a new timestamped `benchmarks/` directory.
It removes only its own temporary classes, retains raw results, and does not remove data volumes.
A failed trial aborts the run rather than being counted as a successful measurement.

Do not run a demo, rebuild, heavy test suite, or other load generator concurrently. The lock coordinates
these two scripts on this workstation; it cannot prevent external cluster changes or unrelated host work.
Do not compare results across hardware, storage, resource limits, payloads, or workload mixes without
recording those differences. The benchmark adds dedicated keys and client sessions to the local database.

## What this does not measure

No saturation search, open-loop arrival-rate test, sustained soak, independent server capacity,
large-dataset performance, TLS overhead, disk-power-loss behavior, or failover-time distribution
is established. Five-voter behavior is tested for correctness in process tests; this report measures
only the local three-voter Kubernetes deployment.
