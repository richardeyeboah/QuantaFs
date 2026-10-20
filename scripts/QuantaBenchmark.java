import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import io.github.richardeyeboah.quantafs.cluster.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Closed-loop workload using persistent channels; excludes CLI journal fsync. */
public class QuantaBenchmark {
    record Sample(int worker, String operation, long nanos, boolean success) {}
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("seeds workers ops-per-worker warmup-per-worker value-bytes");
        var seeds = Arrays.stream(args[0].split(",")).map(NetworkEndpoint::parse).toList();
        int workers = Integer.parseInt(args[1]), count = Integer.parseInt(args[2]);
        int warmup = Integer.parseInt(args[3]), size = Integer.parseInt(args[4]);
        if (workers < 1 || workers > 16 || count < 2 || count % 2 != 0 || warmup < 0 || warmup % 2 != 0 || size < 1 || size > 1048576)
            throw new IllegalArgumentException("invalid workload parameters");
        var barrier = new CyclicBarrier(workers);
        var pool = Executors.newFixedThreadPool(workers);
        var futures = new ArrayList<Future<List<Sample>>>();
        long[] starts = new long[workers], ends = new long[workers];
        String runId = UUID.randomUUID().toString();
        try {
            for (int w = 0; w < workers; w++) {
                final int worker = w;
                futures.add(pool.submit(() -> {
                    var samples = new ArrayList<Sample>();
                    String id = "bench-" + runId + "-" + worker;
                    var value = ByteString.copyFromUtf8("x".repeat(size));
                    long sequence = 1;
                    try (var client = new ClusterClient(seeds, Duration.ofSeconds(20))) {
                        for (int i = -warmup; i < count; i++) {
                            if (i == 0) { barrier.await(60, TimeUnit.SECONDS); starts[worker] = System.nanoTime(); }
                            String operation = i % 2 == 0 ? "put" : "get";
                            var key = ByteString.copyFromUtf8(id + ":" + (Math.floorDiv(i, 2) % 32));
                            long start = System.nanoTime();
                            boolean success = false;
                            try {
                                if (operation.equals("put")) {
                                    client.put(PutRequest.newBuilder().setKey(key).setValue(value)
                                        .setClientId(id).setSequence(sequence++).build());
                                } else {
                                    var result = client.get(GetRequest.newBuilder().setKey(key).build());
                                    if (!result.getFound() || !result.getValue().equals(value))
                                        throw new IllegalStateException("read-back mismatch");
                                }
                                success = true;
                            } finally {
                                if (i >= 0) samples.add(new Sample(worker, operation, System.nanoTime() - start, success));
                            }
                        }
                        ends[worker] = System.nanoTime();
                    }
                    return samples;
                }));
            }
            var all = new ArrayList<Sample>();
            for (var future : futures) all.addAll(future.get(300, TimeUnit.SECONDS));
            long elapsed = Arrays.stream(ends).max().orElseThrow() - Arrays.stream(starts).min().orElseThrow();
            System.err.println("elapsed_ns=" + elapsed);
            System.out.println("worker,operation,latency_ns,success");
            for (var sample : all)
                System.out.printf(Locale.ROOT, "%d,%s,%d,%s%n", sample.worker(), sample.operation(), sample.nanos(), sample.success());
        } finally {
            pool.shutdownNow();
        }
    }
}
