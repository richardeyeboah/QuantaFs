package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.ElectionTiming;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.ProposeResult;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.RaftStorage;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Prints a reproducible log-replication scenario: elect a leader, replicate and apply client writes,
 * reject a write sent to a follower, crash the leader, elect a replacement, verify the committed data,
 * restart the old leader, and watch it catch up.
 *
 * <p>Each node has real RocksDB storage in its own directory under a fresh temporary directory, so the
 * crash and restart close and reopen files. Network and time are simulated; nothing listens on a
 * port. Apart from the temporary path, the same seed prints the same output.
 */
public final class ReplicationDemo {

    public static final String USAGE =
            """
            Usage: replication-demo [--seed <n>] [--voters <3|5>] [--keep]

            Deterministic in-process simulation of Raft log replication (milestone 2B) with
            real per-node RocksDB storage in a new temporary directory. Never touches ./data.

              --seed     simulation seed (default 7)
              --voters   cluster size, 3 or 5 (default 3)
              --keep     keep the temporary storage directory afterwards for inspection
            """;

    private static final int BUDGET = 1_000;

    private ReplicationDemo() {}

    public static int run(String[] args, PrintStream out, PrintStream err) {
        long seed = 7;
        int voters = 3;
        boolean keep = false;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--seed" -> seed = Long.parseLong(value(args, ++i));
                    case "--voters" -> voters = Integer.parseInt(value(args, ++i));
                    case "--keep" -> keep = true;
                    case "-h", "--help" -> {
                        out.print(USAGE);
                        return 0;
                    }
                    default -> throw new IllegalArgumentException("unknown option: " + args[i]);
                }
            }
            if (voters != 3 && voters != 5) {
                throw new IllegalArgumentException("--voters must be 3 or 5");
            }
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.print(USAGE);
            return 2;
        }

        Path base;
        try {
            base = Files.createTempDirectory("quantafs-replication-demo-");
        } catch (IOException e) {
            err.println("error: cannot create a temporary directory: " + e.getMessage());
            return 1;
        }
        try {
            return runScenario(seed, voters, base, out);
        } catch (RuntimeException e) {
            err.println("demo failed: " + e);
            return 1;
        } finally {
            if (keep) {
                out.println("Storage kept at " + base);
            } else {
                deleteRecursively(base);
            }
        }
    }

    private static int runScenario(long seed, int voters, Path base, PrintStream out) {
        String[] ids = IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toArray(String[]::new);
        ElectionTiming timing = ElectionTiming.DEFAULT;
        NetworkConfig network = NetworkConfig.RELIABLE;
        try (Simulation sim = Simulation.builder(ids).seed(seed).timing(timing).network(network)
                .rocksDb(base).listener(out::println).build()) {

            out.println("QuantaFs Raft log-replication simulation (milestone 2B)");
            out.printf("seed %d | voters %s | quorum %d | election timeout %d-%d ticks | heartbeat every %d ticks"
                            + " | network delay %d-%d ticks%n",
                    seed, List.of(ids), sim.cluster().quorum(), timing.electionTimeoutMinTicks(),
                    timing.electionTimeoutMaxTicks() - 1, timing.heartbeatIntervalTicks(),
                    network.minDelayTicks(), network.maxDelayTicks());
            out.println("Per-node RocksDB storage (raft-log/ and state-machine/ per node) under:");
            out.println("  " + base);
            out.println("Network and time are simulated in one process; no ports are opened.");

            out.println();
            out.println("== Phase 1: elect a leader ==");
            RaftNode leader = awaitLeader(sim, 0);
            out.printf("[tick %4d] => %s leads term %d. Its first entry is a NO_OP of term %d: only entries from"
                            + " the leader's own term can be committed by counting replicas.%n",
                    sim.now(), leader.id(), leader.currentTerm(), leader.currentTerm());
            awaitCommit(sim, leader, sim.storage(leader.id().value()).lastIndex());
            printTable(sim, ids, out);

            out.println();
            out.println("== Phase 2: client writes go to the leader ==");
            String leaderId = leader.id().value();
            for (Command command : List.of(
                    put("user:1", "alice"), put("user:2", "bob"), put("user:1", "alice-v2"), delete("user:2"))) {
                proposeAndWait(sim, leaderId, command);
            }
            printTable(sim, ids, out);

            out.println();
            out.println("== Phase 3: a follower refuses client writes ==");
            String follower = sim.liveNodes().stream().map(n -> n.id().value())
                    .filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();
            sim.propose(follower, put("user:9", "ignored"));

            out.println();
            out.println("== Phase 4: the leader is cut off and accepts a write it cannot commit ==");
            sim.isolate(leaderId);
            Simulation.ClientRequest stranded = sim.propose(leaderId, put("user:4", "dave"));
            sim.run(15);
            out.printf("[tick %4d] => %s still believes it leads term %d, but index %d exists only in its own log"
                            + " (1 of %d nodes), so it is not committed and the client has no answer%n",
                    sim.now(), leaderId, leader.currentTerm(), stranded.accepted().orElseThrow().index(), voters);

            out.println();
            out.println("== Phase 5: crash the cut-off leader; the others elect a new one ==");
            long oldTerm = leader.currentTerm();
            sim.crash(leaderId);
            sim.heal();
            RaftNode replacement = awaitLeader(sim, oldTerm);
            out.printf("[tick %4d] => %s leads term %d and appended its own NO_OP%n",
                    sim.now(), replacement.id(), replacement.currentTerm());
            proposeAndWait(sim, replacement.id().value(), put("user:3", "carol"));
            printTable(sim, ids, out);

            out.println();
            out.println("== Phase 6: committed data survived the crash (diagnostic local reads) ==");
            for (String id : ids) {
                if (sim.isUp(id)) {
                    out.printf("  %-3s lastApplied %d  %s%n", id, sim.liveNode(id).lastApplied(),
                            format(sim.stateMachine(id).diagnosticContents()));
                }
            }
            out.println("  A follower can lag behind the leader: it applies an entry only after a later message"
                    + " tells it the leader's commitIndex covers that entry.");

            out.println();
            out.println("== Phase 7: restart the old leader from its files; its uncommitted entry is replaced ==");
            sim.restart(leaderId);
            long target = replacement.commitIndex();
            boolean caughtUp = sim.runUntil(s -> s.liveNode(leaderId).lastApplied() >= target, BUDGET);
            if (!caughtUp) {
                throw new IllegalStateException(leaderId + " did not catch up within " + BUDGET + " ticks");
            }
            sim.run(10); // let the last heartbeat round settle every commit index
            out.printf("[tick %4d] => %s caught up: applied through index %d%n", sim.now(), leaderId, target);

            out.println();
            out.println("== Final state ==");
            printTable(sim, ids, out);
            out.println("Committed log (identical on every node):");
            for (Map.Entry<Long, LogEntry> entry : sim.committedHistory().entrySet()) {
                out.printf("  %d  term %d  %s%n", entry.getKey(), entry.getValue().term(), entry.getValue().command());
            }
            out.println("Key-value state per node (diagnostic local reads):");
            for (String id : ids) {
                out.printf("  %-3s %s%n", id, format(sim.stateMachine(id).diagnosticContents()));
            }
            out.println("Client view:");
            for (Simulation.ClientRequest request : sim.clientRequests()) {
                String status = switch (request.result()) {
                    case ProposeResult.NotLeader notLeader -> "rejected (not leader)";
                    default -> request.outcome().map(Enum::name).orElse("PENDING");
                };
                out.printf("  %-24s via %s: %s%n", request.command(), request.node(), status);
            }
            boolean daveCommitted = sim.committedHistory().values().stream()
                    .anyMatch(e -> e.command().equals(stranded.command()));
            out.println("  " + stranded.command() + " was reported UNKNOWN, not failed; here it was never committed"
                    + (daveCommitted ? " -- UNEXPECTEDLY it was" : " and was replaced on " + leaderId));
            List<String> violations = ElectionSafety.violations(sim.leaderHistory());
            sim.assertConverged();
            long acknowledged = sim.clientRequests().stream().filter(Simulation.ClientRequest::acknowledged).count();
            out.println("Checks: " + (violations.isEmpty() ? "at most one leader per term" : "VIOLATIONS " + violations)
                    + "; logs match; committed entries agree; every node applied the committed log in order; "
                    + acknowledged + " acknowledged writes are in the committed history");
            return violations.isEmpty() ? 0 : 1;
        }
    }

    private static RaftNode awaitLeader(Simulation sim, long afterTerm) {
        if (!sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > afterTerm).isPresent(), BUDGET)) {
            throw new IllegalStateException("no leader elected within " + BUDGET + " ticks");
        }
        return sim.establishedLeader().orElseThrow();
    }

    private static void awaitCommit(Simulation sim, RaftNode leader, long index) {
        if (!sim.runUntil(s -> leader.lastApplied() >= index, BUDGET)) {
            throw new IllegalStateException("index " + index + " not committed within " + BUDGET + " ticks");
        }
    }

    private static void proposeAndWait(Simulation sim, String leaderId, Command command) {
        Simulation.ClientRequest request = sim.propose(leaderId, command);
        if (!sim.runUntil(s -> request.outcome().isPresent(), BUDGET) || !request.acknowledged()) {
            throw new IllegalStateException(command + " was not acknowledged: " + request.outcome());
        }
    }

    private static void printTable(Simulation sim, String[] ids, PrintStream out) {
        out.printf("  %-4s %-9s %-5s %-13s %-12s %s%n", "node", "role", "term", "lastLogIndex", "commitIndex", "lastApplied");
        for (String id : ids) {
            if (!sim.isUp(id)) {
                out.printf("  %-4s %s%n", id, "DOWN (files closed on disk)");
                continue;
            }
            RaftNode node = sim.liveNode(id);
            RaftStorage storage = sim.storage(id);
            out.printf("  %-4s %-9s %-5d %-13d %-12d %d%n",
                    id, node.role(), node.currentTerm(), storage.lastIndex(), node.commitIndex(), node.lastApplied());
        }
    }

    private static String format(SortedMap<Bytes, Bytes> contents) {
        return contents.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static Command put(String key, String value) {
        return Command.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    private static Command delete(String key) {
        return Command.delete(key.getBytes(StandardCharsets.UTF_8));
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + args[index - 1]);
        }
        return args[index];
    }

    private static void deleteRecursively(Path root) {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot delete " + root, e);
        }
    }
}
