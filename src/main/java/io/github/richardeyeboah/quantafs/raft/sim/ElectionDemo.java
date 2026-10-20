package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.AppendEntriesRequest;
import io.github.richardeyeboah.quantafs.raft.ElectionTiming;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Prints a reproducible election, leader crash, and re-election from the deterministic simulator.
 * The same seed always prints the same output.
 */
public final class ElectionDemo {

    public static final String USAGE =
            """
            Usage: election-demo [--seed <n>] [--voters <3|5>]

            Runs a deterministic in-process simulation of the Raft leader-election core
            (milestone 2A): elect a leader, crash it, elect a new one, restart the old one.
            No client data is stored or replicated. Same seed => same output.

              --seed     simulation seed (default 7)
              --voters   cluster size, 3 or 5 (default 5)
            """;

    private static final int STABLE_TICKS = 30;
    private static final int MAX_TICKS_PER_PHASE = 1_000;

    private ElectionDemo() {}

    public static int run(String[] args, PrintStream out, PrintStream err) {
        long seed = 7;
        int voters = 5;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--seed" -> seed = Long.parseLong(value(args, ++i));
                    case "--voters" -> voters = Integer.parseInt(value(args, ++i));
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

        String[] ids = IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toArray(String[]::new);
        ElectionTiming timing = ElectionTiming.DEFAULT;
        NetworkConfig network = NetworkConfig.RELIABLE;
        Simulation sim =
                Simulation.builder(ids).seed(seed).timing(timing).network(network).listener(out::println).build();

        out.println("QuantaFs Raft leader-election simulation (milestone 2A)");
        out.printf(
                "seed %d | voters %s | quorum %d | election timeout %d-%d ticks | heartbeat every %d ticks"
                        + " | network delay %d-%d ticks%n",
                seed, List.of(ids), sim.cluster().quorum(), timing.electionTimeoutMinTicks(),
                timing.electionTimeoutMaxTicks() - 1, timing.heartbeatIntervalTicks(),
                network.minDelayTicks(), network.maxDelayTicks());
        out.println("Simulation of the election core only: no client data, no log replication.");

        out.println();
        out.println("== Phase 1: initial election ==");
        RaftNode first = awaitLeader(sim, 0, out);
        if (first == null) {
            return 1;
        }
        String firstLeader = first.id().value();
        long firstTerm = first.currentTerm();

        out.println();
        out.printf("== Phase 2: %d ticks of steady heartbeats ==%n", STABLE_TICKS);
        long heartbeatsFrom = sim.now() + 1;
        sim.run(STABLE_TICKS);
        out.printf(
                "[tick %4d] %s is still leader of term %d; sent %d heartbeats (AppendEntries with no entries)"
                        + " to %d followers; no new elections%n",
                sim.now(), firstLeader, sim.liveNode(firstLeader).currentTerm(),
                sim.messagesSent(firstLeader, AppendEntriesRequest.class, heartbeatsFrom), voters - 1);

        out.println();
        out.println("== Phase 3: crash the leader ==");
        sim.crash(firstLeader);
        RaftNode second = awaitLeader(sim, firstTerm, out);
        if (second == null) {
            return 1;
        }

        out.println();
        out.println("== Phase 4: restart the old leader ==");
        sim.restart(firstLeader);
        boolean rejoined =
                sim.runUntil(
                        s -> s.liveNode(firstLeader).leaderId().equals(Optional.of(second.id()))
                                && s.liveNode(firstLeader).currentTerm() == second.currentTerm(),
                        MAX_TICKS_PER_PHASE);
        if (!rejoined) {
            out.println("old leader did not rejoin within " + MAX_TICKS_PER_PHASE + " ticks");
            return 1;
        }

        out.println();
        out.println("== Final state ==");
        out.printf("%-5s %-9s %-5s %-9s %s%n", "node", "role", "term", "votedFor", "knownLeader");
        for (String id : ids) {
            RaftNode node = sim.liveNode(id);
            out.printf(
                    "%-5s %-9s %-5d %-9s %s%n",
                    id, node.role(), node.currentTerm(),
                    node.votedFor().map(NodeId::value).orElse("-"),
                    node.leaderId().map(NodeId::value).orElse("-"));
        }
        Map<Long, NodeId> leaders = ElectionSafety.leadersByTerm(sim.leaderHistory());
        out.println("Leaders by term: " + leaders);
        List<String> violations = ElectionSafety.violations(sim.leaderHistory());
        out.println(violations.isEmpty()
                ? "Election safety: OK (no term had more than one leader)"
                : "Election safety VIOLATED: " + violations);
        return violations.isEmpty() ? 0 : 1;
    }

    private static RaftNode awaitLeader(Simulation sim, long afterTerm, PrintStream out) {
        boolean elected =
                sim.runUntil(
                        s -> s.establishedLeader().filter(l -> l.currentTerm() > afterTerm).isPresent(),
                        MAX_TICKS_PER_PHASE);
        if (!elected) {
            out.println("no leader elected within " + MAX_TICKS_PER_PHASE + " ticks");
            return null;
        }
        RaftNode leader = sim.establishedLeader().orElseThrow();
        out.printf(
                "[tick %4d] => %s leads term %d, acknowledged by a quorum%n",
                sim.now(), leader.id(), leader.currentTerm());
        return leader;
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + args[index - 1]);
        }
        return args[index];
    }
}
