package io.github.richardeyeboah.quantafs.cluster;

import io.github.richardeyeboah.quantafs.raft.NodeId;
import java.nio.file.Path;
import java.util.*;

/**
 * Fixed local-demo deployment. Loopback is the default; Compose networking requires an explicit
 * opt-in and is still plaintext and unauthenticated.
 */
public record ClusterSettings(
        NodeId id,
        SortedMap<NodeId, NetworkEndpoint> peers,
        Path directory,
        NetworkMode networkMode) {

    public enum NetworkMode {
        LOOPBACK,
        COMPOSE
    }

    public ClusterSettings {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(peers, "peers");
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(networkMode, "networkMode");
        peers = Collections.unmodifiableSortedMap(new TreeMap<>(peers));
        if (!peers.containsKey(id) || peers.size() < 3 || peers.size() > 5 || peers.size() % 2 == 0) {
            throw new IllegalArgumentException("configure exactly 3 or 5 voters including --id");
        }
        if (new HashSet<>(peers.values()).size() != peers.size()) {
            throw new IllegalArgumentException("peer endpoints must be unique");
        }
        for (var peer : peers.entrySet()) {
            if (networkMode == NetworkMode.LOOPBACK
                    && !peer.getValue().host().equals("127.0.0.1")) {
                throw new IllegalArgumentException(
                        "loopback mode requires every peer to use 127.0.0.1");
            }
            if (networkMode == NetworkMode.COMPOSE
                    && (!peer.getValue().host().equals(peer.getKey().value())
                            || !peer.getValue().host().matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"))) {
                throw new IllegalArgumentException(
                        "compose mode requires each lowercase service DNS name to equal its node id");
            }
        }
        directory = directory.toAbsolutePath().normalize();
    }

    /** Compatibility constructor for the existing loopback process runtime and tests. */
    public ClusterSettings(NodeId id, SortedMap<NodeId, Integer> ports, Path directory) {
        this(id, loopbackPeers(ports), directory, NetworkMode.LOOPBACK);
    }

    private static SortedMap<NodeId, NetworkEndpoint> loopbackPeers(
            SortedMap<NodeId, Integer> ports) {
        SortedMap<NodeId, NetworkEndpoint> peers = new TreeMap<>();
        ports.forEach((id, port) -> peers.put(id, NetworkEndpoint.loopback(port)));
        return peers;
    }

    /** Existing callers use this projection; loopback tests retain their original API. */
    public SortedMap<NodeId, Integer> ports() {
        SortedMap<NodeId, Integer> ports = new TreeMap<>();
        peers.forEach((node, endpoint) -> ports.put(node, endpoint.port()));
        return Collections.unmodifiableSortedMap(ports);
    }

    public NetworkEndpoint localEndpoint() {
        return peers.get(id);
    }

    public String bindHost() {
        return networkMode == NetworkMode.LOOPBACK ? "127.0.0.1" : "0.0.0.0";
    }

    public String clusterKey() {
        return peers.entrySet().stream()
                .map(
                        e ->
                                e.getKey().value()
                                        + "="
                                        + (networkMode == NetworkMode.LOOPBACK
                                                ? e.getValue().port()
                                                : e.getValue().authority()))
                .collect(java.util.stream.Collectors.joining(","));
    }

    public static ClusterSettings parse(String[] args) {
        Map<String, String> flags = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 == args.length
                    || !Set.of("--id", "--peers", "--data-dir", "--network-mode")
                            .contains(args[i])
                    || flags.putIfAbsent(args[i], args[i + 1]) != null) {
                throw new IllegalArgumentException(USAGE);
            }
        }
        if (!flags.keySet().containsAll(Set.of("--id", "--peers", "--data-dir"))
                || flags.size() > 4) {
            throw new IllegalArgumentException(USAGE);
        }
        NetworkMode mode =
                switch (flags.getOrDefault("--network-mode", "loopback")) {
                    case "loopback" -> NetworkMode.LOOPBACK;
                    case "compose" -> NetworkMode.COMPOSE;
                    default -> throw new IllegalArgumentException(
                            "--network-mode must be loopback or compose");
                };
        SortedMap<NodeId, NetworkEndpoint> peers = new TreeMap<>();
        for (String peer : flags.get("--peers").split(",", -1)) {
            String[] pair = peer.split("=", -1);
            if (pair.length != 2
                    || peers.putIfAbsent(NodeId.of(pair[0]), NetworkEndpoint.parse(pair[1]))
                            != null) {
                throw new IllegalArgumentException("duplicate peer id");
            }
        }
        return new ClusterSettings(
                NodeId.of(flags.get("--id")),
                peers,
                Path.of(flags.get("--data-dir")),
                mode);
    }

    public static final String USAGE =
            """
            cluster-server --id n1 \
              --peers n1=127.0.0.1:5101,n2=127.0.0.1:5102,n3=127.0.0.1:5103 \
              --data-dir ./n1-data [--network-mode loopback|compose]

            loopback is the default. compose is an explicit plaintext local-demo mode whose
            lowercase service DNS names must exactly equal the node IDs.
            """;
}
