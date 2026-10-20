package io.github.richardeyeboah.quantafs.cluster;

import io.github.richardeyeboah.quantafs.raft.NodeId;
import java.nio.file.Path;
import java.util.*;

/** Fixed local-process deployment. No unauthenticated remote binding is supported. */
public record ClusterSettings(NodeId id, SortedMap<NodeId, Integer> ports, Path directory) {
    public ClusterSettings {
        Objects.requireNonNull(id); Objects.requireNonNull(directory);
        ports = Collections.unmodifiableSortedMap(new TreeMap<>(ports));
        if (!ports.containsKey(id) || ports.size() < 3 || ports.size() > 5 || ports.size() % 2 == 0)
            throw new IllegalArgumentException("configure exactly 3 or 5 voters including --id");
        if (new HashSet<>(ports.values()).size() != ports.size())
            throw new IllegalArgumentException("peer ports must be unique");
        if (ports.values().stream().anyMatch(p -> p < 1 || p > 65535))
            throw new IllegalArgumentException("ports must be 1..65535");
        directory = directory.toAbsolutePath().normalize();
    }
    public String clusterKey() {
        return ports.entrySet().stream().map(e -> e.getKey().value() + "=" + e.getValue())
                .collect(java.util.stream.Collectors.joining(","));
    }
    public static ClusterSettings parse(String[] args) {
        Map<String,String> flags = new HashMap<>();
        for (int i=0; i<args.length; i+=2) {
            if (i+1==args.length || !Set.of("--id","--peers","--data-dir").contains(args[i])
                    || flags.putIfAbsent(args[i],args[i+1]) != null)
                throw new IllegalArgumentException(USAGE);
        }
        if (flags.size()!=3) throw new IllegalArgumentException(USAGE);
        SortedMap<NodeId,Integer> ports = new TreeMap<>();
        for (String peer : flags.get("--peers").split(",",-1)) {
            String[] pair=peer.split("=",-1);
            if (pair.length!=2 || !pair[1].startsWith("127.0.0.1:"))
                throw new IllegalArgumentException("peers must use id=127.0.0.1:port");
            if (ports.putIfAbsent(NodeId.of(pair[0]),Integer.parseInt(pair[1].substring(10)))!=null)
                throw new IllegalArgumentException("duplicate peer id");
        }
        return new ClusterSettings(NodeId.of(flags.get("--id")),ports,Path.of(flags.get("--data-dir")));
    }
    public static final String USAGE = "cluster-server --id n1 --peers n1=127.0.0.1:5101,n2=127.0.0.1:5102,n3=127.0.0.1:5103 --data-dir ./n1-data";
}
