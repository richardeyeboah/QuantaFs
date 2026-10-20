package io.github.richardeyeboah.quantafs.cluster;
import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

public final class RetryCli {
    private static final String USAGE =
            """
            cluster-client [--network-mode loopback|compose|kubernetes] \
              --seeds 127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103 \
              --session ./checkout.session <put key value|get key|delete key|resume>
            """;

    private RetryCli() {}
    public static int run(String[] args) {
        try {
            int offset=0;
            ClusterSettings.NetworkMode mode=ClusterSettings.NetworkMode.LOOPBACK;
            if(args.length>=2 && args[0].equals("--network-mode")) {
                mode=switch(args[1]) {
                    case "loopback" -> ClusterSettings.NetworkMode.LOOPBACK;
                    case "compose" -> ClusterSettings.NetworkMode.COMPOSE;
                    case "kubernetes" -> ClusterSettings.NetworkMode.KUBERNETES;
                    default -> throw new IllegalArgumentException(
                            "--network-mode must be loopback, compose, or kubernetes");
                };
                offset=2;
            }
            if(args.length<offset+5 || !args[offset].equals("--seeds") || !args[offset+2].equals("--session")) {
                System.err.print(USAGE);return 2;
            }
            List<NetworkEndpoint> seeds=parseSeeds(args[offset+1],mode);
            int command=offset+4;
            try(var journal=new ClientJournal(Path.of(args[offset+3]));var client=new ClusterClient(seeds,Duration.ofSeconds(20))) {
                if(args[command].equals("get") && args.length==command+2) {
                    var result=client.get(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(args[command+1])).build());
                    System.out.println(result.getFound()?result.getValue().toStringUtf8():"NOT FOUND");return result.getFound()?0:1;
                }
                ClientJournal.Mutation operation;
                if(args[command].equals("resume") && args.length==command+1) {
                    operation=journal.pending();if(operation==null) throw new IllegalArgumentException("no unresolved mutation");
                } else if(args[command].equals("put") && args.length==command+3)
                    operation=new ClientJournal.Mutation("put",args[command+1],args[command+2]);
                else if(args[command].equals("delete") && args.length==command+2)
                    operation=new ClientJournal.Mutation("delete",args[command+1],"");
                else throw new IllegalArgumentException("invalid command/arguments");
                journal.prepare(operation);
                if(operation.kind().equals("put")) client.put(PutRequest.newBuilder().setClientId(journal.clientId())
                        .setSequence(journal.sequence()).setKey(ByteString.copyFromUtf8(operation.key()))
                        .setValue(ByteString.copyFromUtf8(operation.value())).build());
                else client.delete(DeleteRequest.newBuilder().setClientId(journal.clientId()).setSequence(journal.sequence())
                        .setKey(ByteString.copyFromUtf8(operation.key())).build());
                journal.confirmed();System.out.println("OK");return 0;
            }
        } catch(Exception e) {System.err.println("error: "+e.getMessage());return 3;}
    }

    static List<NetworkEndpoint> parseSeeds(
            String text, ClusterSettings.NetworkMode mode) {
        List<NetworkEndpoint> seeds=Arrays.stream(text.split(",",-1))
                .map(NetworkEndpoint::parse).toList();
        if(seeds.isEmpty() || new HashSet<>(seeds).size()!=seeds.size())
            throw new IllegalArgumentException("seed endpoints must be non-empty and unique");
        for(NetworkEndpoint seed:seeds) {
            if(mode==ClusterSettings.NetworkMode.LOOPBACK && !seed.host().equals("127.0.0.1"))
                throw new IllegalArgumentException(
                        "loopback seeds required unless a container network mode is explicit");
            if(mode==ClusterSettings.NetworkMode.COMPOSE
                    && !seed.host().matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"))
                throw new IllegalArgumentException("compose seeds must use lowercase service DNS names");
            if(mode==ClusterSettings.NetworkMode.KUBERNETES
                    && !seed.host().matches(
                            "[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.quantafs-peer"))
                throw new IllegalArgumentException(
                        "kubernetes seeds must use <lowercase-node-id>.quantafs-peer");
        }
        return seeds;
    }
}
