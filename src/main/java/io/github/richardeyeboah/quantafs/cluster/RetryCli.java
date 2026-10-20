package io.github.richardeyeboah.quantafs.cluster;
import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

public final class RetryCli {
    private RetryCli() {}
    public static int run(String[] args) {
        if(args.length<5 || !args[0].equals("--seeds") || !args[2].equals("--session")) {
            System.err.println("cluster-client --seeds 127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103 --session ./checkout.session <put key value|get key|delete key|resume>");return 2;
        }
        try {
            List<Integer> ports=Arrays.stream(args[1].split(",")).map(seed -> {
                if(!seed.matches("127\\.0\\.0\\.1:[0-9]+")) throw new IllegalArgumentException("loopback seeds required");
                int port=Integer.parseInt(seed.substring(10));if(port<1||port>65535) throw new IllegalArgumentException("invalid port");return port;
            }).toList();
            try(var journal=new ClientJournal(Path.of(args[3]));var client=new ClusterClient(ports,Duration.ofSeconds(20))) {
                if(args[4].equals("get") && args.length==6) {
                    var result=client.get(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(args[5])).build());
                    System.out.println(result.getFound()?result.getValue().toStringUtf8():"NOT FOUND");return result.getFound()?0:1;
                }
                ClientJournal.Mutation operation;
                if(args[4].equals("resume") && args.length==5) {
                    operation=journal.pending();if(operation==null) throw new IllegalArgumentException("no unresolved mutation");
                } else if(args[4].equals("put") && args.length==7) operation=new ClientJournal.Mutation("put",args[5],args[6]);
                else if(args[4].equals("delete") && args.length==6) operation=new ClientJournal.Mutation("delete",args[5],"");
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
}
