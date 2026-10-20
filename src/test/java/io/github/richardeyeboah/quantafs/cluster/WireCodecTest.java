package io.github.richardeyeboah.quantafs.cluster;
import io.github.richardeyeboah.quantafs.api.v1.*;
import io.github.richardeyeboah.quantafs.raft.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class WireCodecTest {
    @Test void allMessageKindsRoundTripIncludingBinaryEntries() {
        NodeId a=NodeId.of("a"),b=NodeId.of("b");
        List<RaftMessage> messages=List.of(new RequestVoteRequest(a,b,3,new LogPosition(2,7)),
                new RequestVoteResponse(b,a,3,true),
                new AppendEntriesRequest(a,b,3,0,0,List.of(new LogEntry(1,3,Command.noOp()),
                        new LogEntry(2,3,Command.put(new byte[]{0,(byte)255},new byte[]{10,0})),
                        new LogEntry(3,3,Command.delete(new byte[]{0,(byte)255}))),2),
                new AppendEntriesResponse(b,a,3,3,true,0,3,3),
                new AppendEntriesResponse(b,a,4,3,false,4,0,1),
                new AppendEntriesResponse(b,a,3,3,false,0,0,10,7,2),
                new InstallSnapshotRequest(a,b,3,new StateImage(new LogPosition(2,7),new TreeMap<>(),new TreeMap<>()).snapshot()),
                new InstallSnapshotResponse(b,a,3,7,true));
        for(var m:messages) assertEquals(m,WireCodec.decode(WireCodec.encode("test",m)));
    }
    @Test void invalidWireDataIsRejected() {
        assertThrows(IllegalArgumentException.class,() -> WireCodec.decode(RaftEnvelope.getDefaultInstance()));
        var w=RaftEnvelope.newBuilder().setSender("a").setReceiver("b").setTerm(1)
                .setAppendCall(AppendCall.newBuilder().addEntries(WireEntry.newBuilder().setIndex(1).setTerm(1)
                        .setKindValue(100))).build();
        assertThrows(IllegalArgumentException.class,() -> WireCodec.decode(w));
    }
    @Test void fixedMembershipConfigurationIsValidatedAndCanonical() {
        var s=ClusterSettings.parse(new String[]{"--id","n1","--peers",
                "n3=127.0.0.1:5103,n1=127.0.0.1:5101,n2=127.0.0.1:5102","--data-dir","./n1-data"});
        assertEquals(5101,s.ports().get(NodeId.of("n1")));
        assertEquals("n1=5101,n2=5102,n3=5103",s.clusterKey());
        assertThrows(IllegalArgumentException.class,() -> ClusterSettings.parse(new String[]{"--id","n1","--peers",
                "n1=0.0.0.0:5101","--data-dir","./n1-data"}));
        assertThrows(IllegalArgumentException.class,() -> new ClusterSettings(NodeId.of("n1"),
                new TreeMap<>(Map.of(NodeId.of("n1"),1,NodeId.of("n2"),1,NodeId.of("n3"),3)),Path.of("x")));
    }
}
