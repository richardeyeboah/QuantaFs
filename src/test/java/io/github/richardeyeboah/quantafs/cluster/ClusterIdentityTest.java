package io.github.richardeyeboah.quantafs.cluster;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
class ClusterIdentityTest {
    @TempDir Path root;
    private ClusterSettings settings(String id) throws Exception {
        var ports=new TreeMap<NodeId,Integer>();
        List<ServerSocket> sockets=new ArrayList<>();
        try {
            for(int i=1;i<=3;i++) {
                var socket=new ServerSocket(0); sockets.add(socket);
                ports.put(NodeId.of("n"+i),socket.getLocalPort());
            }
        } finally { for(var s:sockets) s.close(); }
        return new ClusterSettings(NodeId.of(id),ports,root);
    }
    @Test void directoryIsLockedAndCannotBeReassignedToAnotherIdentity() throws Exception {
        var s=settings("n1");
        try(var server=ClusterServer.start(s)) {
            assertThrows(Exception.class,() -> ClusterServer.start(s));
        }
        var changed=new ClusterSettings(NodeId.of("n2"),s.ports(),root);
        assertThrows(IOException.class,() -> ClusterServer.start(changed));
        try(var reopened=ClusterServer.start(s)) { assertNotNull(reopened); }
    }
    @Test void existingUnidentifiedDataIsNeverSilentlyReused() throws Exception {
        Files.createDirectories(root.resolve("raft-log"));
        assertThrows(IOException.class,() -> ClusterServer.start(settings("n1")));
    }
}
