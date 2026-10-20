package io.github.richardeyeboah.quantafs.raft;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class SnapshotCatchupTest {
    @Test void offlineFollowerInstallsImageAfterLeaderDiscardsItsMissingPrefix() {
        var ids=List.of(NodeId.of("n1"),NodeId.of("n2"),NodeId.of("n3"));
        var stores=new HashMap<NodeId,InMemoryRaftStorage>();var apps=new HashMap<NodeId,InMemoryStateMachine>();
        var nodes=new HashMap<NodeId,RaftNode>();var queue=new ArrayDeque<RaftMessage>();
        for(int i=0;i<3;i++) {
            var id=ids.get(i);var store=new InMemoryRaftStorage();var app=new InMemoryStateMachine();final int delay=i*3;
            stores.put(id,store);apps.put(id,app);nodes.put(id,new RaftNode(id,ClusterConfig.of(ids),new ElectionTiming(1,5,20),
                    (a,b)->a+delay,store,app));
        }
        for(int tick=0;tick<8;tick++) {for(var node:nodes.values()) queue.addAll(node.tick());pump(nodes,queue,null);}
        var leader=nodes.get(ids.getFirst());assertEquals(Role.LEADER,leader.role());
        NodeId offline=ids.get(2);
        for(int i=1;i<=100;i++) {
            var accepted=assertInstanceOf(ProposeResult.Accepted.class,leader.propose(new Command.Request("c",i,
                    Command.put("key".getBytes(),("value"+i).getBytes()))));queue.addAll(accepted.messages());pump(nodes,queue,offline);
        }
        assertTrue(leader.compact());assertTrue(leader.snapshotIndex()>=100);
        assertEquals(0,leader.retainedEntries());
        for(int tick=0;tick<100;tick++) {for(var node:nodes.values()) queue.addAll(node.tick());pump(nodes,queue,null);}
        assertTrue(nodes.get(offline).snapshotIndex()>0,"lagging node must receive snapshot, not discarded entries");
        assertEquals(apps.get(ids.getFirst()).diagnosticContents(),apps.get(offline).diagnosticContents());
        assertTrue(nodes.values().stream().noneMatch(RaftNode::isHalted));
    }
    @Test void followerCompactedBeyondCurrentProbeCanAdvanceUsingVerifiedBoundaryHint() {
        var n1=NodeId.of("n1");var n2=NodeId.of("n2");
        var store=new InMemoryRaftStorage(new TermAndVote(1,Optional.empty()),InMemoryRaftStorage.noOpLog(1,1,1,1,1,1,1,1,1,1));
        var leader=new RaftNode(n1,ClusterConfig.of("n1","n2","n3"),new ElectionTiming(1,5,8),(a,b)->a,store,new InMemoryStateMachine());
        for(int i=0;i<5;i++) leader.tick();
        leader.receive(new RequestVoteResponse(n2,n1,2,true));
        leader.receive(new AppendEntriesResponse(n2,n1,2,2,false,10,0,0));
        assertEquals(1,leader.nextIndex(n2).orElseThrow());
        var retry=leader.receive(new AppendEntriesResponse(n2,n1,2,2,false,0,0,10,10,1));
        assertEquals(10,leader.matchIndex(n2).orElseThrow());assertEquals(11,leader.nextIndex(n2).orElseThrow());
        assertEquals(10,assertInstanceOf(AppendEntriesRequest.class,retry.getFirst()).prevLogIndex());
    }

    @Test void crashAfterSnapshotPersistenceBeforeApplicationIsRepairedAtRestart() {
        var source=new InMemoryStateMachine();source.apply(new LogEntry(1,1,Command.put("key".getBytes(),"value".getBytes())));
        Snapshot image=source.snapshot();
        var store=new InMemoryRaftStorage();var app=new InMemoryStateMachine();
        var follower=new RaftNode(NodeId.of("n2"),ClusterConfig.of("n1","n2","n3"),ElectionTiming.DEFAULT,new SeededRandomSource(1),store,app);
        // Persist the higher term first, then inject failure at the snapshot boundary.
        follower.receive(new RequestVoteRequest(NodeId.of("n1"),NodeId.of("n2"),1,LogPosition.EMPTY));
        store.setFailureMode(FailureMode.FAIL_AFTER);
        assertEquals(List.of(),follower.receive(new InstallSnapshotRequest(NodeId.of("n1"),NodeId.of("n2"),1,image)));
        assertTrue(follower.isHalted());assertEquals(0,app.lastApplied().index());
        store.setFailureMode(FailureMode.NONE);
        var restarted=new RaftNode(NodeId.of("n2"),ClusterConfig.of("n1","n2","n3"),ElectionTiming.DEFAULT,new SeededRandomSource(1),store,app);
        assertEquals(1,restarted.lastApplied());assertEquals(Bytes.utf8("value"),app.diagnosticContents().get(Bytes.utf8("key")));
    }

    @Test void oversizedImageLeavesLogIntactAndCanCompactAfterDatasetShrinks() {
        var store=new InMemoryRaftStorage();var app=new InMemoryStateMachine();
        var node=new RaftNode(NodeId.of("n1"),ClusterConfig.of("n1"),ElectionTiming.DEFAULT,new SeededRandomSource(1),store,app);
        while(node.role()!=Role.LEADER) node.tick();
        byte[] value=new byte[1024*1024];
        for(int i=0;i<9;i++) node.propose(Command.put(("key"+i).getBytes(),value));
        assertFalse(node.compact());assertEquals(0,node.snapshotIndex());assertEquals(10,node.retainedEntries());assertFalse(node.isHalted());
        for(int i=0;i<9;i++) node.propose(Command.delete(("key"+i).getBytes()));
        assertTrue(node.compact());assertEquals(0,node.retainedEntries());assertEquals(19,node.snapshotIndex());
    }

    private void pump(Map<NodeId,RaftNode> nodes,ArrayDeque<RaftMessage> queue,NodeId offline) {
        int budget=10000;
        while(!queue.isEmpty()) {
            assertTrue(budget-->0,"protocol failed to quiesce");var message=queue.remove();
            if(message.from().equals(offline)||message.to().equals(offline)) continue;
            queue.addAll(nodes.get(message.to()).receive(message));
        }
    }
}
