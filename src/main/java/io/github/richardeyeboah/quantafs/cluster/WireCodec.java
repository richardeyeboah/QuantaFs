package io.github.richardeyeboah.quantafs.cluster;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import io.github.richardeyeboah.quantafs.raft.*;

/** Validated protobuf boundary; the deterministic core has no generated-code dependencies. */
public final class WireCodec {
    private WireCodec() {}
    public static RaftEnvelope encode(String cluster, RaftMessage m) {
        var b=RaftEnvelope.newBuilder().setClusterKey(cluster).setSender(m.from().value())
                .setReceiver(m.to().value()).setTerm(m.term());
        switch(m) {
            case RequestVoteRequest r -> b.setVoteCall(VoteCall.newBuilder()
                    .setLastTerm(r.lastLog().term()).setLastIndex(r.lastLog().index()));
            case RequestVoteResponse r -> b.setVoteReply(VoteReply.newBuilder().setGranted(r.voteGranted()));
            case AppendEntriesRequest r -> {
                var a=AppendCall.newBuilder().setPrevIndex(r.prevLogIndex()).setPrevTerm(r.prevLogTerm())
                        .setCommitIndex(r.leaderCommit());
                r.entries().forEach(e -> a.addEntries(encodeEntry(e)));
                b.setAppendCall(a);
            }
            case AppendEntriesResponse r -> b.setAppendReply(AppendReply.newBuilder()
                    .setRequestTerm(r.requestTerm()).setSuccess(r.success()).setRequestPrevIndex(r.requestPrevLogIndex())
                    .setMatchIndex(r.matchIndex()).setLastIndex(r.followerLastIndex()));
        }
        return b.build();
    }
    public static RaftMessage decode(RaftEnvelope w) {
        NodeId from=NodeId.of(w.getSender()), to=NodeId.of(w.getReceiver());
        long t=w.getTerm();
        return switch(w.getPayloadCase()) {
            case VOTE_CALL -> new RequestVoteRequest(from,to,t,
                    new LogPosition(w.getVoteCall().getLastTerm(),w.getVoteCall().getLastIndex()));
            case VOTE_REPLY -> new RequestVoteResponse(from,to,t,w.getVoteReply().getGranted());
            case APPEND_CALL -> {
                var a=w.getAppendCall();
                if (a.getEntriesCount()>RaftNode.MAX_ENTRIES_PER_APPEND)
                    throw new IllegalArgumentException("too many entries");
                yield new AppendEntriesRequest(from,to,t,a.getPrevIndex(),a.getPrevTerm(),
                        a.getEntriesList().stream().map(WireCodec::decodeEntry).toList(),a.getCommitIndex());
            }
            case APPEND_REPLY -> {
                var a=w.getAppendReply();
                yield new AppendEntriesResponse(from,to,t,a.getRequestTerm(),a.getSuccess(),
                        a.getRequestPrevIndex(),a.getMatchIndex(),a.getLastIndex());
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("missing payload");
        };
    }
    private static WireEntry encodeEntry(LogEntry e) {
        var b=WireEntry.newBuilder().setIndex(e.index()).setTerm(e.term());
        switch(e.command()) {
            case Command.Put p -> b.setKind(WireEntry.Kind.PUT).setKey(ByteString.copyFrom(p.key().toByteArray()))
                    .setValue(ByteString.copyFrom(p.value().toByteArray()));
            case Command.Delete d -> b.setKind(WireEntry.Kind.DELETE).setKey(ByteString.copyFrom(d.key().toByteArray()));
            case Command.NoOp ignored -> b.setKind(WireEntry.Kind.NO_OP);
        }
        return b.build();
    }
    private static LogEntry decodeEntry(WireEntry e) {
        Command c=switch(e.getKind()) {
            case PUT -> Command.put(e.getKey().toByteArray(),e.getValue().toByteArray());
            case DELETE -> {
                if (!e.getValue().isEmpty()) throw new IllegalArgumentException("delete has value");
                yield Command.delete(e.getKey().toByteArray());
            }
            case NO_OP -> {
                if (!e.getKey().isEmpty() || !e.getValue().isEmpty()) throw new IllegalArgumentException("no-op has data");
                yield Command.noOp();
            }
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown command");
        };
        return new LogEntry(e.getIndex(),e.getTerm(),c);
    }
}
