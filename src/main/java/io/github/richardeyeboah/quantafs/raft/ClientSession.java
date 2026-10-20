package io.github.richardeyeboah.quantafs.raft;

import java.io.*;
import java.security.*;

/** One durable latest result per client. Clients must serialize operations, starting at sequence 1. */
public record ClientSession(long sequence, Bytes fingerprint) {
    public ClientSession {
        if (sequence < 1 || fingerprint.size()!=32) throw new IllegalArgumentException("invalid session record");
    }
    public enum Result { SUCCESS, STALE, GAP, CONFLICT }
    public static Bytes fingerprint(Command.Request request) {
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            switch(request.mutation()) {
                case Command.Put p -> {out.writeByte(1); write(out,p.key()); write(out,p.value());}
                case Command.Delete d -> {out.writeByte(2); write(out,d.key());}
                default -> throw new IllegalArgumentException("not a mutation");
            }
            out.flush(); return Bytes.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch(IOException | NoSuchAlgorithmException e) {throw new IllegalStateException(e);}
    }
    private static void write(DataOutputStream out,Bytes value) throws IOException {
        out.writeInt(value.size()); out.write(value.toByteArray());
    }
    public static boolean isNext(ClientSession previous,Command.Request request) {
        return request.sequence()==(previous==null?1:previous.sequence()+1);
    }
    public static Result result(ClientSession stored,Command.Request request) {
        if(stored==null || request.sequence()>stored.sequence()) return Result.GAP;
        if(request.sequence()<stored.sequence()) return Result.STALE;
        return stored.fingerprint().equals(fingerprint(request))?Result.SUCCESS:Result.CONFLICT;
    }
    public byte[] encode() {
        return java.nio.ByteBuffer.allocate(40).putLong(sequence).put(fingerprint.toByteArray()).array();
    }
    public static ClientSession decode(byte[] data) {
        if(data.length!=40) throw new StateMachineException("corrupt session record");
        var b=java.nio.ByteBuffer.wrap(data); long seq=b.getLong(); byte[] digest=new byte[32]; b.get(digest);
        return new ClientSession(seq,Bytes.copyOf(digest));
    }
}
