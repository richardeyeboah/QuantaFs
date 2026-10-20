package io.github.richardeyeboah.quantafs.cluster;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Crash-safe, single-owner session journal. Pending bytes are durable before any network call. */
public final class ClientJournal implements AutoCloseable {
    public record Mutation(String kind,String key,String value) {
        public Mutation {
            if(!Set.of("put","delete").contains(kind) || key==null || key.isEmpty() || value==null)
                throw new IllegalArgumentException("invalid mutation");
            if(key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>1024 || value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>1024*1024)
                throw new IllegalArgumentException("mutation exceeds key/value limits");
        }
    }
    private final Path file;
    private final FileChannel channel;
    private final FileLock lock;
    private String clientId;
    private long sequence;
    private Mutation pending;
    public ClientJournal(Path path) throws IOException {
        file=path.toAbsolutePath().normalize(); Files.createDirectories(file.getParent());
        channel=FileChannel.open(file.resolveSibling(file.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired;
        try {acquired=channel.tryLock();} catch(IOException|RuntimeException e) {channel.close();throw e;}
        if(acquired==null) {channel.close();throw new IOException("session is already in use");} lock=acquired;
        try {
            if(Files.exists(file)) load();
            else {clientId=UUID.randomUUID().toString();sequence=1;save();}
        } catch(IOException|RuntimeException e) {close();throw e;}
    }
    public String clientId() {return clientId;}
    public long sequence() {return sequence;}
    public Mutation pending() {return pending;}
    public void prepare(Mutation operation) throws IOException {
        if(pending!=null && !pending.equals(operation)) throw new IOException("unresolved mutation exists; use resume before another write");
        pending=operation;save();
    }
    public void confirmed() throws IOException {
        if(pending==null) throw new IllegalStateException("nothing pending");
        sequence=Math.addExact(sequence,1);pending=null;save();
    }
    private static byte[] digest(byte[] bytes) {
        try {return MessageDigest.getInstance("SHA-256").digest(bytes);} catch(NoSuchAlgorithmException e) {throw new IllegalStateException(e);}
    }
    private void save() throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
        out.writeInt(0x51464331);out.writeUTF(clientId);out.writeLong(sequence);out.writeBoolean(pending!=null);
        if(pending!=null) {out.writeUTF(pending.kind()); writeString(out,pending.key());writeString(out,pending.value());} out.flush();
        byte[] body=bytes.toByteArray();Path temp=file.resolveSibling(file.getFileName()+".tmp");
        try(var target=FileChannel.open(temp,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE)) {
            ByteBuffer buffer=ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).flip();
            while(buffer.hasRemaining()) target.write(buffer);target.force(true);
        }
        Files.move(temp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        try(var parent=FileChannel.open(file.getParent(),StandardOpenOption.READ)) {parent.force(true);}
    }
    private void load() throws IOException {
        long length=Files.size(file);if(length<32 || length>2*1024*1024) throw new IOException("invalid session journal size");
        byte[] all=Files.readAllBytes(file),body=Arrays.copyOf(all,all.length-32);
        if(!MessageDigest.isEqual(digest(body),Arrays.copyOfRange(all,body.length,all.length))) throw new IOException("session checksum mismatch");
        var in=new DataInputStream(new ByteArrayInputStream(body));
        if(in.readInt()!=0x51464331) throw new IOException("unknown session format");
        clientId=in.readUTF();sequence=in.readLong();
        if(!clientId.matches("[A-Za-z0-9_.-]{1,64}") || sequence<1) throw new IOException("invalid client identity");
        pending=in.readBoolean()?new Mutation(in.readUTF(),readString(in),readString(in)):null;
        if(in.available()!=0) throw new IOException("trailing session data");
    }
    private static void writeString(DataOutputStream out,String text) throws IOException {
        byte[] bytes=text.getBytes(java.nio.charset.StandardCharsets.UTF_8);out.writeInt(bytes.length);out.write(bytes);
    }
    private static String readString(DataInputStream in) throws IOException {
        int length=in.readInt();if(length<0 || length>in.available()) throw new IOException("invalid string length");
        return new String(in.readNBytes(length),java.nio.charset.StandardCharsets.UTF_8);
    }
    @Override public void close() throws IOException {try {lock.release();} finally {channel.close();}}
}
