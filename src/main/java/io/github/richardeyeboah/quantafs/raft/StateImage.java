package io.github.richardeyeboah.quantafs.raft;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

/** Versioned, checksummed image including retry sessions, not just application values. */
public record StateImage(LogPosition position,SortedMap<Bytes,Bytes> data,SortedMap<String,ClientSession> sessions) {
    public StateImage {
        data=Collections.unmodifiableSortedMap(new TreeMap<>(data));
        sessions=Collections.unmodifiableSortedMap(new TreeMap<>(sessions));
    }
    public Snapshot snapshot() {
        try {
            var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);
            out.writeInt(0x51465331);out.writeLong(position.term());out.writeLong(position.index());
            out.writeInt(data.size());
            for(var e:data.entrySet()) {write(out,e.getKey());write(out,e.getValue());}
            out.writeInt(sessions.size());
            for(var e:sessions.entrySet()) {out.writeUTF(e.getKey());out.write(e.getValue().encode());}
            out.flush();var crc=new CRC32();crc.update(bytes.toByteArray());out.writeLong(crc.getValue());out.flush();
            return new Snapshot(position,Bytes.copyOf(bytes.toByteArray()));
        } catch(IOException e) {throw new StateMachineException("cannot encode snapshot",e);}
    }
    public static StateImage decode(Snapshot snapshot) {
        try {
            byte[] all=snapshot.image().toByteArray();if(all.length<32) throw new IOException("truncated image");
            int size=all.length-8;var crc=new CRC32();crc.update(all,0,size);
            if(crc.getValue()!=ByteBuffer.wrap(all,size,8).getLong()) throw new IOException("image checksum mismatch");
            var in=new DataInputStream(new ByteArrayInputStream(all,0,size));
            if(in.readInt()!=0x51465331) throw new IOException("unknown image version");
            var position=new LogPosition(in.readLong(),in.readLong());
            if(!position.equals(snapshot.position())) throw new IOException("snapshot boundary mismatch");
            var data=new TreeMap<Bytes,Bytes>();int count=count(in);
            for(int i=0;i<count;i++) {
                Bytes key=read(in),value=read(in);Command.put(key.toByteArray(),value.toByteArray());
                if(data.put(key,value)!=null) throw new IOException("duplicate key");
            }
            var sessions=new TreeMap<String,ClientSession>();count=count(in);
            for(int i=0;i<count;i++) {
                String id=in.readUTF();if(!id.matches("[A-Za-z0-9_.-]{1,64}")) throw new IOException("invalid session id");
                byte[] record=in.readNBytes(40);
                if(sessions.put(id,ClientSession.decode(record))!=null) throw new IOException("duplicate session");
            }
            if(in.available()!=0) throw new IOException("trailing image bytes");
            return new StateImage(position,data,sessions);
        } catch(IOException|IllegalArgumentException e) {throw new StateMachineException("invalid snapshot: "+e.getMessage(),e);}
    }
    private static int count(DataInputStream in) throws IOException {
        int n=in.readInt();if(n<0 || n>in.available()) throw new IOException("invalid count");return n;
    }
    private static void write(DataOutputStream out,Bytes value) throws IOException {out.writeInt(value.size());out.write(value.toByteArray());}
    private static Bytes read(DataInputStream in) throws IOException {
        int length=in.readInt();if(length<0||length>in.available()) throw new IOException("invalid length");
        return Bytes.copyOf(in.readNBytes(length));
    }
}
