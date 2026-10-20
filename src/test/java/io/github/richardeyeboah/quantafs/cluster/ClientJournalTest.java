package io.github.richardeyeboah.quantafs.cluster;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
class ClientJournalTest {
    @TempDir Path root;
    @Test void unresolvedRequestSurvivesRestartAndPreventsNewMutation() throws Exception {
        Path path=root.resolve("checkout.session");String id;
        var mutation=new ClientJournal.Mutation("put","order","paid");
        try(var journal=new ClientJournal(path)) {
            id=journal.clientId();journal.prepare(mutation);
            assertThrows(Exception.class,() -> new ClientJournal(path));
        }
        try(var journal=new ClientJournal(path)) {
            assertEquals(id,journal.clientId());assertEquals(1,journal.sequence());assertEquals(mutation,journal.pending());
            assertThrows(java.io.IOException.class,() -> journal.prepare(new ClientJournal.Mutation("delete","order","")));
            journal.prepare(mutation);journal.confirmed();
        }
        try(var journal=new ClientJournal(path)) {assertEquals(2,journal.sequence());assertNull(journal.pending());}
    }
    @Test void corruptJournalIsNotReplacedWithANewIdentity() throws Exception {
        Path path=root.resolve("checkout.session");
        try(var journal=new ClientJournal(path)) {journal.prepare(new ClientJournal.Mutation("put","key","v"));}
        byte[] bytes=Files.readAllBytes(path);bytes[10]^=1;Files.write(path,bytes);
        assertThrows(java.io.IOException.class,() -> new ClientJournal(path));
    }
}
