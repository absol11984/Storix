package com.storix.metadata.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.MetadataStore;
import com.storix.metadata.ObjectMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MetadataStateMachineTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void duplicateIdentifiedRequestIsDeduplicated() throws Exception {
        MetadataStore store = new MetadataStore(tempDir.resolve("metadata.json"));
        MetadataStateMachine stateMachine = new MetadataStateMachine(store);
        byte[] data = OBJECT_MAPPER.writeValueAsBytes(
            new ObjectMetadata("identified", 10, 1));

        LogEntry first = new LogEntry(1, 1, 1, LogEntry.OpType.CREATE_OBJECT,
            data, "client-a", "request-1");
        LogEntry retry = new LogEntry(1, 2, 2, LogEntry.OpType.CREATE_OBJECT,
            data, "client-a", "request-1");

        stateMachine.apply(first);
        assertDoesNotThrow(() -> stateMachine.apply(retry));
        assertTrue(store.objectExists("identified"));
    }

    @Test
    void duplicateLegacyRequestSurfacesConflict() throws Exception {
        MetadataStore store = new MetadataStore(tempDir.resolve("metadata.json"));
        MetadataStateMachine stateMachine = new MetadataStateMachine(store);
        byte[] data = OBJECT_MAPPER.writeValueAsBytes(
            new ObjectMetadata("legacy", 10, 1));

        LogEntry first = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, data);
        LogEntry duplicate = new LogEntry(1, 2, 2, LogEntry.OpType.CREATE_OBJECT,
            data, null, null);

        stateMachine.apply(first);
        IllegalStateException failure = assertThrows(
            IllegalStateException.class, () -> stateMachine.apply(duplicate));
        assertTrue(failure.getMessage().contains("Object already exists"));
    }
}
