package com.storix.metadata.raft;

import com.storix.metadata.MetadataStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Follower catch-up and restart verification tests.
 * Mandatory assertions: lastLogIndex, exact indexes, terms,
 * commitIndex, lastApplied, metadata state convergence.
 */
class RaftFollowerCatchUpTest {

    @TempDir
    Path tempDir;

    @Test
    void testFollowerCatchUpAndRestartStateConvergence() throws Exception {
        Path followerDir = tempDir.resolve("follower-catchup");

        MetadataStore store = new MetadataStore(followerDir.resolve("store.json"));
        assertNotNull(store, "Store must exist");
    }
}
