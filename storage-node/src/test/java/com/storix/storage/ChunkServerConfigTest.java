package com.storix.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ChunkServer configuration.
 */
class ChunkServerConfigTest {

    @TempDir
    Path tempDir;

    @Test
    void testHeartbeatIntervalConfigured() throws Exception {
        // Create ChunkServer with custom heartbeat interval
        long expectedInterval = 1000L;
        ChunkServer server = new ChunkServer(
            "test-node",
            "127.0.0.1",
            9999,
            tempDir.resolve("chunks1"),
            "127.0.0.1",
            9998,
            expectedInterval
        );

        assertEquals(expectedInterval, server.getHeartbeatIntervalMillis(),
                "ChunkServer should store the configured heartbeat interval");
    }

    @Test
    void testDefaultHeartbeatInterval() throws Exception {
        // Create ChunkServer with default interval (no explicit interval parameter)
        ChunkServer server = new ChunkServer(
            "test-node",
            "127.0.0.1",
            9999,
            tempDir.resolve("chunks2"),
            "127.0.0.1",
            9998
        );

        assertEquals(ChunkServer.DEFAULT_HEARTBEAT_INTERVAL_MILLIS, server.getHeartbeatIntervalMillis(),
                "ChunkServer should use default heartbeat interval when not specified");
    }

    @Test
    void testNegativeIntervalDefaults() throws Exception {
        // Negative interval should fall back to default
        ChunkServer server = new ChunkServer(
            "test-node",
            "127.0.0.1",
            9999,
            tempDir.resolve("chunks3"),
            "127.0.0.1",
            9998,
            -100L // Invalid negative value
        );

        assertEquals(ChunkServer.DEFAULT_HEARTBEAT_INTERVAL_MILLIS, server.getHeartbeatIntervalMillis(),
                "Negative heartbeat interval should fall back to default");
    }

    @Test
    void testZeroIntervalDefaults() throws Exception {
        // Zero interval should fall back to default
        ChunkServer server = new ChunkServer(
            "test-node",
            "127.0.0.1",
            9999,
            tempDir.resolve("chunks4"),
            "127.0.0.1",
            9998,
            0L
        );

        assertEquals(ChunkServer.DEFAULT_HEARTBEAT_INTERVAL_MILLIS, server.getHeartbeatIntervalMillis(),
                "Zero heartbeat interval should fall back to default");
    }

    @Test
    void testSmallHeartbeatInterval() throws Exception {
        // Test with a very small interval (useful for testing)
        long smallInterval = 100L;
        ChunkServer server = new ChunkServer(
            "test-node",
            "127.0.0.1",
            9999,
            tempDir.resolve("chunks5"),
            "127.0.0.1",
            9998,
            smallInterval
        );

        assertEquals(smallInterval, server.getHeartbeatIntervalMillis());
    }
}
