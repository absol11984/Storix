package com.storix.metadata.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RaftState and LogEntry.
 */
class RaftStateTest {

    @Test
    void testRaftStateValues() {
        assertEquals(3, RaftState.values().length);
        assertEquals(RaftState.FOLLOWER, RaftState.valueOf("FOLLOWER"));
        assertEquals(RaftState.CANDIDATE, RaftState.valueOf("CANDIDATE"));
        assertEquals(RaftState.LEADER, RaftState.valueOf("LEADER"));
    }

    @Test
    void testLogEntryOpTypes() {
        assertEquals(7, LogEntry.OpType.values().length);
        assertEquals(LogEntry.OpType.NO_OP, LogEntry.OpType.fromCode((byte) 0));
        assertEquals(LogEntry.OpType.CREATE_OBJECT, LogEntry.OpType.fromCode((byte) 1));
        assertEquals(LogEntry.OpType.UPDATE_OBJECT, LogEntry.OpType.fromCode((byte) 2));
        assertEquals(LogEntry.OpType.DELETE_OBJECT, LogEntry.OpType.fromCode((byte) 3));
        assertEquals(LogEntry.OpType.SNAPSHOT_RESTORE, LogEntry.OpType.fromCode((byte) 6));
    }

    @Test
    void testLogEntryCreate() {
        byte[] data = "test data".getBytes();
        LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, data);

        assertEquals(1, entry.term());
        assertEquals(0, entry.index()); // Index is 0 when created
        assertEquals(LogEntry.OpType.CREATE_OBJECT, entry.opType());
        assertArrayEquals(data, entry.data());
    }

    @Test
    void testLogEntryWithIndex() {
        byte[] data = "test data".getBytes();
        LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, data);
        LogEntry indexedEntry = entry.withIndex(42);

        assertEquals(1, indexedEntry.term());
        assertEquals(42, indexedEntry.index());
        assertEquals(entry.timestamp(), indexedEntry.timestamp());
        assertEquals(entry.opType(), indexedEntry.opType());
        assertArrayEquals(data, indexedEntry.data());
    }

    @Test
    void testRaftPeer() {
        RaftPeer peer = new RaftPeer("node1", "192.168.1.1", 9090);

        assertEquals("node1", peer.nodeId());
        assertEquals("192.168.1.1", peer.host());
        assertEquals(9090, peer.port());
        assertEquals("node1@192.168.1.1:9090", peer.toString());
    }

    @Test
    void testClusterConfigSingleNode() {
        ClusterConfig config = new ClusterConfig(
                "test-cluster", "node1", "127.0.0.1", 9090, null);

        assertEquals("test-cluster", config.clusterId());
        assertEquals("node1", config.nodeId());
        assertTrue(config.isSingleNode());
        assertEquals(1, config.getAllPeers().size());
    }

    @Test
    void testClusterConfigMultiNode() {
        ClusterConfig config = new ClusterConfig(
                "test-cluster", "node1", "127.0.0.1", 9090,
                java.util.List.of(
                        new RaftPeer("node2", "127.0.0.1", 9091),
                        new RaftPeer("node3", "127.0.0.1", 9092)
                ));

        assertFalse(config.isSingleNode());
        assertEquals(3, config.getAllPeers().size());
        assertEquals(2, config.getOtherPeers().size());
    }
}
