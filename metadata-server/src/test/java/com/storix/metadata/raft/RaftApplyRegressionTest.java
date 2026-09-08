package com.storix.metadata.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.MetadataServer;
import com.storix.metadata.ObjectMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for absolute Raft index allocation across failover.
 *
 * A follower can receive index 1 before becoming leader. Its new submission
 * must therefore reserve index 2 rather than reusing index 1.
 */
class RaftApplyRegressionTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TestPortAllocator.Lease ports;
    private Path clusterDir;
    private MetadataServer serverA;
    private MetadataServer serverB;
    private MetadataServer serverC;

    @AfterEach
    void tearDown() {
        for (MetadataServer server : Arrays.asList(serverA, serverB, serverC)) {
            if (server != null) {
                server.stop();
            }
        }
        if (ports != null) {
            ports.close();
            ports = null;
        }
        if (clusterDir != null) {
            try {
                Files.walk(clusterDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            } catch (Exception ignored) {
                // Preserve the test result; temporary directories are best effort.
            }
        }
    }

    @Test
    void testDirectSubmitUsesNextAbsoluteIndexAfterLeaderFailover() throws Exception {
        ports = TestPortAllocator.lease(6);
        clusterDir = Files.createTempDirectory("raft-index-regression-");

        ClusterConfig configA = config("meta-a", ports.port(0), ports.port(3),
            new RaftPeer("meta-b", "127.0.0.1", ports.port(4)),
            new RaftPeer("meta-c", "127.0.0.1", ports.port(5)));
        ClusterConfig configB = config("meta-b", ports.port(1), ports.port(4),
            new RaftPeer("meta-a", "127.0.0.1", ports.port(3)),
            new RaftPeer("meta-c", "127.0.0.1", ports.port(5)));
        ClusterConfig configC = config("meta-c", ports.port(2), ports.port(5),
            new RaftPeer("meta-a", "127.0.0.1", ports.port(3)),
            new RaftPeer("meta-b", "127.0.0.1", ports.port(4)));
        ports.release();

        serverA = start("meta-a", ports.port(0), configA);
        serverB = start("meta-b", ports.port(1), configB);
        serverC = start("meta-c", ports.port(2), configC);
        waitForLeader(15_000);

        MetadataServer firstLeader = leader();
        assertNotNull(firstLeader);
        String firstLeaderId = firstLeader.getRaftNode().getNodeId();
        long firstIndex = submitCreate(firstLeader, "allocator-first");
        assertEquals(1, firstIndex, "The first direct submit must use index 1");
        waitForAppliedOnAll(1, 10_000);

        MetadataServer survivor = firstLeaderId.equals("meta-a") ? serverB : serverA;
        if (firstLeaderId.equals("meta-b")) {
            survivor = serverA;
        }
        if (firstLeaderId.equals("meta-c")) {
            survivor = serverA;
        }
        assertNotEquals(firstLeaderId, survivor.getRaftNode().getNodeId());

        firstLeader.stop();
        if (firstLeader == serverA) serverA = null;
        if (firstLeader == serverB) serverB = null;
        if (firstLeader == serverC) serverC = null;

        waitForLeader(15_000);
        MetadataServer secondLeader = leader();
        assertNotNull(secondLeader);
        assertNotEquals(firstLeaderId, secondLeader.getRaftNode().getNodeId());
        assertTrue(secondLeader.getRaftNode().getRaftLog().getEntry(1) != null,
            "The new leader must retain the first committed entry");
        assertEquals(2, secondLeader.getRaftNode().getNextLogIndex(),
            "The new leader's allocator must begin at index 2");

        long secondIndex = submitCreate(secondLeader, "allocator-second");
        assertEquals(2, secondIndex, "The post-failover submit must use index 2");
        System.out.println("[REGRESSION] old index=" + firstIndex
            + ", new leader next index=2, new index=" + secondIndex);

        waitForAppliedOnAll(2, 10_000);
        RaftLog newLeaderLog = secondLeader.getRaftNode().getRaftLog();
        assertEquals(2, newLeaderLog.getLastLogIndex());
        assertEquals(2, newLeaderLog.getCommitIndex());
        assertEquals(2, newLeaderLog.getLastApplied());
        assertNotNull(newLeaderLog.getEntry(1));
        assertNotNull(newLeaderLog.getEntry(2));
        assertNull(newLeaderLog.getEntry(3));
        assertTrue(secondLeader.getMetadataStore().objectExists("allocator-first"));
        assertTrue(secondLeader.getMetadataStore().objectExists("allocator-second"));

        for (MetadataServer server : Arrays.asList(serverA, serverB, serverC)) {
            if (server == null) continue;
            assertEquals(2, server.getRaftNode().getRaftLog().getCommitIndex(),
                server.getRaftNode().getNodeId() + " commit index");
            assertEquals(2, server.getRaftNode().getRaftLog().getLastApplied(),
                server.getRaftNode().getNodeId() + " last applied");

            // The new leader must have fully replicated the second entry to
            // every surviving peer before submit() reports success.  The
            // stopped leader is intentionally excluded because its transport
            // failure leaves its replication cursor pending for a later retry.
            if (server != secondLeader) {
                String peerId = server.getRaftNode().getNodeId();
                assertEquals(3, secondLeader.getRaftNode().getNextIndex(peerId),
                    "nextIndex for " + peerId);
                assertEquals(2, secondLeader.getRaftNode().getMatchIndex(peerId),
                    "matchIndex for " + peerId);
            }
        }
    }

    private ClusterConfig config(String nodeId, int clientPort, int raftPort, RaftPeer... peers) {
        return new ClusterConfig("allocator-regression", nodeId, "127.0.0.1",
            clientPort, raftPort, List.of(peers));
    }

    private MetadataServer start(String nodeId, int clientPort, ClusterConfig config) throws Exception {
        Path stateDir = clusterDir.resolve(nodeId);
        Files.createDirectories(stateDir);
        MetadataServer server = new MetadataServer(clientPort,
            stateDir.resolve("metadata.json"), 2, 6000, 2000, config, stateDir);
        server.getRaftNode().setLogEntryApplier(entry -> {
            try {
                server.getStateMachine().apply(entry);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        server.getRaftNode().start();
        server.getRaftNode().waitForRpcServerReady();
        return server;
    }

    private MetadataServer leader() {
        for (MetadataServer server : Arrays.asList(serverA, serverB, serverC)) {
            if (server != null && server.getRaftNode().isLeader()) return server;
        }
        return null;
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (leader() != null) return;
            Thread.sleep(50);
        }
        fail("Leader election timeout");
    }

    private long submitCreate(MetadataServer leader, String objectName) throws Exception {
        byte[] data = OBJECT_MAPPER.writeValueAsBytes(
            new ObjectMetadata(objectName, 1024L, 4096));
        LogEntry entry = LogEntry.create(leader.getRaftNode().getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT, data);
        assertTrue(leader.getRaftNode().submit(entry));
        return entryIndex(leader, objectName);
    }

    private long entryIndex(MetadataServer leader, String objectName) {
        for (long index = 1; index <= leader.getRaftNode().getRaftLog().getLastLogIndex(); index++) {
            LogEntry entry = leader.getRaftNode().getRaftLog().getEntry(index);
            if (entry != null) {
                try {
                    ObjectMetadata metadata = OBJECT_MAPPER.readValue(entry.data(), ObjectMetadata.class);
                    if (objectName.equals(metadata.getObjectName())) return index;
                } catch (Exception ignored) {
                    // Ignore unrelated entries.
                }
            }
        }
        fail("Entry not found for " + objectName);
        return -1;
    }

    private void waitForAppliedOnAll(long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean applied = true;
            for (MetadataServer server : Arrays.asList(serverA, serverB, serverC)) {
                if (server != null && server.getRaftNode().getRaftLog().getLastApplied() < target) {
                    applied = false;
                    break;
                }
            }
            if (applied) return;
            Thread.sleep(50);
        }
        fail("Timed out waiting for all survivors to apply index " + target);
    }
}
