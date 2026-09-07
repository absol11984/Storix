package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Committed entry survival after leader failure.
 * Mandatory assertions:
 * - New leader has committed entry
 * - commitIndex >= entry.index on new leader
 * - lastApplied >= entry.index on new leader
 * - Exact metadata state convergence
 */
class RaftCommittedEntryLeaderFailureTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final AtomicLong portCounter = new AtomicLong(System.currentTimeMillis() % 10000);

    private int portA, portB, portC;
    private Path clusterDir;
    private Path dataDirA, dataDirB, dataDirC;
    private MetadataServer serverA, serverB, serverC;
    private ClusterConfig configA, configB, configC;

    @TempDir
    Path tempDir;

    private void setupCluster() throws Exception {
        long base = portCounter.addAndGet(10);
        portA = (int) (55000 + base % 10000);
        portB = portA + 10;
        portC = portA + 20;
        int raftPortA = portA + 1;
        int raftPortB = portB + 1;
        int raftPortC = portC + 1;

        clusterDir = Files.createTempDirectory("committed-entry-test-" + System.nanoTime());
        dataDirA = clusterDir.resolve("meta-a");
        dataDirB = clusterDir.resolve("meta-b");
        dataDirC = clusterDir.resolve("meta-c");
        Files.createDirectories(dataDirA);
        Files.createDirectories(dataDirB);
        Files.createDirectories(dataDirC);

        configA = new ClusterConfig("storix", "meta-a", "127.0.0.1", portA, raftPortA,
            List.of(new RaftPeer("meta-b", "127.0.0.1", raftPortB),
                    new RaftPeer("meta-c", "127.0.0.1", raftPortC)));

        configB = new ClusterConfig("storix", "meta-b", "127.0.0.1", portB, raftPortB,
            List.of(new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                    new RaftPeer("meta-c", "127.0.0.1", raftPortC)));

        configC = new ClusterConfig("storix", "meta-c", "127.0.0.1", portC, raftPortC,
            List.of(new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                    new RaftPeer("meta-b", "127.0.0.1", raftPortB)));
    }

    private MetadataServer startServer(int port, Path metaFile, ClusterConfig config, Path dataDir) throws Exception {
        MetadataServer server = new MetadataServer(port, metaFile, 2, 6000, 2000, config, dataDir);
        server.getRaftNode().start();
        server.getRaftNode().waitForRpcServerReady();
        return server;
    }

    private MetadataServer getLeader() {
        if (serverA != null && serverA.getRaftNode().isLeader()) return serverA;
        if (serverB != null && serverB.getRaftNode().isLeader()) return serverB;
        if (serverC != null && serverC.getRaftNode().isLeader()) return serverC;
        return null;
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeader() != null) return;
            Thread.sleep(50);
        }
        fail("Leader election timeout");
    }

    private long submitCreate(MetadataServer leader, String name) throws Exception {
        ObjectMetadata obj = new ObjectMetadata(name, 1024L, 4096);
        byte[] data = objectMapper.writeValueAsBytes(obj);
        LogEntry entry = LogEntry.create(leader.getRaftNode().getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT, data);
        boolean ok = leader.getRaftNode().submit(entry);
        assertTrue(ok, "submit() should succeed with majority available");
        return leader.getRaftNode().getRaftLog().getLastLogIndex();
    }

    private void waitForApplied(MetadataServer server, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (server.getRaftNode().getRaftLog().getLastApplied() >= target) return;
            Thread.sleep(50);
        }
        fail("Timed out waiting for lastApplied >= " + target + " on " +
             server.getRaftNode().getNodeId());
    }

    private Map<String, Object> captureState(MetadataServer server) {
        Map<String, Object> state = new HashMap<>();
        for (String name : server.getMetadataStore().listObjects()) {
            state.put(name, name);
        }
        return state;
    }

    private void wireApplier(MetadataServer server) {
        try {
            var field = RaftNode.class.getDeclaredField("stateMachineApplier");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.function.Consumer<LogEntry> applier = entry -> {
                try {
                    server.getStateMachine().apply(entry);
                } catch (Exception e) {
                    System.err.println("[APPLIER] Failed: " + e.getMessage());
                }
            };
            field.set(server.getRaftNode(), applier);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void teardown() {
        try { if (serverA != null) serverA.stop(); } catch (Exception e) { /* ignore */ }
        try { if (serverB != null) serverB.stop(); } catch (Exception e) { /* ignore */ }
        try { if (serverC != null) serverC.stop(); } catch (Exception e) { /* ignore */ }
        try {
            if (clusterDir != null) {
                Files.walk(clusterDir).sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(java.io.File::delete);
            }
        } catch (Exception e) { /* ignore */ }
        try { Thread.sleep(8000); } catch (InterruptedException e) { /* ignore */ }
    }

    /**
     * Test: Committed entry survives leader failure.
     *
     * Scenario:
     * 1. Cluster of 3 nodes elects leader
     * 2. Submit entries and wait for full replication + commitment
     * 3. Verify committed state on all nodes
     * 4. Kill the leader
     * 5. New leader is elected among survivors.
     * 6. Mandatory: new leader contains all committed entries
     * 7. Verify commitIndex and lastApplied on new leader
     * 8. Verify exact metadata state matches
     */
    @Test
    void testCommittedEntrySurvivesLeaderFailure() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Committed Entry Survives Leader Failure");
        System.out.println("========================================\n");

        // Phase 1: Start cluster
        setupCluster();
        serverA = startServer(portA, dataDirA.resolve("meta.json"), configA, dataDirA);
        serverB = startServer(portB, dataDirB.resolve("meta.json"), configB, dataDirB);
        serverC = startServer(portC, dataDirC.resolve("meta.json"), configC, dataDirC);
        wireApplier(serverA);
        wireApplier(serverB);
        wireApplier(serverC);

        waitForLeader(15000);
        MetadataServer leader = getLeader();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        System.out.println("Initial leader: " + leaderId);

        // Phase 2: Submit entries and wait for full commit + apply
        String objectName = "committed-entry-" + UUID.randomUUID();
        long entryIndex = submitCreate(leader, objectName);
        System.out.println("Submitted entry at index: " + entryIndex + " object=" + objectName);

        // Wait for commit and apply on all nodes
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s == null) continue;
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                long ci = s.getRaftNode().getRaftLog().getCommitIndex();
                long la = s.getRaftNode().getRaftLog().getLastApplied();
                if (ci >= entryIndex && la >= entryIndex) break;
                Thread.sleep(50);
            }
        }

        // Capture committed state before killing leader
        Map<String, Object> committedState = captureState(leader);
        System.out.println("Committed state before failure: " + committedState.keySet());

        long oldTerm = leader.getRaftNode().getCurrentTerm();
        long oldCommitIndex = leader.getRaftNode().getRaftLog().getCommitIndex();
        long oldLastApplied = leader.getRaftNode().getRaftLog().getLastApplied();
        System.out.println("Leader term=" + oldTerm + " commitIndex=" + oldCommitIndex + " lastApplied=" + oldLastApplied);

        // Verify committed state exists on all nodes before failure
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s == null) continue;
            long ci = s.getRaftNode().getRaftLog().getCommitIndex();
            long la = s.getRaftNode().getRaftLog().getLastApplied();
            assertTrue(ci >= entryIndex, s.getRaftNode().getNodeId() + " commitIndex should include entry");
            assertTrue(la >= entryIndex, s.getRaftNode().getNodeId() + " lastApplied should include entry");
            Map<String, Object> state = captureState(s);
            assertTrue(state.containsKey(objectName),
                s.getRaftNode().getNodeId() + " should contain committed object");
            System.out.println("  " + s.getRaftNode().getNodeId() + " before failure: commitIndex=" + ci +
                " lastApplied=" + la + " hasObject=" + state.containsKey(objectName));
        }

        // Phase 3: Kill the leader
        System.out.println("\nKilling leader: " + leaderId);
        leader.stop();
        Thread.sleep(200);

        // Phase 4: Wait for new leader election
        System.out.println("Waiting for new leader election...");
        waitForLeader(15000);

        MetadataServer newLeader = getLeader();
        assertNotNull(newLeader, "Should have a new leader");
        String newLeaderId = newLeader.getRaftNode().getNodeId();
        System.out.println("New leader: " + newLeaderId);
        System.out.println("New leader term: " + newLeader.getRaftNode().getCurrentTerm());

        // Verify new leader is a DIFFERENT node (old leader was stopped)
        assertNotEquals(leaderId, newLeaderId,
            "New leader should be different from killed leader");

        // Phase 5: MANDATORY ASSERTIONS - new leader has committed entry
        long newCommitIndex = newLeader.getRaftNode().getRaftLog().getCommitIndex();
        long newLastApplied = newLeader.getRaftNode().getRaftLog().getLastApplied();
        Map<String, Object> newLeaderState = captureState(newLeader);

        System.out.println("\nNew leader state:");
        System.out.println("  commitIndex: " + newCommitIndex);
        System.out.println("  lastApplied: " + newLastApplied);
        System.out.println("  hasCommittedObject: " + newLeaderState.containsKey(objectName));
        System.out.println("  state: " + newLeaderState.keySet());

        // Mandatory assertion 1: new leader has committed entry
        assertTrue(newLeaderState.containsKey(objectName),
            "New leader MUST contain the committed entry");

        // Mandatory assertion 2: commitIndex >= entry.index
        assertTrue(newCommitIndex >= entryIndex,
            "New leader commitIndex=" + newCommitIndex + " must be >= entryIndex=" + entryIndex);

        // Mandatory assertion 3: lastApplied >= entry.index
        assertTrue(newLastApplied >= entryIndex,
            "New leader lastApplied=" + newLastApplied + " must be >= entryIndex=" + entryIndex);

        // Mandatory assertion 4: exact metadata state convergence
        Map<String, Object> finalState = captureState(newLeader);
        assertEquals(committedState, finalState,
            "New leader state must exactly match committed state before failure");

        System.out.println("\n========================================");
        System.out.println("TEST: Committed Entry Survives Leader Failure - PASSED");
        System.out.println("========================================\n");
    }
}
