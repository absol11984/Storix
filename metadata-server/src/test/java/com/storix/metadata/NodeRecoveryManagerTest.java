package com.storix.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NodeRecoveryManagerTest {

    private static final String HOST = "127.0.0.1";

    private static final byte OK = 0;
    private static final byte ERROR = 1;

    @TempDir
    java.nio.file.Path tempDir;

    private NodeRegistry registry;
    private MetadataStore store;
    private ChunkOperationLock chunkOperationLock;
    private NodeRecoveryManager manager;

    private MockStorageNode a;
    private MockStorageNode b;
    private MockStorageNode c;
    private MockStorageNode d;

    @BeforeEach
    void setUp() throws IOException {
        registry = new NodeRegistry();
        store = new MetadataStore(tempDir.resolve("metadata.json"));
        chunkOperationLock = new ChunkOperationLock();

        // Create mock storage nodes for recovery network calls.
        a = new MockStorageNode("node-a", HOST, 1000);
        b = new MockStorageNode("node-b", HOST, 1000);
        c = new MockStorageNode("node-c", HOST, 1000);
        d = new MockStorageNode("node-d", HOST, 1000);

        a.start();
        b.start();
        c.start();
        d.start();

        // Use a very small timeout window to keep hasRecentHeartbeat transitions deterministic.
        manager = new NodeRecoveryManager(registry, store, chunkOperationLock, 50L);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.stop();
        }
        if (a != null) a.stop();
        if (b != null) b.stop();
        if (c != null) c.stop();
        if (d != null) d.stop();
    }

    private void registerAll() throws IOException {
        registry.registerNode("node-a", HOST, a.getPort(), a.getTotalCapacityBytes(), a.getUsedCapacityBytes());
        registry.registerNode("node-b", HOST, b.getPort(), b.getTotalCapacityBytes(), b.getUsedCapacityBytes());
        registry.registerNode("node-c", HOST, c.getPort(), c.getTotalCapacityBytes(), c.getUsedCapacityBytes());
        registry.registerNode("node-d", HOST, d.getPort(), d.getTotalCapacityBytes(), d.getUsedCapacityBytes());
    }

    private void setNodeStatus(String nodeId, NodeStatus status) {
        registry.getNode(nodeId).ifPresent(n -> n.setStatus(status));
    }

    private void setLastHeartbeat(String nodeId, long epochMillis) {
        registry.getNode(nodeId).ifPresent(n -> n.setLastHeartbeat(epochMillis));
    }

    private void driveTo(String nodeId, NodeRecoveryState targetState, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            manager.recoverAll();
            NodeRecoveryState cur = manager.getRecoveryState(nodeId);
            if (cur == targetState) {
                return;
            }
            Thread.sleep(10);
        }
        fail("Timed out waiting for " + nodeId + " to reach " + targetState + ", last=" + manager.getRecoveryState(nodeId));
    }

    private void driveInitFailed(String nodeId) throws Exception {
        // First scan initializes state based on current registry status.
        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState(nodeId));
        assertTrue(registry.getNode(nodeId).orElseThrow().isRecoveryHold());
    }

    private ObjectMetadata createObject(String objectName, ChunkInfo... chunks) throws IOException {
        ObjectMetadata meta = new ObjectMetadata(objectName, 0, 0);
        for (ChunkInfo chunk : chunks) {
            meta.addChunk(chunk);
        }
        store.createObject(meta);
        return meta;
    }

    private ChunkInfo chunk(String chunkId, int idx, int size, List<String> replicas, String checksum) {
        return new ChunkInfo(chunkId, idx, size, replicas, checksum);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------
    // 1. failed node returns detectable + begins recovery
    // 2. health check succeeds -> READY
    // ------------------------------

    @Test
    void testFailedNodeBecomesReadyOnHeartbeatResumption() throws Exception {
        registerAll();

        String chunkId = "chunk-1";
        byte[] good = bytes("test");
        String checksum = sha256Hex(good);

        ChunkInfo chunk = chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum);
        createObject("obj1", chunk);

        a.setChunk(chunkId, good);
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        // Simulate node-a failure detected by health monitor.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);

        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));
        assertTrue(registry.getNode("node-a").orElseThrow().isRecoveryHold());

        // Simulate heartbeats arriving while recoveryHold keeps it UNHEALTHY.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);

        driveTo("node-a", NodeRecoveryState.READY, 2000);
        assertFalse(registry.getNode("node-a").orElseThrow().isRecoveryHold());
        assertEquals(NodeStatus.ACTIVE, registry.getNode("node-a").orElseThrow().getStatus());
    }

    // ------------------------------
    // 3. capacity refreshed during health check
    // ------------------------------

    @Test
    void testRecoveryHealthCheckRefreshesCapacityTelemetry() throws Exception {
        registerAll();

        String chunkId = "chunk-cap";
        byte[] good = new byte[123];
        for (int i = 0; i < good.length; i++) good[i] = (byte) (i & 0xff);
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        // node-a has the chunk locally; start used capacity as 0 in the registry.
        a.setChunk(chunkId, good);
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        // Re-register node-a with used=0 to ensure we can see it refresh.
        registry.registerNode("node-a", HOST, a.getPort(), 1000, 0);
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);

        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));

        // Heartbeat arrives while still UNHEALTHY.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);

        driveTo("node-a", NodeRecoveryState.READY, 2000);

        NodeInfo nodeA = registry.getNode("node-a").orElseThrow();
        assertEquals(1000, nodeA.getTotalCapacityBytes());
        assertEquals(good.length, nodeA.getUsedCapacityBytes());
    }

    // ------------------------------
    // 4. repeated registration is idempotent (no extra reconcile/puts)
    // ------------------------------

    @Test
    void testRepeatedNodeRegistrationDoesNotRetriggerRecovery() throws Exception {
        registerAll();

        String chunkId = "chunk-1";
        byte[] good = bytes("test");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        a.setChunk(chunkId, good);
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);

        // Init -> FAILED.
        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));

        // Heartbeat arrives, start reconcile; should verify but not put.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        int putsAfterFirst = a.getPutCount(chunkId).get();
        assertEquals(0, putsAfterFirst, "Valid chunk should not be overwritten during recovery");

        // Re-registration should only refresh heartbeat/capacity, not restart recovery.
        registry.registerNode("node-a", HOST, a.getPort(), 1000, a.getUsedCapacityBytes());

        manager.recoverAll();
        assertEquals(NodeRecoveryState.READY, manager.getRecoveryState("node-a"));

        assertEquals(putsAfterFirst, a.getPutCount(chunkId).get(), "No additional PUTs on re-registration");
    }

    // ------------------------------
    // 5. valid existing chunk retained (no overwrite)
    // ------------------------------

    @Test
    void testValidChunkRetainedWithoutOverwrite() throws Exception {
        registerAll();

        String chunkId = "chunk-valid";
        byte[] good = bytes("hello");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        // node-a already has correct bytes.
        a.setChunk(chunkId, good);
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertEquals(0, a.getPutCount(chunkId).get(), "Recovery should not overwrite a valid chunk");
        assertArrayEquals(good, a.getChunkBytes(chunkId));
    }

    // ------------------------------
    // 6. missing expected chunk repaired
    // ------------------------------

    @Test
    void testMissingExpectedChunkIsRepaired() throws Exception {
        registerAll();

        String chunkId = "chunk-missing";
        byte[] good = bytes("recovery");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        // node-a is missing; b/c have correct bytes.
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        // Heartbeat arrives.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertTrue(a.hasChunk(chunkId), "Missing chunk must be repaired");
        assertEquals(1, a.getPutCount(chunkId).get(), "Exactly one PUT expected for missing chunk");
        assertArrayEquals(good, a.getChunkBytes(chunkId));
    }

    // ------------------------------
    // 7. corrupt expected chunk repaired
    // ------------------------------

    @Test
    void testCorruptExpectedChunkIsRepaired() throws Exception {
        registerAll();

        String chunkId = "chunk-corrupt";
        byte[] good = bytes("correct-bytes");
        byte[] bad = bytes("bad-bytes");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        a.setChunk(chunkId, bad);
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertTrue(a.hasChunk(chunkId));
        assertArrayEquals(good, a.getChunkBytes(chunkId));
        assertTrue(a.getPutCount(chunkId).get() >= 1, "Corrupt chunk should be overwritten");
    }

    // ------------------------------
    // 8. stale recovered chunk replaced
    // ------------------------------

    @Test
    void testStaleRecoveredChunkReplacedWhenChecksumMismatches() throws Exception {
        registerAll();

        String chunkId = "chunk-stale";
        byte[] expected = bytes("new-version");
        byte[] stale = bytes("old-version");
        String checksum = sha256Hex(expected);

        createObject("obj1", chunk(chunkId, 0, expected.length, List.of("node-a", "node-b", "node-c"), checksum));

        a.setChunk(chunkId, stale);
        b.setChunk(chunkId, expected);
        c.setChunk(chunkId, expected);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertArrayEquals(expected, a.getChunkBytes(chunkId));
        assertTrue(a.getPutCount(chunkId).get() >= 1, "Stale bytes must be replaced");
    }

    // ------------------------------
    // 9. unexpected orphan chunk not blindly deleted
    // ------------------------------

    @Test
    void testOrphanChunkNotDeleted() throws Exception {
        registerAll();

        String expectedChunkId = "chunk-expected";
        byte[] expectedBytes = bytes("keep-me");
        String checksum = sha256Hex(expectedBytes);

        String orphanChunkId = "chunk-orphan";
        byte[] orphanBytes = bytes("orphan-data");

        createObject("obj1", chunk(expectedChunkId, 0, expectedBytes.length,
                List.of("node-a", "node-b", "node-c"), checksum));

        // node-a has both expected chunk and orphan chunk.
        a.setChunk(expectedChunkId, expectedBytes);
        a.setChunk(orphanChunkId, orphanBytes);

        b.setChunk(expectedChunkId, expectedBytes);
        c.setChunk(expectedChunkId, expectedBytes);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertTrue(a.hasChunk(orphanChunkId), "Orphan data should not be deleted during recovery");
        assertArrayEquals(orphanBytes, a.getChunkBytes(orphanChunkId));
    }

    // ------------------------------
    // 10. failure during recovery keeps node unavailable
    // ------------------------------

    @Test
    void testRecoveryFailureReturnsNodeToUnavailable() throws Exception {
        registerAll();

        String chunkId = "chunk-failput";
        byte[] good = bytes("repair-me");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        // node-a missing chunk; configure node-a to fail PUT for this chunk.
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);
        a.setPutErrorForChunk(chunkId);

        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));

        // Heartbeat arrives; recovery should attempt reconcile and fail.
        setLastHeartbeat("node-a", System.currentTimeMillis());

        // One scan is enough to attempt.
        manager.recoverAll();

        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"), "Recovery failure should keep FAILED");
        assertEquals(NodeStatus.UNHEALTHY, registry.getNode("node-a").orElseThrow().getStatus(), "Failed node remains unavailable");
        assertFalse(a.hasChunk(chunkId), "Chunk must not be marked repaired when PUT fails");
    }

    // ------------------------------
    // 11. repeated recovery is idempotent (no extra PUTs)
    // ------------------------------

    @Test
    void testRepeatedRecoveryIsIdempotent() throws Exception {
        registerAll();

        String chunkId = "chunk-idempotent";
        byte[] good = bytes("stable-bytes");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        // Initial: node-a missing.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();

        // Start first recovery.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        int putsAfterFirst = a.getPutCount(chunkId).get();
        assertTrue(putsAfterFirst >= 1);

        // Trigger recovery again: READY -> FAILED on heartbeat loss/UNHEALTHY.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));
        assertTrue(registry.getNode("node-a").orElseThrow().isRecoveryHold());

        // Heartbeats resume while UNHEALTHY.
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        int putsAfterSecond = a.getPutCount(chunkId).get();
        assertEquals(putsAfterFirst, putsAfterSecond, "Second recovery should not re-put a valid chunk");
    }

    // ------------------------------
    // 12. concurrent recovery of same node prevented
    // ------------------------------

    @Test
    void testConcurrentRecoveryOfSameNodeIsPrevented() throws Exception {
        registerAll();

        String chunkId = "chunk-concurrent";
        byte[] good = bytes("concurrent-bytes");
        String checksum = sha256Hex(good);

        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        // node-a missing chunk; configure PUT on node-a to block.
        CountDownLatch putStarted = new CountDownLatch(1);
        CountDownLatch allowPut = new CountDownLatch(1);
        a.blockPutForChunk(chunkId, putStarted, allowPut);

        // Initialize FAILED.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();
        assertEquals(NodeRecoveryState.FAILED, manager.getRecoveryState("node-a"));

        // Heartbeat arrives while still UNHEALTHY.
        setLastHeartbeat("node-a", System.currentTimeMillis());

        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<?> t1 = exec.submit(() -> manager.recoverAll());

        assertTrue(putStarted.await(2000, TimeUnit.MILLISECONDS), "PUT must start in thread-1");

        // Thread-2 attempts recovery while node-a reconcile is still blocked.
        Future<?> t2 = exec.submit(() -> manager.recoverAll());

        // Allow thread-1 to finish.
        allowPut.countDown();

        t1.get(2000, TimeUnit.MILLISECONDS);
        t2.get(2000, TimeUnit.MILLISECONDS);
        exec.shutdownNow();

        driveTo("node-a", NodeRecoveryState.READY, 2000);

        assertEquals(1, a.getPutCount(chunkId).get(), "Only one recovery PUT should occur under concurrency");
    }

    // ------------------------------
    // 13. recovered node does not trigger additional duplicate replica creation
    // ------------------------------

    @Test
    void testRecoveredNodeDoesNotCauseDuplicateReplicaCreationViaRepair() throws Exception {
        registerAll();

        String chunkId = "chunk-no-dup";
        byte[] good = bytes("no-dup");
        String checksum = sha256Hex(good);

        List<String> replicas = List.of("node-a", "node-b", "node-c");
        createObject("obj1", chunk(chunkId, 0, good.length, replicas, checksum));

        // Make node-a missing so recovery has work.
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);

        // Start recovery.
        manager.recoverAll();
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        // After recovery, RepairManager should not add any extra replicas (already healthy).
        PlacementManager placement = new PlacementManager(registry, 3);
        RepairManager repair = new RepairManager(store, registry, placement, 4, chunkOperationLock);

        ChunkInfo before = store.getObject("obj1").orElseThrow().getChunks().get(0);
        int replicaCountBefore = before.getReplicaNodeIds().size();
        Set<String> replicaSetBefore = new HashSet<>(before.getReplicaNodeIds());

        RepairManager.RepairResult res = repair.repairAll();
        assertEquals(0, res.chunksRepaired(), "No repairs expected after recovery to READY");

        ChunkInfo after = store.getObject("obj1").orElseThrow().getChunks().get(0);
        assertEquals(replicaCountBefore, after.getReplicaNodeIds().size());
        assertEquals(replicaSetBefore, new HashSet<>(after.getReplicaNodeIds()));
    }

    // ------------------------------
    // 14. recovery does not cause replica-count oscillation
    // ------------------------------

    @Test
    void testRecoveryDoesNotOscillateReplicaCountWhenRepairRanWhileNodeDown() throws Exception {
        registerAll();

        String chunkId = "chunk-no-osc";
        byte[] good = bytes("oscillation");
        String checksum = sha256Hex(good);

        // Metadata expects RF=3 on A,B,C.
        createObject("obj1", chunk(chunkId, 0, good.length, List.of("node-a", "node-b", "node-c"), checksum));

        // Source bytes on B/C.
        b.setChunk(chunkId, good);
        c.setChunk(chunkId, good);

        // Node-a is unhealthy -> healthy replicas count is 2.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);

        PlacementManager placement = new PlacementManager(registry, 3);
        RepairManager repair = new RepairManager(store, registry, placement, 4, chunkOperationLock);

        RepairManager.RepairResult res1 = repair.repairAll();
        assertEquals(1, res1.chunksRepaired(), "Repair should add one extra replica when node-a is down");

        ChunkInfo mid = store.getObject("obj1").orElseThrow().getChunks().get(0);
        Set<String> replicasMid = new HashSet<>(mid.getReplicaNodeIds());
        assertTrue(replicasMid.contains("node-d"), "With 4 nodes, the repaired destination should be node-d");
        int replicaCountMid = mid.getReplicaNodeIds().size();
        assertEquals(4, replicaCountMid, "Replica set should temporarily grow to include extra replica while node-a is down");

        // Now recover node-a; should reconcile bytes without changing replica metadata.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis());
        manager.recoverAll();
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        ChunkInfo afterRecovery = store.getObject("obj1").orElseThrow().getChunks().get(0);
        assertEquals(replicaCountMid, afterRecovery.getReplicaNodeIds().size(), "Recovery should not oscillate replica set size");

        // Re-run repair; should NOT add more replicas.
        RepairManager.RepairResult res2 = repair.repairAll();
        assertEquals(0, res2.chunksRepaired(), "No additional repairs expected after node-a is healthy again");

        ChunkInfo afterRepair = store.getObject("obj1").orElseThrow().getChunks().get(0);
        assertEquals(replicaCountMid, afterRepair.getReplicaNodeIds().size(), "Replica set size must remain stable after recovery");
        assertEquals(new HashSet<>(afterRepair.getReplicaNodeIds()), replicasMid);
    }

    // ------------------------------
    // 15. recovered node later participates in controlled rebalancing
    // ------------------------------

    @Test
    void testRecoveredNodeParticipatesInRebalanceAsDestination() throws Exception {
        registerAll();

        // Chunk c0 is expected to be on node-a (so recovery touches A), but will remain valid.
        String c0 = "chunk-c0";
        byte[] c0Bytes = bytes("c0");
        String c0Checksum = sha256Hex(c0Bytes);
        createObject("obj1",
                chunk(c0, 0, c0Bytes.length, List.of("node-a", "node-b", "node-c"), c0Checksum));

        a.setChunk(c0, c0Bytes);
        b.setChunk(c0, c0Bytes);
        c.setChunk(c0, c0Bytes);

        // Force node-a into FAILED then to READY.
        setNodeStatus("node-a", NodeStatus.UNHEALTHY);
        setLastHeartbeat("node-a", System.currentTimeMillis() - 10_000L);
        manager.recoverAll();
        setLastHeartbeat("node-a", System.currentTimeMillis());
        driveTo("node-a", NodeRecoveryState.READY, 2000);

        // Chunk c1 has RF=3 replicas on B,C,D and NOT on A.
        // We expect rebalancer to move the overloaded replica B -> recovered underloaded A.
        String c1 = "chunk-c1";
        byte[] c1Bytes = bytes("c1-bytes");
        String c1Checksum = sha256Hex(c1Bytes);

        // Add c1 to the same object.
        ObjectMetadata meta = store.getObject("obj1").orElseThrow();
        meta.addChunk(chunk(c1, 1, c1Bytes.length, List.of("node-b", "node-c", "node-d"), c1Checksum));
        store.save();

        // Capacity model: B overloaded, A underloaded.
        registry.getNode("node-b").orElseThrow().setTotalCapacityBytes(1000);
        registry.getNode("node-b").orElseThrow().setUsedCapacityBytes(900); // > 0.80
        registry.getNode("node-c").orElseThrow().setTotalCapacityBytes(1000);
        registry.getNode("node-c").orElseThrow().setUsedCapacityBytes(500);
        registry.getNode("node-d").orElseThrow().setTotalCapacityBytes(1000);
        registry.getNode("node-d").orElseThrow().setUsedCapacityBytes(400);

        // A used ratio is driven by c0 bytes via recovery; keep it under 0.30.
        NodeInfo nodeA = registry.getNode("node-a").orElseThrow();
        assertTrue(nodeA.getUsedCapacityBytes() / (double) nodeA.getTotalCapacityBytes() < 0.30);

        // Fake transfer holds the chunk data.
        ChunkTransferFake transfer = new ChunkTransferFake();
        transfer.put("node-b", c1, c1Bytes);
        transfer.put("node-c", c1, c1Bytes);
        transfer.put("node-d", c1, c1Bytes);

        PlacementManager placement = new PlacementManager(registry, 3);
        RebalanceManager rebalance = new RebalanceManager(
                store,
                registry,
                placement,
                chunkOperationLock,
                /*raftNode=*/ null,
                0.80,
                0.30,
                30_000,
                transfer,
                RebalanceManager.FailureInjector.noop());

        RebalanceManager.RebalanceResult res = rebalance.rebalanceOnce();
        assertEquals(1, res.chunksMoved(), "One overloaded replica should be moved");

        ChunkInfo updated = store.getObject("obj1").orElseThrow().getChunks().stream()
                .filter(ch -> ch.getChunkId().equals(c1)).findFirst().orElseThrow();

        assertFalse(updated.getReplicaNodeIds().contains("node-b"), "Overloaded source should be removed");
        assertTrue(updated.getReplicaNodeIds().contains("node-a"), "Recovered node should be added as destination");
    }

    // =====================================================================
    // Mock storage-node implementation (TCP GET/PUT/VERIFY/LIST)
    // =====================================================================

    private static class MockStorageNode {
        private final String nodeId;
        private final String host;
        private final long totalCapacityBytes;

        private final Map<String, byte[]> chunks = new ConcurrentHashMap<>();
        private final AtomicInteger putAttempts = new AtomicInteger(0);
        private final AtomicInteger verifyAttempts = new AtomicInteger(0);

        private final Map<String, AtomicInteger> putCountPerChunk = new ConcurrentHashMap<>();

        private final ExecutorService acceptExec;
        private volatile boolean running = false;
        private ServerSocketChannel serverChannel;

        private volatile CountDownLatch putStartLatch;
        private volatile CountDownLatch allowPutLatch;
        private volatile String blockingPutChunkId;

        private final Set<String> putErrorChunks = ConcurrentHashMap.newKeySet();

        MockStorageNode(String nodeId, String host, long totalCapacityBytes) {
            this.nodeId = nodeId;
            this.host = host;
            this.totalCapacityBytes = totalCapacityBytes;
            this.acceptExec = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "mock-storage-" + nodeId);
                t.setDaemon(true);
                return t;
            });
        }

        void start() throws IOException {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(host, 0));
            running = true;
            acceptExec.submit(this::acceptLoop);
        }

        void stop() {
            running = false;
            if (acceptExec != null) {
                acceptExec.shutdownNow();
            }
            if (serverChannel != null) {
                try {
                    serverChannel.close();
                } catch (IOException ignored) {
                }
            }
        }

        int getPort() throws IOException {
            if (serverChannel == null) {
                throw new IllegalStateException("Server not started yet");
            }
            return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        }

        long getTotalCapacityBytes() {
            return totalCapacityBytes;
        }

        long getUsedCapacityBytes() {
            return chunks.values().stream().mapToLong(b -> b != null ? b.length : 0).sum();
        }

        void setChunk(String chunkId, byte[] data) {
            chunks.put(chunkId, data);
        }

        boolean hasChunk(String chunkId) {
            return chunks.containsKey(chunkId);
        }

        byte[] getChunkBytes(String chunkId) {
            return chunks.get(chunkId);
        }

        void setPutErrorForChunk(String chunkId) {
            putErrorChunks.add(chunkId);
        }

        AtomicInteger getPutCount(String chunkId) {
            return putCountPerChunk.computeIfAbsent(chunkId, k -> new AtomicInteger(0));
        }

        void blockPutForChunk(String chunkId, CountDownLatch putStarted, CountDownLatch allowPut) {
            this.blockingPutChunkId = chunkId;
            this.putStartLatch = putStarted;
            this.allowPutLatch = allowPut;
        }

        private void acceptLoop() {
            while (running) {
                try {
                    SocketChannel ch = serverChannel.accept();
                    if (ch == null) continue;
                    try {
                        handleOne(ch);
                    } finally {
                        ch.close();
                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[MOCK] accept failed for " + nodeId + ": " + e.getMessage());
                    }
                }
            }
        }

        private void handleOne(SocketChannel ch) throws IOException {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (!readFully(ch, lenBuf)) return;
            lenBuf.flip();
            int reqLen = lenBuf.getInt();
            if (reqLen <= 0 || reqLen > 10 * 1024 * 1024) {
                return;
            }
            ByteBuffer req = ByteBuffer.allocate(reqLen);
            readFully(ch, req);
            req.flip();

            byte opcode = req.get();
            switch (opcode) {
                case 5 -> handleListNodeState(ch, req);
                case 2 -> handleGetChunk(ch, req);
                case 1 -> handlePutChunk(ch, req);
                case 4 -> handleVerifyChunk(ch, req);
                default -> sendResponse(ch, ERROR, ("Unknown opcode " + opcode).getBytes(StandardCharsets.UTF_8));
            }
        }

        private void handleListNodeState(SocketChannel ch, ByteBuffer req) throws IOException {
            ByteBuffer payload = buildInventoryPayload();
            sendResponseRaw(ch, OK, payload);
        }

        private void handleGetChunk(SocketChannel ch, ByteBuffer req) throws IOException {
            int cidLen = req.getInt();
            byte[] cidBytes = new byte[cidLen];
            req.get(cidBytes);
            String chunkId = new String(cidBytes, StandardCharsets.UTF_8);

            byte[] data = chunks.get(chunkId);
            if (data == null) {
                sendResponse(ch, ERROR, ("MISSING:" + chunkId).getBytes(StandardCharsets.UTF_8));
                return;
            }
            sendResponse(ch, OK, data);
        }

        private void handlePutChunk(SocketChannel ch, ByteBuffer req) throws IOException {
            int cidLen = req.getInt();
            byte[] cidBytes = new byte[cidLen];
            req.get(cidBytes);
            String chunkId = new String(cidBytes, StandardCharsets.UTF_8);

            int dataLen = req.getInt();
            byte[] data = new byte[dataLen];
            req.get(data);

            putAttempts.incrementAndGet();
            putCountPerChunk.computeIfAbsent(chunkId, k -> new AtomicInteger(0)).incrementAndGet();

            if (putErrorChunks.contains(chunkId)) {
                sendResponse(ch, ERROR, ("PUT_INJECTED_FAIL").getBytes(StandardCharsets.UTF_8));
                return;
            }

            if (chunkId.equals(blockingPutChunkId) && putStartLatch != null) {
                putStartLatch.countDown();
            }
            if (chunkId.equals(blockingPutChunkId) && allowPutLatch != null) {
                try {
                    allowPutLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }

            setChunk(chunkId, data);
            sendResponse(ch, OK, new byte[0]);
        }

        private void handleVerifyChunk(SocketChannel ch, ByteBuffer req) throws IOException {
            int cidLen = req.getInt();
            byte[] cidBytes = new byte[cidLen];
            req.get(cidBytes);
            String chunkId = new String(cidBytes, StandardCharsets.UTF_8);

            int checksumLen = req.getInt();
            byte[] checksumBytes = new byte[checksumLen];
            req.get(checksumBytes);
            String expectedChecksum = new String(checksumBytes, StandardCharsets.UTF_8);

            byte[] data = chunks.get(chunkId);
            if (data == null) {
                sendResponse(ch, ERROR, ("MISSING:" + chunkId).getBytes(StandardCharsets.UTF_8));
                return;
            }

            verifyAttempts.incrementAndGet();
            String actualChecksum = sha256Hex(data);
            if (actualChecksum.equalsIgnoreCase(expectedChecksum)) {
                sendResponse(ch, OK, new byte[0]);
            } else {
                sendResponse(ch, ERROR, ("CHECKSUM_MISMATCH").getBytes(StandardCharsets.UTF_8));
            }
        }

        private ByteBuffer buildInventoryPayload() {
            byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
            List<String> ids = new ArrayList<>(chunks.keySet());
            Collections.sort(ids);

            long used = chunks.values().stream().mapToLong(b -> b != null ? b.length : 0).sum();

            int totalLen = 4 + nodeIdBytes.length + 8 + 8 + 4;
            for (String id : ids) {
                byte[] b = id.getBytes(StandardCharsets.UTF_8);
                totalLen += 4 + b.length;
            }

            ByteBuffer buf = ByteBuffer.allocate(totalLen);
            buf.putInt(nodeIdBytes.length);
            buf.put(nodeIdBytes);
            buf.putLong(totalCapacityBytes);
            buf.putLong(used);
            buf.putInt(ids.size());
            for (String id : ids) {
                byte[] b = id.getBytes(StandardCharsets.UTF_8);
                buf.putInt(b.length);
                buf.put(b);
            }
            buf.flip();
            return buf;
        }

        private static void sendResponse(SocketChannel ch, byte status, byte[] data) throws IOException {
            ByteBuffer resp = ByteBuffer.allocate(1 + 4 + data.length);
            resp.put(status);
            resp.putInt(data.length);
            resp.put(data);
            resp.flip();

            ByteBuffer len = ByteBuffer.allocate(4);
            len.putInt(resp.remaining());
            len.flip();

            while (len.hasRemaining()) ch.write(len);
            while (resp.hasRemaining()) ch.write(resp);
        }

        private static void sendResponseRaw(SocketChannel ch, byte status, ByteBuffer payload) throws IOException {
            payload = payload.asReadOnlyBuffer();

            ByteBuffer resp = ByteBuffer.allocate(1 + 4 + payload.remaining());
            resp.put(status);
            resp.putInt(payload.remaining());
            resp.put(payload);
            resp.flip();

            ByteBuffer len = ByteBuffer.allocate(4);
            len.putInt(resp.remaining());
            len.flip();

            while (len.hasRemaining()) ch.write(len);
            while (resp.hasRemaining()) ch.write(resp);
        }

        private static boolean readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int r = ch.read(buf);
                if (r == -1) return false;
            }
            return true;
        }

        private static String sha256Hex(byte[] data) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(data);
                StringBuilder sb = new StringBuilder();
                for (byte b : hash) sb.append(String.format("%02x", b));
                return sb.toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ChunkTransferFake implements RebalanceManager.ChunkTransfer {
        private final ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>> data = new ConcurrentHashMap<>();

        void put(String nodeId, String chunkId, byte[] bytes) {
            data.computeIfAbsent(nodeId, k -> new ConcurrentHashMap<>()).put(chunkId, bytes);
        }

        @Override
        public byte[] getChunk(NodeInfo node, String chunkId) throws IOException {
            byte[] d = data.getOrDefault(node.getNodeId(), new ConcurrentHashMap<>()).get(chunkId);
            if (d == null) throw new IOException("Chunk not found for GET: " + node.getNodeId() + ":" + chunkId);
            return d;
        }

        @Override
        public void putChunk(NodeInfo node, String chunkId, byte[] chunkData) throws IOException {
            put(node.getNodeId(), chunkId, chunkData);
        }

        @Override
        public void deleteChunk(NodeInfo node, String chunkId) throws IOException {
            ConcurrentHashMap<String, byte[]> m = data.get(node.getNodeId());
            if (m != null) {
                m.remove(chunkId);
            }
        }
    }
}
