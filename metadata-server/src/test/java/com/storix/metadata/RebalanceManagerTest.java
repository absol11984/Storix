package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RebalanceManagerTest {
    @TempDir
    Path tempDir;

    private NodeRegistry registry;
    private MetadataStore store;
    private PlacementManager placementManager;
    private ChunkOperationLock chunkOperationLock;
    
    private FakeChunkTransfer transfer;
    private FakeFailureInjector injector;
    
    private RebalanceManager manager;

    @BeforeEach
    void setUp() throws IOException {
        registry = new NodeRegistry();
        store = new MetadataStore(tempDir.resolve("meta.json"));
        placementManager = new PlacementManager(registry, 3);
        chunkOperationLock = new ChunkOperationLock();
        
        transfer = new FakeChunkTransfer();
        injector = new FakeFailureInjector();
        
        manager = new RebalanceManager(
                store, registry, placementManager, chunkOperationLock,
                0.80, 0.30, 30_000, transfer, injector
        );
    }

    private void addNode(String id, long total, long used) {
        registry.registerNode(id, "127.0.0.1", 9000, total, used);
    }
    
    private void setNodeStatus(String id, NodeStatus status) {
        registry.getNode(id).ifPresent(n -> n.setStatus(status));
    }

    private void createObject(String objName, int rf, ChunkDef... chunks) throws IOException {
        ObjectMetadata meta = new ObjectMetadata(objName, 0, 0);
        for (ChunkDef def : chunks) {
            String checksum = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"; // sha256("test")
            ChunkInfo c = new ChunkInfo(def.id, 0, def.size, def.replicas, checksum);
            meta.addChunk(c);

            for (String replica : def.replicas) {
                transfer.storeData(replica, def.id, "test".getBytes());
            }
        }
        store.createObject(meta);
    }

    private record ChunkDef(String id, int size, List<String> replicas) {
        static ChunkDef of(String id, int size, String... nodes) {
            return new ChunkDef(id, size, Arrays.asList(nodes));
        }
    }

    @Test
    void testBalancedClusterNoMoves() throws IOException {
        addNode("A", 100, 50);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
    }

    @Test
    void testUnknownCapacityNoMoves() throws IOException {
        addNode("A", -1, 50);
        addNode("B", -1, 50);
        addNode("C", -1, 50);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
    }

    @Test
    void testOverloadDetectionAndDestEligibility() throws IOException {
        // A is overloaded (0.9), B is normal (0.5), C is underloaded (0.1), D is underloaded (0.2)
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 10);
        addNode("D", 100, 20);
        addNode("E", 100, 10); // But E will be unhealthy
        setNodeStatus("E", NodeStatus.UNHEALTHY);
        
        // Chunk is on A, B, X. We need 3 healthy replicas, so make X=C (but C holds it, so C is ineligible). Let's use F.
        addNode("F", 100, 50);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "F"));
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(1, res.chunksMoved());
        
        // C has the smallest usage (0.1) so it should be the destination.
        ObjectMetadata meta = store.getObject("obj1").get();
        List<String> replicas = meta.getChunks().get(0).getReplicaNodeIds();
        
        assertFalse(replicas.contains("A"), "Source A should be removed");
        assertTrue(replicas.contains("C"), "Dest C should be added");
        assertEquals(3, replicas.size());
        
        // Check bytes transferred
        assertTrue(transfer.hasData("C", "c1"));
        assertFalse(transfer.hasData("A", "c1"));
    }

    @Test
    void testUnderReplicatedChunkSkipped() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 10);
        
        // RF=3 but only 2 replicas
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B"));
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        assertEquals(1, res.chunksSkipped());
    }

    @Test
    void testChunkLockHeldSkip() throws Exception {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));

        // Acquire lock in another thread so this thread's tryAcquire fails
        CountDownLatch latch = new CountDownLatch(1);
        Thread locker = new Thread(() -> {
            chunkOperationLock.tryAcquire("c1");
            latch.countDown();
            try { Thread.sleep(5000); } catch (InterruptedException e) {}
            chunkOperationLock.release("c1");
        });
        locker.start();
        latch.await();

        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        assertEquals(1, res.chunksSkipped());

        locker.interrupt();
        locker.join();
    }

    @Test
    void testDestExceedsHighThresholdSkipped() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        // D is underloaded (0.2), but taking chunk of size 65 makes it 85/100 = 0.85 > 0.80
        addNode("D", 100, 20);
        
        createObject("obj1", 3, ChunkDef.of("c1", 65, "A", "B", "C"));
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        // No valid dest found because D would exceed threshold.
    }

    @Test
    void testSourceGetFailure() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        injector.failAt = RebalanceManager.FailureStep.AFTER_GET;
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        
        assertFalse(transfer.hasData("D", "c1"), "No PUT should occur if GET fails");
        List<String> replicas = store.getObject("obj1").get().getChunks().get(0).getReplicaNodeIds();
        assertTrue(replicas.contains("A"));
        assertFalse(replicas.contains("D"));
    }

    @Test
    void testDestPutFailure() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        injector.failAt = RebalanceManager.FailureStep.AFTER_PUT;
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        
        // The manager cleans up dest data on failure after put
        assertFalse(transfer.hasData("D", "c1"));
        assertTrue(transfer.hasData("A", "c1"));
    }

    @Test
    void testDestChecksumMismatch() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        transfer.corruptNextPut = true;
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        
        // Dest cleaned up, metadata unchanged
        assertFalse(transfer.hasData("D", "c1"));
        assertTrue(transfer.hasData("A", "c1"));
        List<String> replicas = store.getObject("obj1").get().getChunks().get(0).getReplicaNodeIds();
        assertTrue(replicas.contains("A"));
    }

    @Test
    void testMetadataPersistFailure() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        injector.failAt = RebalanceManager.FailureStep.BEFORE_METADATA_PERSIST;
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(0, res.chunksMoved());
        
        assertFalse(transfer.hasData("D", "c1"), "Dest should be cleaned up on MD persist fail");
        assertTrue(transfer.hasData("A", "c1"), "Source should be intact");
    }

    @Test
    void testSourceDeleteFailureWithMetadataSuccess() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        injector.failAt = RebalanceManager.FailureStep.AFTER_SOURCE_DELETE;
        
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        // The move loop logs error and continues - metadata has already succeeded so it counts as moved.
        assertEquals(1, res.chunksMoved());
        
        List<String> replicas = store.getObject("obj1").get().getChunks().get(0).getReplicaNodeIds();
        assertTrue(replicas.contains("D"));
        assertFalse(replicas.contains("A"));
        
        // By injection, we pretend the delete fails, but the mock doesn't actually prevent deletion
        // unless we mock it. Wait, the injector FAILS after source delete, which means delete 
        // threw exception inside our mock transfer, or the injector threw it.
        // Let's rely on the injector throwing an exception at AFTER_SOURCE_DELETE to simulate failure.
        // If injector throws, the catch block runs, but returns true! Let's check RebalanceManager code.
        // Yes, the code is:
        // try { failureInjector... chunkTransfer.deleteChunk... } catch (IOException) { log }... returns true
    }

    @Test
    void testLocalUsedCapacityAdjust() throws IOException {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        
        // Chunk size 20 means if moved, A goes to 70 (not overloaded).
        createObject("obj1", 3, ChunkDef.of("c1", 20, "A", "B", "C"));
        // A second chunk c2 also on A.
        createObject("obj2", 3, ChunkDef.of("c2", 20, "A", "B", "C"));
        
        // RebalanceOnce scans both. It moves c1 from A to D.
        // After c1 moves, A's local used capacity is adjusted to 70.
        // When checking c2, A is no longer overloaded (70 <= 80), so c2 is skipped.
        RebalanceManager.RebalanceResult res = manager.rebalanceOnce();
        assertEquals(1, res.chunksMoved());
        assertEquals(1, res.chunksSkipped());
        
        assertEquals(70, registry.getNode("A").get().getUsedCapacityBytes());
        assertEquals(30, registry.getNode("D").get().getUsedCapacityBytes());
    }

    @Test
    void testConcurrentMovesOfSameChunk() throws Exception {
        addNode("A", 100, 90);
        addNode("B", 100, 50);
        addNode("C", 100, 50);
        addNode("D", 100, 10);
        createObject("obj1", 3, ChunkDef.of("c1", 10, "A", "B", "C"));
        
        int nThreads = 4;
        ExecutorService exec = Executors.newFixedThreadPool(nThreads);
        List<Future<RebalanceManager.RebalanceResult>> futures = new ArrayList<>();
        
        for (int i = 0; i < nThreads; i++) {
            futures.add(exec.submit(() -> manager.rebalanceOnce()));
        }
        
        int movedCount = 0;
        int skippedCount = 0;
        for (Future<RebalanceManager.RebalanceResult> f : futures) {
            RebalanceManager.RebalanceResult r = f.get();
            movedCount += r.chunksMoved();
            skippedCount += r.chunksSkipped();
        }
        exec.shutdown();
        
        // Only one should succeed because of the chunk Operation Lock or moved sets
        assertEquals(1, movedCount);
        assertEquals(nThreads - 1, skippedCount);
        
        List<String> replicas = store.getObject("obj1").get().getChunks().get(0).getReplicaNodeIds();
        assertEquals(3, replicas.size());
        assertTrue(replicas.contains("D"));
        assertFalse(replicas.contains("A"));
    }

    static class FakeChunkTransfer implements RebalanceManager.ChunkTransfer {
        ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();
        volatile boolean corruptNextPut = false;

        private String key(NodeInfo node, String chunkId) {
            return node.getNodeId() + ":" + chunkId;
        }

        void storeData(String nodeId, String chunkId, byte[] content) {
            data.put(nodeId + ":" + chunkId, content);
        }

        boolean hasData(String nodeId, String chunkId) {
            return data.containsKey(nodeId + ":" + chunkId);
        }

        @Override
        public byte[] getChunk(NodeInfo node, String chunkId) throws IOException {
            byte[] d = data.get(key(node, chunkId));
            if (d == null) throw new IOException("Chunk not found for GET: " + key(node, chunkId));
            return d;
        }

        @Override
        public void putChunk(NodeInfo node, String chunkId, byte[] chunkData) throws IOException {
            byte[] toStore = chunkData;
            if (corruptNextPut) {
                toStore = "corrupted".getBytes();
                corruptNextPut = false;
            }
            data.put(key(node, chunkId), toStore);
        }

        @Override
        public void deleteChunk(NodeInfo node, String chunkId) throws IOException {
            data.remove(key(node, chunkId));
        }
    }

    static class FakeFailureInjector implements RebalanceManager.FailureInjector {
        RebalanceManager.FailureStep failAt;

        @Override
        public void maybeFail(RebalanceManager.FailureStep step, String objectName, String chunkId) throws IOException {
            if (step == failAt) {
                throw new IOException("Injected failure at " + step);
            }
        }
    }

}
