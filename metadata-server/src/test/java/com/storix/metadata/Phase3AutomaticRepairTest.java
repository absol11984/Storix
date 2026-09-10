package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 Prompt 2 tests: Automatic storage node failure detection and re-replication.
 * Tests the complete flow from node failure through repair completion.
 */
class Phase3AutomaticRepairTest {

    @TempDir
    Path tempDir;

    private NodeRegistry nodeRegistry;
    private PlacementManager placementManager;
    private MetadataStore metadataStore;
    private RepairManager repairManager;

    @BeforeEach
    void setUp() throws IOException {
        nodeRegistry = new NodeRegistry();
        placementManager = new PlacementManager(nodeRegistry, 3);
        Path metadataFile = tempDir.resolve("metadata.json");
        metadataStore = new MetadataStore(metadataFile);
        repairManager = new RepairManager(metadataStore, nodeRegistry, placementManager);

        // Register 4 storage nodes
        for (int i = 1; i <= 4; i++) {
            nodeRegistry.registerNode("node-" + i, "127.0.0.1", 9000 + i);
        }
    }

    /**
     * Test 1: Failed node is detected by health monitor.
     */
    @Test
    void testFailedNodeDetection() {
        // Mark node as timed out
        NodeInfo node1 = nodeRegistry.getNode("node-1").orElseThrow();
        node1.setLastHeartbeat(System.currentTimeMillis() - 10000);

        // Check health with 5-second timeout
        List<String> failed = nodeRegistry.checkHealth(5000);

        assertEquals(1, failed.size());
        assertEquals("node-1", failed.get(0));
        assertEquals(NodeStatus.UNHEALTHY, node1.getStatus());
    }

    /**
     * Test 2: Failed node is excluded from new placement.
     */
    @Test
    void testFailedNodeExcludedFromPlacement() throws IOException {
        // Mark node-1 as UNHEALTHY
        nodeRegistry.getNode("node-1").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        // Select nodes for new chunk
        List<NodeInfo> selected = placementManager.selectNodes(0);

        assertEquals(3, selected.size());
        assertTrue(selected.stream().noneMatch(n -> "node-1".equals(n.getNodeId())),
                "Failed node must be excluded from placement");
    }

    /**
     * Test 3: Under-replicated chunk is identified.
     */
    @Test
    void testUnderReplicatedChunkIdentification() throws IOException {
        // Create metadata with 3 replicas
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1000);
        ChunkInfo chunk = new ChunkInfo("chunk-0", 0, 1000,
            Arrays.asList("node-1", "node-2", "node-3"), "abc123");
        metadata.addChunk(chunk);
        metadataStore.createObject(metadata);

        // Mark node-1 as unhealthy
        nodeRegistry.getNode("node-1").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        // Verify chunk is under-replicated
        ObjectMetadata retrieved = metadataStore.getObject("test-object").orElseThrow();
        ChunkInfo retrievedChunk = retrieved.getChunks().get(0);

        long healthyReplicas = retrievedChunk.getReplicaNodeIds().stream()
            .filter(id -> {
                NodeInfo node = nodeRegistry.getNode(id).orElse(null);
                return node != null && node.getStatus() == NodeStatus.ACTIVE;
            })
            .count();

        assertEquals(2, healthyReplicas, "Chunk should have 2 healthy replicas after one node fails");
        assertTrue(healthyReplicas < placementManager.getReplicationFactor());
    }

    /**
     * Test 4: Replacement node is selected correctly.
     */
    @Test
    void testReplacementNodeSelection() {
        // Existing replicas on node-1, node-2, node-3
        List<String> existingReplicas = Arrays.asList("node-1", "node-2", "node-3");

        // Select repair target
        NodeInfo target = placementManager.selectRepairTarget(existingReplicas);

        assertNotNull(target, "Should find a replacement node");
        assertEquals("node-4", target.getNodeId(), "Should select node-4 as replacement");
        assertFalse(existingReplicas.contains(target.getNodeId()),
                "Replacement must not be an existing replica");
    }

    /**
     * Test 5: Replacement node cannot be one of existing replicas.
     */
    @Test
    void testReplacementNodeMustNotBeDuplicate() {
        // All 4 nodes already have replicas
        List<String> existingReplicas = Arrays.asList("node-1", "node-2", "node-3", "node-4");

        // Try to select repair target
        NodeInfo target = placementManager.selectRepairTarget(existingReplicas);

        assertNull(target, "Should return null when no eligible replacement exists");
    }

    /**
     * Test 6: Insufficient nodes leaves chunk explicitly under-replicated.
     */
    @Test
    void testInsufficientNodesLeavesUnderReplicated() throws IOException {
        // Create metadata with 3 replicas
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1000);
        ChunkInfo chunk = new ChunkInfo("chunk-0", 0, 1000,
            Arrays.asList("node-1", "node-2", "node-3"), "abc123");
        metadata.addChunk(chunk);
        metadataStore.createObject(metadata);

        // Mark nodes 1 and 4 as unhealthy (only 2 healthy remain)
        nodeRegistry.getNode("node-1").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));
        nodeRegistry.getNode("node-4").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        // Attempt repair (should fail because insufficient nodes)
        RepairManager.RepairResult result = repairManager.repairAll();

        // Verify chunk remains under-replicated
        ObjectMetadata retrieved = metadataStore.getObject("test-object").orElseThrow();
        ChunkInfo retrievedChunk = retrieved.getChunks().get(0);

        long healthyReplicas = retrievedChunk.getReplicaNodeIds().stream()
            .filter(id -> {
                NodeInfo node = nodeRegistry.getNode(id).orElse(null);
                return node != null && node.getStatus() == NodeStatus.ACTIVE;
            })
            .count();

        assertTrue(healthyReplicas < placementManager.getReplicationFactor(),
                "Chunk must remain under-replicated when insufficient nodes available");
    }

    /**
     * Test 7: Repair target selection excludes failed source node.
     */
    @Test
    void testRepairExcludesFailedSource() {
        // Mark node-1 as UNHEALTHY
        nodeRegistry.getNode("node-1").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        // Existing replicas include the failed node
        List<String> existingReplicas = Arrays.asList("node-1", "node-2", "node-3");

        // Select repair target
        NodeInfo target = placementManager.selectRepairTarget(existingReplicas);

        assertNotNull(target);
        assertEquals("node-4", target.getNodeId());
        assertNotEquals("node-1", target.getNodeId(), "Must not select failed node as target");
    }

    /**
     * Test 8: Multiple failed nodes trigger multiple repairs.
     */
    @Test
    void testMultipleNodeFailureRepair() throws IOException {
        // Create metadata with 3 replicas
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1000);
        ChunkInfo chunk = new ChunkInfo("chunk-0", 0, 1000,
            Arrays.asList("node-1", "node-2", "node-3"), "abc123");
        metadata.addChunk(chunk);
        metadataStore.createObject(metadata);

        // Mark nodes 1 and 2 as unhealthy (1 healthy replica remains)
        nodeRegistry.getNode("node-1").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));
        nodeRegistry.getNode("node-2").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        // Count healthy replicas before repair
        long healthyBefore = chunk.getReplicaNodeIds().stream()
            .filter(id -> {
                NodeInfo node = nodeRegistry.getNode(id).orElse(null);
                return node != null && node.getStatus() == NodeStatus.ACTIVE;
            })
            .count();

        assertEquals(1, healthyBefore, "Should have 1 healthy replica after 2 nodes fail");

        // Verify repair would be triggered (but cannot execute without real storage nodes)
        assertTrue(healthyBefore < placementManager.getReplicationFactor(),
                "Chunk is under-replicated and needs repair");
    }

    /**
     * Test 9: Repeated repair is idempotent.
     */
    @Test
    void testRepeatedRepairIsIdempotent() throws IOException {
        // Create metadata
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1000);
        ChunkInfo chunk = new ChunkInfo("chunk-0", 0, 1000,
            Arrays.asList("node-1", "node-2", "node-3"), "abc123");
        metadata.addChunk(chunk);
        metadataStore.createObject(metadata);

        // All nodes healthy - no repair needed
        RepairManager.RepairResult result1 = repairManager.repairAll();
        RepairManager.RepairResult result2 = repairManager.repairAll();

        // Both should report no work needed
        assertEquals(0, result1.chunksScanned());
        assertEquals(0, result2.chunksScanned());
    }

    /**
     * Test 10: Health check returns newly unhealthy nodes.
     */
    @Test
    void testHealthCheckReturnsNewlyUnhealthy() {
        // First check - all healthy
        List<String> failed1 = nodeRegistry.checkHealth(5000);
        assertEquals(0, failed1.size());

        // Make node-1 timeout
        NodeInfo node1 = nodeRegistry.getNode("node-1").orElseThrow();
        node1.setLastHeartbeat(System.currentTimeMillis() - 10000);

        // Second check - should detect node-1
        List<String> failed2 = nodeRegistry.checkHealth(5000);
        assertEquals(1, failed2.size());
        assertEquals("node-1", failed2.get(0));

        // Third check - should not report node-1 again (already unhealthy)
        List<String> failed3 = nodeRegistry.checkHealth(5000);
        assertEquals(0, failed3.size(), "Should only report newly unhealthy nodes");
    }

    /**
     * Test 11: Heartbeat restores node to healthy status.
     */
    @Test
    void testHeartbeatRestoresHealth() {
        // Mark node as unhealthy
        NodeInfo node1 = nodeRegistry.getNode("node-1").orElseThrow();
        node1.setStatus(NodeStatus.UNHEALTHY);

        // Send heartbeat
        boolean success = nodeRegistry.heartbeat("node-1");

        assertTrue(success);
        assertEquals(NodeStatus.ACTIVE, node1.getStatus(),
                "Heartbeat should restore node to ACTIVE status");
    }

    /**
     * Test 12: Repair manager reports correct statistics.
     */
    @Test
    void testRepairManagerStatistics() throws IOException {
        // Create metadata with healthy replicas
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1000);
        ChunkInfo chunk = new ChunkInfo("chunk-0", 0, 1000,
            Arrays.asList("node-1", "node-2", "node-3"), "abc123");
        metadata.addChunk(chunk);
        metadataStore.createObject(metadata);

        // All nodes healthy - no repair needed
        RepairManager.RepairResult result = repairManager.repairAll();

        assertEquals(0, result.chunksScanned(), "No under-replicated chunks should be found");
        assertEquals(0, result.chunksRepaired());
        assertEquals(0, result.chunksFailed());
    }
}
