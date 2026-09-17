package com.storix.client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Command-line interface for Storix.
 *
 * Commands:
 *   storix put <file>
 *   storix get <object> <output>
 *   storix delete <object>
 *   storix info <object>
 *   storix list
 *   storix status
 *   storix repair
 */
public class StorixCLI {

    private static final String DEFAULT_METADATA_HOST = "localhost";
    private static final int DEFAULT_METADATA_PORT = 9090;
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1 MB

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0].toLowerCase();

        try {
            switch (command) {
                case "put" -> handlePut(args);
                case "get" -> handleGet(args);
                case "delete" -> handleDelete(args);
                case "info" -> handleInfo(args);
                case "list" -> handleList();
                case "status" -> handleStatus();
                case "repair" -> handleRepair();
                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void handlePut(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: storix put <file>");
            System.exit(1);
        }

        Path filePath = Path.of(args[1]);
        if (!filePath.toFile().exists()) {
            System.err.println("File not found: " + filePath);
            System.exit(1);
        }

        int chunkSize = parseChunkSize(args, 2);

        try (StorixClient client = createClient(chunkSize)) {
            client.putFile(filePath);
        }
    }

    private static void handleGet(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: storix get <object> <output>");
            System.exit(1);
        }

        String objectName = args[1];
        Path outputPath = Path.of(args[2]);

        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            client.getFile(objectName, outputPath);
        }
    }

    private static void handleDelete(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: storix delete <object>");
            System.exit(1);
        }

        String objectName = args[1];

        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            client.deleteObject(objectName);
        }
    }

    private static void handleInfo(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: storix info <object>");
            System.exit(1);
        }

        String objectName = args[1];

        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            ObjectMetadataDTO metadata = client.getInfo(objectName);
            if (metadata == null) {
                System.out.println("Object not found: " + objectName);
                System.exit(1);
            }

            Map<String, Object> statusMap = client.getMetadataClient().getClusterStatus();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodesList = (List<Map<String, Object>>) statusMap.get("nodes");

            System.out.println("Object: " + metadata.getObjectName());
            System.out.println("Size: " + metadata.getFileSize() + " bytes");
            System.out.println("Chunks: " + metadata.getChunkCount());
            System.out.println("Chunk Size: " + metadata.getChunkSize() + " bytes");
            System.out.println("Replication Factor: " + statusMap.get("replicationFactor"));
            System.out.println();

            int overallDegradedCount = 0;

            for (ChunkInfoDTO chunk : metadata.getChunks()) {
                System.out.println("Chunk " + chunk.getChunkIndex() + ":");
                int healthyReplicaCount = 0;
                for (String replicaNodeId : chunk.getReplicaNodeIds()) {
                    String nodeStatus = "UNKNOWN";
                    if (nodesList != null) {
                        for (Map<String, Object> nodeMap : nodesList) {
                            if (replicaNodeId.equals(nodeMap.get("nodeId"))) {
                                nodeStatus = (String) nodeMap.get("status");
                                break;
                            }
                        }
                    }
                    if ("ACTIVE".equals(nodeStatus)) {
                        healthyReplicaCount++;
                    }
                    System.out.println("  " + replicaNodeId + " " + nodeStatus);
                }
                if (healthyReplicaCount < (Integer) statusMap.get("replicationFactor")) {
                    overallDegradedCount++;
                }
                System.out.println();
            }

            if (overallDegradedCount > 0) {
                System.out.println("Status: DEGRADED");
            } else {
                System.out.println("Status: HEALTHY");
            }
        }
    }

    private static void handleList() throws Exception {
        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            String[] objects = client.listObjects();
            if (objects.length == 0) {
                System.out.println("No objects found");
            } else {
                System.out.println("Objects:");
                for (String name : objects) {
                    System.out.println("  " + name);
                }
            }
        }
    }

    private static void handleStatus() throws Exception {
        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            Map<String, Object> status = client.getMetadataClient().getClusterStatus();

            System.out.println("Storix Cluster Status\n");

            Object healthState = status.getOrDefault("healthState", "UNKNOWN");
            Object lifecycleState = status.getOrDefault("lifecycleState", "UNKNOWN");
            System.out.printf("Health: %s (Lifecycle: %s)\n\n", healthState, lifecycleState);

            @SuppressWarnings("unchecked")
            Map<String, Object> raftSnapshot = (Map<String, Object>) status.get("raftSnapshot");
            if (raftSnapshot != null) {
                System.out.println("Raft Consensus:");
                System.out.printf("  Role: %s | Term: %s | Leader: %s | CommitIndex: %s | LastApplied: %s\n\n",
                        raftSnapshot.getOrDefault("role", status.get("raftState")),
                        raftSnapshot.getOrDefault("term", "0"),
                        raftSnapshot.getOrDefault("leaderId", "unknown"),
                        raftSnapshot.getOrDefault("commitIndex", "0"),
                        raftSnapshot.getOrDefault("lastApplied", "0"));
            } else {
                System.out.printf("Raft State: %s (Leader: %s)\n\n",
                        status.getOrDefault("raftState", "UNKNOWN"),
                        status.getOrDefault("isLeader", "false"));
            }

            System.out.println("Storage Nodes:");
            System.out.println("--------------------------------------------------------------------------------");
            System.out.printf("%-10s %-16s %-10s %-10s %-8s %-10s\n", "Node ID", "Address", "Status", "Health", "Chunks", "Used/Total");
            System.out.println("--------------------------------------------------------------------------------");

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) status.get("nodes");
            if (nodes != null) {
                for (Map<String, Object> node : nodes) {
                    String addr = String.format("%s:%d", node.get("host"), node.get("port"));
                    String nodeStatus = String.valueOf(node.getOrDefault("status", "UNKNOWN"));
                    String nodeHealth = String.valueOf(node.getOrDefault("healthState", "UNKNOWN"));
                    Object chunkCount = node.getOrDefault("chunkCount", 0);
                    long usedBytes = node.get("usedCapacityBytes") instanceof Number n ? n.longValue() : 0L;
                    long totalBytes = node.get("totalCapacityBytes") instanceof Number n ? n.longValue() : 0L;
                    String capacityStr = totalBytes > 0
                            ? String.format("%dMB/%dMB", usedBytes / (1024 * 1024), totalBytes / (1024 * 1024))
                            : "-";
                    System.out.printf("%-10s %-16s %-10s %-10s %-8s %-10s\n",
                            node.get("nodeId"), addr, nodeStatus, nodeHealth, chunkCount, capacityStr);
                }
            }
            System.out.println("--------------------------------------------------------------------------------\n");

            System.out.println("Cluster Data:");
            System.out.println("  Objects: " + status.getOrDefault("objects", 0));
            System.out.println("  Chunks: " + status.getOrDefault("chunks", 0));
            System.out.println("  Healthy nodes: " + status.getOrDefault("healthyNodes", 0) + "/" + status.getOrDefault("totalNodes", 0));
            System.out.println("  Replication: Healthy=" + status.getOrDefault("healthyChunks", 0) + ", Degraded=" + status.getOrDefault("degradedChunks", 0));
            System.out.println();

            @SuppressWarnings("unchecked")
            Map<String, Object> metrics = (Map<String, Object>) status.get("metrics");
            if (metrics != null) {
                System.out.println("Metrics Summary:");
                @SuppressWarnings("unchecked")
                Map<String, Object> reqMetrics = (Map<String, Object>) metrics.get("requests");
                if (reqMetrics != null) {
                    System.out.printf("  Requests: Total=%s, Success=%s, Failed=%s, Active=%s\n",
                            reqMetrics.getOrDefault("totalRequests", 0),
                            reqMetrics.getOrDefault("successfulRequests", 0),
                            reqMetrics.getOrDefault("failedRequests", 0),
                            reqMetrics.get("activeRequests"));
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> mgrMetrics = (Map<String, Object>) metrics.get("managers");
                if (mgrMetrics != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> repair = (Map<String, Object>) mgrMetrics.get("repair");
                    if (repair != null) {
                        System.out.printf("  Repair: Active=%s, Repaired=%s, Failed=%s\n",
                                repair.getOrDefault("activeRepairs", 0),
                                repair.getOrDefault("chunksRepaired", 0),
                                repair.getOrDefault("failedRepairs", 0));
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> recovery = (Map<String, Object>) mgrMetrics.get("recovery");
                    if (recovery != null) {
                        System.out.printf("  Recovery: Active=%s, Restored=%s, Failed=%s\n",
                                recovery.getOrDefault("activeRecoveries", 0),
                                recovery.getOrDefault("chunksRestored", 0),
                                recovery.getOrDefault("failedRecoveries", 0));
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> rebalance = (Map<String, Object>) mgrMetrics.get("rebalance");
                    if (rebalance != null) {
                        System.out.printf("  Rebalance: Active=%s, Moved=%s, Failed=%s\n",
                                rebalance.getOrDefault("activeMoves", 0),
                                rebalance.getOrDefault("chunksMoved", 0),
                                rebalance.getOrDefault("failedMoves", 0));
                    }
                }
            }
        }
    }

    private static void handleRepair() throws Exception {
        try (StorixClient client = createClient(DEFAULT_CHUNK_SIZE)) {
            Map<String, Object> result = client.getMetadataClient().repair();

            System.out.println("Repair Results:");
            System.out.println("Chunks Scanned: " + result.get("chunksScanned"));
            System.out.println("Chunks Repaired: " + result.get("chunksRepaired"));
            System.out.println("Chunks Failed: " + result.get("chunksFailed"));
            System.out.println("Chunks Already Healthy: " + result.get("chunksAlreadyHealthy"));
        }
    }

    private static StorixClient createClient(int chunkSize) {
        return new StorixClient(DEFAULT_METADATA_HOST, DEFAULT_METADATA_PORT, chunkSize);
    }

    private static int parseChunkSize(String[] args, int index) {
        if (args.length > index) {
            String sizeStr = args[index];
            if (sizeStr.endsWith("K") || sizeStr.endsWith("k")) {
                return Integer.parseInt(sizeStr.substring(0, sizeStr.length() - 1)) * 1024;
            } else if (sizeStr.endsWith("M") || sizeStr.endsWith("m")) {
                return Integer.parseInt(sizeStr.substring(0, sizeStr.length() - 1)) * 1024 * 1024;
            } else {
                return Integer.parseInt(sizeStr);
            }
        }
        return DEFAULT_CHUNK_SIZE;
    }

    private static void printUsage() {
        System.out.println("Usage: storix <command> [arguments]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  put <file> [chunkSize]  Upload a file");
        System.out.println("  get <object> <output>   Download an object");
        System.out.println("  delete <object>         Delete an object");
        System.out.println("  info <object>           Show object information");
        System.out.println("  list                    List all objects");
        System.out.println("  status                  Show cluster status");
        System.out.println("  repair                  Trigger manual repair of under-replicated chunks");
        System.out.println();
    }
}
