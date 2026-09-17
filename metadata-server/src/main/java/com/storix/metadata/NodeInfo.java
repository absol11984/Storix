package com.storix.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Information about a registered storage node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfo {

    private String nodeId;
    private String host;
    private int port;
    private NodeStatus status;
    private long lastHeartbeat;

    // When true, heartbeats should refresh capacity/lastHeartbeat without re-enabling
    // ordinary eligibility. Used during controlled node reintegration.
    private volatile boolean recoveryHold = false;

    // Set by recordHeartbeat so recovery can distinguish a heartbeat from a
    // status change made by the health monitor.
    private volatile boolean heartbeatArrived = false;

    // Capacity model (optional; unknown when totalCapacityBytes <= 0)
    // Used by capacity-aware placement/repair.
    private volatile long totalCapacityBytes = -1;
    private volatile long usedCapacityBytes = 0;

    // Optional aggregate telemetry supplied by the storage node heartbeat.
    private volatile long activeConnections;
    private volatile long chunkReadSuccesses;
    private volatile long chunkReadFailures;
    private volatile long chunkWriteSuccesses;
    private volatile long chunkWriteFailures;
    private volatile long checksumFailures;
    private volatile long chunkCount;

    public NodeInfo() {
        this.status = NodeStatus.ACTIVE;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.status = NodeStatus.ACTIVE;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }

    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public long getTotalCapacityBytes() { return totalCapacityBytes; }
    public void setTotalCapacityBytes(long totalCapacityBytes) { this.totalCapacityBytes = totalCapacityBytes; }

    public long getUsedCapacityBytes() { return usedCapacityBytes; }
    public void setUsedCapacityBytes(long usedCapacityBytes) { this.usedCapacityBytes = Math.max(0, usedCapacityBytes); }

    /**
     * Available capacity in bytes.
     *
     * If total capacity is unknown (<= 0), returns Long.MAX_VALUE so eligibility
     * checks do not exclude nodes based on missing telemetry.
     */
    public long getAvailableCapacityBytes() {
        if (totalCapacityBytes <= 0) {
            return Long.MAX_VALUE;
        }
        long available = totalCapacityBytes - usedCapacityBytes;
        return Math.max(0, available);
    }

    /**
     * Records a heartbeat, updating timestamp and restoring ACTIVE status.
     */
    public void recordHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        if (!recoveryHold) {
            this.status = NodeStatus.ACTIVE;
        }
    }

    public long getActiveConnections() { return activeConnections; }
    public long getChunkReadSuccesses() { return chunkReadSuccesses; }
    public long getChunkReadFailures() { return chunkReadFailures; }
    public long getChunkWriteSuccesses() { return chunkWriteSuccesses; }
    public long getChunkWriteFailures() { return chunkWriteFailures; }
    public long getChecksumFailures() { return checksumFailures; }
    public long getChunkCount() { return chunkCount; }

    public void updateTelemetry(long activeConnections, long chunkReadSuccesses, long chunkReadFailures,
                                long chunkWriteSuccesses, long chunkWriteFailures, long checksumFailures,
                                long chunkCount) {
        this.activeConnections = Math.max(0, activeConnections);
        this.chunkReadSuccesses = Math.max(0, chunkReadSuccesses);
        this.chunkReadFailures = Math.max(0, chunkReadFailures);
        this.chunkWriteSuccesses = Math.max(0, chunkWriteSuccesses);
        this.chunkWriteFailures = Math.max(0, chunkWriteFailures);
        this.checksumFailures = Math.max(0, checksumFailures);
        this.chunkCount = Math.max(0, chunkCount);
    }

    public boolean isRecoveryHold() {
        return recoveryHold;
    }

    public void setRecoveryHold(boolean recoveryHold) {
        this.recoveryHold = recoveryHold;
    }

    /**
     * Checks if this node has timed out based on the given timeout in milliseconds.
     */
    public boolean isTimedOut(long timeoutMillis) {
        return (System.currentTimeMillis() - lastHeartbeat) > timeoutMillis;
    }
}
