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

    /**
     * Records a heartbeat, updating timestamp and restoring ACTIVE status.
     */
    public void recordHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.status = NodeStatus.ACTIVE;
    }

    /**
     * Checks if this node has timed out based on the given timeout in milliseconds.
     */
    public boolean isTimedOut(long timeoutMillis) {
        return (System.currentTimeMillis() - lastHeartbeat) > timeoutMillis;
    }
}
