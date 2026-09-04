package com.storix.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Data transfer object for storage node information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfoDTO {
    private String nodeId;
    private String host;
    private int port;
    private String status;
    private long lastHeartbeat;

    public NodeInfoDTO() {}

    public NodeInfoDTO(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
}
