package com.storix.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Cluster-aware metadata client with automatic failover.
 * Maintains a list of all metadata server addresses and transparently handles
 * leader changes, connection failures, and request retries.
 */
public class ClusterMetadataClient implements AutoCloseable {

    private final List<InetSocketAddress> servers;
    private final int maxRetries;
    private MetadataClient currentClient;
    private InetSocketAddress currentServer;
    private InetSocketAddress discoveredLeader;

    public ClusterMetadataClient(List<InetSocketAddress> servers) {
        this(servers, servers.size() * 2);
    }

    public ClusterMetadataClient(List<InetSocketAddress> servers, int maxRetries) {
        this.servers = List.copyOf(servers);
        this.maxRetries = maxRetries;
    }

    /**
     * Connects to the first available server.
     */
    public void connect() throws IOException {
        connectWithRetry(0);
    }

    /**
     * Connects to a specific server.
     */
    public void connect(InetSocketAddress server) throws IOException {
        closeCurrentClient();
        currentClient = new MetadataClient(server.getHostString(), server.getPort());
        currentClient.connect();
        currentServer = server;
    }

    private void connectWithRetry(int attempt) throws IOException {
        if (attempt >= maxRetries) {
            throw new IOException("Failed to connect to any metadata server after " + maxRetries + " attempts");
        }

        for (InetSocketAddress server : servers) {
            try {
                closeCurrentClient();
                currentClient = new MetadataClient(server.getHostString(), server.getPort());
                currentClient.connect();
                currentServer = server;
                return;
            } catch (IOException e) {
                // Try next server
            }
        }

        // All servers failed, wait and retry
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        connectWithRetry(attempt + 1);
    }

    /**
     * Creates an object, retrying on NOT_LEADER or connection failures.
     */
    public void createObject(ObjectMetadataDTO metadata) throws IOException {
        executeVoidWithRetry(() -> { currentClient.createObject(metadata); return null; });
    }

    /**
     * Gets an object, retrying on connection failures.
     */
    public ObjectMetadataDTO getObject(String objectName) throws IOException {
        return executeWithRetry(() -> currentClient.getObject(objectName));
    }

    /**
     * Updates an object, retrying on NOT_LEADER or connection failures.
     */
    public void updateObject(ObjectMetadataDTO metadata) throws IOException {
        executeVoidWithRetry(() -> { currentClient.updateObject(metadata); return null; });
    }

    /**
     * Deletes an object, retrying on NOT_LEADER or connection failures.
     */
    public boolean deleteObject(String objectName) throws IOException {
        return executeWithRetry(() -> currentClient.deleteObject(objectName));
    }

    /**
     * Lists all objects, retrying on connection failures.
     */
    public String[] listObjects() throws IOException {
        return executeWithRetry(() -> currentClient.listObjects());
    }

    /**
     * Registers a storage node.
     */
    public void registerNode(NodeInfoDTO nodeInfo) throws IOException {
        executeVoidWithRetry(() -> { currentClient.registerNode(nodeInfo); return null; });
    }

    /**
     * Sends a heartbeat.
     */
    public void heartbeat(String nodeId) throws IOException {
        executeVoidWithRetry(() -> { currentClient.heartbeat(nodeId); return null; });
    }

    /**
     * Gets all registered nodes.
     */
    public NodeInfoDTO[] getNodes() throws IOException {
        return executeWithRetry(() -> currentClient.getNodes());
    }

    /**
     * Gets chunk placement for a given index.
     */
    public NodeInfoDTO[] getPlacement(int chunkIndex) throws IOException {
        return executeWithRetry(() -> currentClient.getPlacement(chunkIndex));
    }

    /**
     * Gets cluster status.
     */
    public java.util.Map<String, Object> getClusterStatus() throws IOException {
        return executeWithRetry(() -> currentClient.getClusterStatus());
    }

    /**
     * Triggers a repair operation.
     */
    public java.util.Map<String, Object> repair() throws IOException {
        return executeWithRetry(() -> currentClient.repair());
    }

    /**
     * Executes an operation with automatic retry on failures.
     */
    private <T> T executeWithRetry(Operation<T> operation) throws IOException {
        int attempts = 0;
        IOException lastException = null;

        while (attempts < maxRetries) {
            try {
                // Ensure connected
                if (currentClient == null) {
                    connect();
                }

                T result = operation.execute();
                return result;

            } catch (IOException e) {
                lastException = e;
                attempts++;

                String message = e.getMessage() != null ? e.getMessage() : "";

                // Check if it's a NOT_LEADER error
                if (message.contains("NOT_LEADER") || message.contains("not the leader")) {
                    // Try to discover the leader from cluster status
                    if (discoverLeader()) {
                        continue;
                    }
                }

                // Close current connection and try next server
                closeCurrentClient();

                // Try to reconnect to a different server
                if (attempts < maxRetries) {
                    try {
                        Thread.sleep(50); // Brief backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        throw new IOException("Operation failed after " + attempts + " attempts", lastException);
    }

    /**
     * Executes a void operation with automatic retry on failures.
     */
    private void executeVoidWithRetry(Operation<Void> operation) throws IOException {
        executeWithRetry(operation);
    }

    /**
     * Attempts to discover the current leader from cluster status.
     * Returns true if leader was discovered and we connected to it.
     */
    private boolean discoverLeader() {
        // Try each server to get cluster status
        for (InetSocketAddress server : servers) {
            if (server.equals(currentServer)) {
                continue; // Skip current server
            }

            MetadataClient testClient = null;
            try {
                testClient = new MetadataClient(server.getHostString(), server.getPort());
                testClient.connect();

                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> status = testClient.getClusterStatus();

                if (status != null && status.containsKey("leader")) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> leader = (java.util.Map<String, Object>) status.get("leader");
                    if (leader != null) {
                        String leaderHost = (String) leader.get("host");
                        int leaderPort = ((Number) leader.get("port")).intValue();

                        InetSocketAddress leaderAddr = new InetSocketAddress(leaderHost, leaderPort);
                        connect(leaderAddr);
                        discoveredLeader = leaderAddr;
                        return true;
                    }
                }
            } catch (IOException e) {
                // Try next server
            } finally {
                if (testClient != null) {
                    try {
                        testClient.close();
                    } catch (IOException ignored) {}
                }
            }
        }

        return false;
    }

    /**
     * Returns the current server we're connected to.
     */
    public InetSocketAddress getCurrentServer() {
        return currentServer;
    }

    /**
     * Returns the discovered leader address, if known.
     */
    public InetSocketAddress getDiscoveredLeader() {
        return discoveredLeader;
    }

    private void closeCurrentClient() {
        if (currentClient != null) {
            try {
                currentClient.close();
            } catch (IOException ignored) {}
            currentClient = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrentClient();
    }

    @FunctionalInterface
    private interface Operation<T> {
        T execute() throws IOException;
    }
}
