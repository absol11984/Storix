package com.storix.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Cluster-aware metadata client with automatic leader routing and failover.
 * Maintains a list of all metadata server addresses and transparently handles:
 * - Leader discovery
 * - Follower redirect (NOT_LEADER responses)
 * - Leader caching
 * - Connection failure retry
 * - Bounded retries
 * - Request idempotency via client/request IDs
 */
public class ClusterMetadataClient implements AutoCloseable {

    private final List<InetSocketAddress> servers;
    private final int maxAttempts;
    private final long connectionTimeoutMs;
    private final long requestTimeoutMs;
    private final String clientId;

    private MetadataClient currentClient;
    private InetSocketAddress currentServer;
    private volatile InetSocketAddress cachedLeader;
    private volatile boolean closed = false;

    // Current request tracking - set once per logical request and reused on retries
    private String currentRequestId;

    public ClusterMetadataClient(List<InetSocketAddress> servers) {
        this(servers, 3, 5000, 30000);
    }

    public ClusterMetadataClient(List<InetSocketAddress> servers, int maxAttempts) {
        this(servers, maxAttempts, 5000, 30000);
    }

    public ClusterMetadataClient(List<InetSocketAddress> servers, int maxAttempts,
                                 long connectionTimeoutMs, long requestTimeoutMs) {
        this.servers = List.copyOf(servers);
        this.maxAttempts = Math.max(1, maxAttempts);
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Generates a unique request ID for idempotency.
     */
    public String generateRequestId() {
        return clientId + "-" + System.nanoTime();
    }

    /**
     * Prepares a new request by generating a unique request ID.
     * This should be called before each logical operation.
     */
    public void beginRequest() {
        this.currentRequestId = generateRequestId();
    }

    /**
     * Sets a specific request ID for idempotency.
     * Used when the request ID must be preserved across retries.
     */
    public void setRequestId(String requestId) {
        this.currentRequestId = requestId;
    }

    /**
     * Gets the current request ID.
     */
    public String getRequestId() {
        return currentRequestId;
    }

    /**
     * Gets the client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Clears the current request ID after the operation completes.
     */
    private void clearRequestId() {
        this.currentRequestId = null;
    }

    /**
     * Sets the clientId and requestId on the current MetadataClient.
     */
    private void setClientRequestId() {
        if (currentClient != null && currentRequestId != null) {
            currentClient.setClientId(clientId);
            currentClient.setCurrentRequestId(currentRequestId);
        }
    }

    /**
     * Connects to the first available server.
     */
    public void connect() throws IOException {
        if (currentClient != null && currentClient.isConnected()) {
            return;
        }
        connectToAnyServer();
    }

    /**
     * Connects to a specific server.
     */
    public void connect(InetSocketAddress server) throws IOException {
        ensureNotClosed();
        closeCurrentClient();
        currentClient = new MetadataClient(server.getHostString(), server.getPort());
        currentClient.setConnectionTimeout(connectionTimeoutMs);
        currentClient.setRequestTimeout(requestTimeoutMs);
        currentClient.connect();
        currentServer = server;
    }

    private void connectToAnyServer() throws IOException {
        ensureNotClosed();

        // First try cached leader if available
        if (cachedLeader != null) {
            try {
                connect(cachedLeader);
                return;
            } catch (IOException e) {
                // Leader unavailable, will try other servers
                cachedLeader = null;
            }
        }

        // Try each server
        IOException lastException = null;
        for (InetSocketAddress server : servers) {
            try {
                connect(server);
                return;
            } catch (IOException e) {
                lastException = e;
                // Try next server
            }
        }

        throw new IOException("Failed to connect to any metadata server. Last error: " +
                (lastException != null ? lastException.getMessage() : "unknown"), lastException);
    }

    /**
     * Connects to a specific server by index.
     */
    private void connectToServer(int index) throws IOException {
        closeCurrentClient();
        InetSocketAddress server = servers.get(index % servers.size());
        connect(server);
    }

    /**
     * Creates an object with automatic leader routing.
     */
    public void createObject(ObjectMetadataDTO metadata) throws IOException {
        beginRequest();
        try {
            executeVoidWithLeaderRouting(() -> {
                setClientRequestId();
                currentClient.createObject(metadata);
                return null;
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Gets an object.
     * Retries on NOT_FOUND responses (may be a follower that hasn't received
     * the data through replication yet).
     */
    public ObjectMetadataDTO getObject(String objectName) throws IOException {
        beginRequest();
        try {
            return executeWithLeaderRoutingForObjectGet(() -> {
                setClientRequestId();
                return currentClient.getObject(objectName);
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Updates an object with automatic leader routing.
     */
    public void updateObject(ObjectMetadataDTO metadata) throws IOException {
        beginRequest();
        try {
            executeVoidWithLeaderRouting(() -> {
                setClientRequestId();
                currentClient.updateObject(metadata);
                return null;
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Deletes an object with automatic leader routing.
     */
    public boolean deleteObject(String objectName) throws IOException {
        beginRequest();
        Boolean[] result = new Boolean[1];
        try {
            executeVoidWithLeaderRouting(() -> {
                setClientRequestId();
                result[0] = currentClient.deleteObject(objectName);
                return null;
            });
        } finally {
            clearRequestId();
        }
        return result[0] != null && result[0];
    }

    /**
     * Lists all objects.
     */
    public String[] listObjects() throws IOException {
        return executeWithLeaderRouting(() -> currentClient.listObjects());
    }

    /**
     * Registers a storage node.
     */
    public void registerNode(NodeInfoDTO nodeInfo) throws IOException {
        beginRequest();
        try {
            executeVoidWithLeaderRouting(() -> {
                setClientRequestId();
                currentClient.registerNode(nodeInfo);
                return null;
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Sends a heartbeat.
     */
    public void heartbeat(String nodeId) throws IOException {
        beginRequest();
        try {
            executeVoidWithLeaderRouting(() -> {
                setClientRequestId();
                currentClient.heartbeat(nodeId);
                return null;
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Gets all registered nodes.
     */
    public NodeInfoDTO[] getNodes() throws IOException {
        beginRequest();
        try {
            return executeWithLeaderRouting(() -> {
                setClientRequestId();
                return currentClient.getNodes();
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Gets chunk placement for a given index.
     */
    public NodeInfoDTO[] getPlacement(int chunkIndex) throws IOException {
        beginRequest();
        try {
            return executeWithLeaderRouting(() -> {
                setClientRequestId();
                return currentClient.getPlacement(chunkIndex);
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Gets cluster status.
     */
    public Map<String, Object> getClusterStatus() throws IOException {
        beginRequest();
        try {
            return executeWithLeaderRouting(() -> {
                setClientRequestId();
                return currentClient.getClusterStatus();
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Triggers a repair operation.
     */
    public Map<String, Object> repair() throws IOException {
        beginRequest();
        try {
            return executeWithLeaderRouting(() -> {
                setClientRequestId();
                return currentClient.repair();
            });
        } finally {
            clearRequestId();
        }
    }

    /**
     * Executes an operation with leader routing and bounded retries.
     * Cycles through all servers when leader info is unknown.
     */
    private <T> T executeWithLeaderRouting(Operation<T> operation) throws IOException {
        ensureNotClosed();

        int attempts = 0;
        IOException lastException = null;
        int serverIndex = 0;  // Track which server to try next

        while (attempts < maxAttempts) {
            attempts++;

            try {
                // Ensure connected - cycle through servers to find one that knows the leader
                if (currentClient == null || !currentClient.isConnected()) {
                    connectToServer(serverIndex % servers.size());
                    serverIndex++;
                }

                return operation.execute();

            } catch (IOException e) {
                lastException = e;
                String message = e.getMessage() != null ? e.getMessage() : "";

                // Check if it's a NOT_LEADER error with leader info
                LeaderInfo leaderInfo = extractLeaderInfo(e);
                if (leaderInfo != null && leaderInfo != LeaderInfo.UNKNOWN) {
                    // Got leader info - try to connect to leader
                    System.out.println("[CLIENT] Received NOT_LEADER, leader=" + leaderInfo);
                    if (tryConnectToLeader(leaderInfo)) {
                        continue; // Retry with new leader
                    }
                    // Leader connect failed, invalidate cache and try other servers
                    cachedLeader = null;
                    closeCurrentClient();
                } else if (leaderInfo == LeaderInfo.UNKNOWN || isConnectionFailure(e)) {
                    // Unknown leader or connection failed - try a different server
                    System.out.println("[CLIENT] " + (leaderInfo == LeaderInfo.UNKNOWN ? "Unknown leader" : "Connection failure") + ", trying another server");
                    cachedLeader = null;
                    closeCurrentClient();
                    serverIndex++;
                } else if (message.contains("Object not found") || message.contains("NOT_FOUND")) {
                    // Object not found on this server - might be a follower that hasn't received
                    // the data through replication yet. Try another server.
                    System.out.println("[CLIENT] Object not found, trying another server");
                    cachedLeader = null;
                    closeCurrentClient();
                    serverIndex++;
                }

                // Brief backoff before retry
                if (attempts < maxAttempts) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry", ie);
                    }
                }
            }
        }

        throw new IOException("Operation failed after " + attempts + " attempts. Last error: " +
                (lastException != null ? lastException.getMessage() : "unknown"), lastException);
    }

    /**
     * Executes an object GET operation with leader routing and null-return retry.
     * When getObject() returns null (NOT_FOUND), tries another server since the object
     * might exist on the leader but the request hit a follower that hasn't received
     * the data through replication yet.
     */
    private ObjectMetadataDTO executeWithLeaderRoutingForObjectGet(Operation<ObjectMetadataDTO> operation) throws IOException {
        ensureNotClosed();

        int attempts = 0;
        IOException lastException = null;
        int serverIndex = 0;

        while (attempts < maxAttempts) {
            attempts++;

            try {
                // Ensure connected - cycle through servers
                if (currentClient == null || !currentClient.isConnected()) {
                    connectToServer(serverIndex % servers.size());
                    serverIndex++;
                }

                ObjectMetadataDTO result = operation.execute();

                // If we got a non-null result, success!
                if (result != null) {
                    return result;
                }

                // Null result means NOT_FOUND - might be a follower that hasn't replicated yet
                System.out.println("[CLIENT] Object not found (null return), trying another server");
                cachedLeader = null;
                closeCurrentClient();
                serverIndex++;

                // Brief backoff before retry
                if (attempts < maxAttempts) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry", ie);
                    }
                }

            } catch (IOException e) {
                lastException = e;
                String message = e.getMessage() != null ? e.getMessage() : "";

                // Check if it's a NOT_LEADER error with leader info
                LeaderInfo leaderInfo = extractLeaderInfo(e);
                if (leaderInfo != null && leaderInfo != LeaderInfo.UNKNOWN) {
                    System.out.println("[CLIENT] Received NOT_LEADER, leader=" + leaderInfo);
                    if (tryConnectToLeader(leaderInfo)) {
                        continue;
                    }
                    cachedLeader = null;
                    closeCurrentClient();
                } else if (leaderInfo == LeaderInfo.UNKNOWN || isConnectionFailure(e)) {
                    System.out.println("[CLIENT] " + (leaderInfo == LeaderInfo.UNKNOWN ? "Unknown leader" : "Connection failure") + ", trying another server");
                    cachedLeader = null;
                    closeCurrentClient();
                    serverIndex++;
                }

                // Brief backoff before retry
                if (attempts < maxAttempts) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry", ie);
                    }
                }
            }
        }

        if (lastException != null) {
            throw new IOException("Operation failed after " + attempts + " attempts. Last error: " +
                    lastException.getMessage(), lastException);
        } else {
            // All attempts returned null
            return null;
        }
    }

    /**
     * Executes a void operation with leader routing.
     */
    private void executeVoidWithLeaderRouting(Operation<Void> operation) throws IOException {
        executeWithLeaderRouting(operation);
    }

    /**
     * Extracts leader info from NOT_LEADER exception.
     */
    @SuppressWarnings("unchecked")
    private LeaderInfo extractLeaderInfo(IOException e) {
        String message = e.getMessage();
        if (message == null) return null;

        // Check for NOT_LEADER indication
        if (!message.contains("NOT_LEADER") && !message.contains("not the leader")) {
            return null;
        }

        try {
            // Try to parse leader info from error message
            // Format: "CREATE_OBJECT failed: {leaderHost:..., leaderPort:...}"
            int jsonStart = message.indexOf('{');
            if (jsonStart >= 0) {
                String json = message.substring(jsonStart);
                Map<String, String> info = new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(json, Map.class);

                String host = info.get("leaderHost");
                String portStr = info.get("leaderPort");

                if (host != null && portStr != null) {
                    int port = Integer.parseInt(portStr);
                    return new LeaderInfo(host, port, info.get("leaderId"));
                }
            }
        } catch (Exception ex) {
            // Parsing failed, fall through
        }

        // Couldn't extract leader info, but it's still a NOT_LEADER error
        return LeaderInfo.UNKNOWN;
    }

    /**
     * Tries to connect to the leader.
     */
    private boolean tryConnectToLeader(LeaderInfo leaderInfo) {
        if (leaderInfo == LeaderInfo.UNKNOWN) {
            return false;
        }

        try {
            InetSocketAddress leaderAddr = new InetSocketAddress(leaderInfo.host, leaderInfo.port);
            connect(leaderAddr);
            cachedLeader = leaderAddr;
            System.out.println("[CLIENT] Connected to leader: " + leaderAddr);
            return true;
        } catch (IOException e) {
            System.out.println("[CLIENT] Failed to connect to leader " +
                    leaderInfo.host + ":" + leaderInfo.port + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Checks if the exception indicates a connection failure.
     */
    private boolean isConnectionFailure(IOException e) {
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        return message.contains("connection refused") ||
               message.contains("connection reset") ||
               message.contains("connection closed") ||
               message.contains("broken pipe") ||
               message.contains("timeout") ||
               message.contains("unreachable") ||
               message.contains("no route") ||
               message.contains("closed prematurely");
    }

    /**
     * Invalidates the cached leader (e.g., when leader fails).
     */
    public void invalidateLeaderCache() {
        cachedLeader = null;
    }

    /**
     * Returns the current server we're connected to.
     */
    public InetSocketAddress getCurrentServer() {
        return currentServer;
    }

    /**
     * Returns the cached leader address, if known.
     */
    public InetSocketAddress getCachedLeader() {
        return cachedLeader;
    }

    /**
     * Returns the current request ID for testing.
     */
    public String getCurrentRequestId() {
        return currentRequestId;
    }

    private void ensureNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Client is closed");
        }
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
        closed = true;
        closeCurrentClient();
    }

    @FunctionalInterface
    private interface Operation<T> {
        T execute() throws IOException;
    }

    /**
     * Leader information extracted from NOT_LEADER response.
     */
    private static class LeaderInfo {
        static final LeaderInfo UNKNOWN = new LeaderInfo(null, 0, null);

        final String host;
        final int port;
        final String id;

        LeaderInfo(String host, int port, String id) {
            this.host = host;
            this.port = port;
            this.id = id;
        }

        @Override
        public String toString() {
            if (this == UNKNOWN) return "UNKNOWN";
            return host + ":" + port + (id != null ? " (" + id + ")" : "");
        }
    }
}
