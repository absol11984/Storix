package com.storix.storage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * TCP server that accepts chunk storage requests.
 * Uses Java 21 virtual threads for lightweight concurrency.
 */
public class ChunkServer {

    private final String nodeId;
    private final String host;
    private final int port;
    private final ChunkStorage storage;
    private final String metadataHost;
    private final int metadataPort;
    private final long heartbeatIntervalMillis;
    private volatile boolean running = true;
    private ScheduledExecutorService heartbeatScheduler;
    private ServerSocketChannel serverChannel;

    private static final long DEFAULT_TOTAL_CAPACITY_BYTES = -1; // unlimited
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 2000;

    public ChunkServer(String nodeId, String host, int port, Path storageDir,
                       String metadataHost, int metadataPort) throws IOException {
        this(nodeId, host, port, storageDir, metadataHost, metadataPort,
                DEFAULT_TOTAL_CAPACITY_BYTES, DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
    }

    /**
     * Creates a ChunkServer with an explicit heartbeat interval.
     *
     * Note: This constructor uses the default (unlimited) total capacity and treats the last
     * {@code long} parameter as {@code heartbeatIntervalMillis}, matching the expectations of
     * existing tests.
     */
    public ChunkServer(String nodeId, String host, int port, Path storageDir,
                       String metadataHost, int metadataPort, long heartbeatIntervalMillis) throws IOException {
        this(nodeId, host, port, storageDir, metadataHost, metadataPort,
                DEFAULT_TOTAL_CAPACITY_BYTES, heartbeatIntervalMillis);
    }

    public ChunkServer(String nodeId, String host, int port, Path storageDir,
                       String metadataHost, int metadataPort, long totalCapacityBytes,
                       long heartbeatIntervalMillis) throws IOException {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.storage = new ChunkStorage(storageDir, totalCapacityBytes);
        this.metadataHost = metadataHost;
        this.metadataPort = metadataPort;

        // Treat non-positive intervals as invalid and fall back to default.
        this.heartbeatIntervalMillis = heartbeatIntervalMillis > 0
                ? heartbeatIntervalMillis : DEFAULT_HEARTBEAT_INTERVAL_MILLIS;
    }

    /**
     * Returns the configured total capacity in bytes (<=0 means unlimited).
     */
    public long getTotalCapacityBytes() {
        return storage.getTotalCapacityBytes();
    }

    /**
     * Returns the current used capacity in bytes.
     */
    public long getUsedCapacityBytes() {
        return storage.getUsedCapacityBytes();
    }

    /**
     * Returns the configured heartbeat interval in milliseconds.
     */
    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    /**
     * Returns the current available capacity in bytes.
     */
    public long getAvailableCapacityBytes() {
        return storage.getAvailableCapacityBytes();
    }

    /**
     * Returns the ChunkStorage used by this server for test access.
     */
    public ChunkStorage getStorage() {
        return storage;
    }

    /**
     * Starts the server and begins accepting connections.
     */
    public void start() throws IOException {
        registerWithMetadataServer();
        startHeartbeat();

        try (ServerSocketChannel sc = ServerSocketChannel.open();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            this.serverChannel = sc;
            serverChannel.bind(new InetSocketAddress(port));
            System.out.println("Storage node " + nodeId + " listening on " + host + ":" + port);

            while (running) {
                try {
                    SocketChannel clientChannel = serverChannel.accept();
                    executor.submit(() -> handleClient(clientChannel));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Accept failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Stops the server gracefully.
     */
    public void stop() {
        running = false;
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
        }
        if (serverChannel != null && serverChannel.isOpen()) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {}
        }
    }

    private void handleClient(SocketChannel clientChannel) {
        ChunkHandler handler = new ChunkHandler(storage);
        try (clientChannel) {
            handler.handle(clientChannel);
        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        }
    }

    private void registerWithMetadataServer() {
        try {
            // JSON: {nodeId, host, port, totalCapacityBytes, usedCapacityBytes}
            String json = String.format(
                    "{\"nodeId\":\"%s\",\"host\":\"%s\",\"port\":%d,\"totalCapacityBytes\":%d,\"usedCapacityBytes\":%d}",
                    nodeId, host, port,
                    storage.getTotalCapacityBytes(),
                    storage.getUsedCapacityBytes());

            sendToMetadataServer((byte) 10, json); // REGISTER_NODE = 10
            System.out.println("Registered with metadata server at " + metadataHost + ":" + metadataPort);
        } catch (IOException e) {
            System.err.println("Failed to register with metadata server: " + e.getMessage());
        }
    }

    private void startHeartbeat() {
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-thread");
            t.setDaemon(true);
            return t;
        });

        heartbeatScheduler.scheduleAtFixedRate(() -> {
            // Rebuild heartbeat payload each tick from live capacity so metadata
            // server sees the current used/total ratio (updated by rebalance moves).
            String json = String.format(
                    "{\"nodeId\":\"%s\",\"totalCapacityBytes\":%d,\"usedCapacityBytes\":%d}",
                    nodeId,
                    storage.getTotalCapacityBytes(),
                    storage.getUsedCapacityBytes());
            try {
                sendToMetadataServer((byte) 11, json); // HEARTBEAT = 11
            } catch (IOException e) {
                System.err.println("Heartbeat failed: " + e.getMessage());
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void sendToMetadataServer(byte opcode, String jsonPayload) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(metadataHost, metadataPort))) {
            byte[] payloadBytes = jsonPayload.getBytes();
            int requestSize = 1 + 4 + payloadBytes.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put(opcode);
            request.putInt(payloadBytes.length);
            request.put(payloadBytes);
            request.flip();

            while (request.hasRemaining()) {
                channel.write(request);
            }

            // Read response
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            int read = 0;
            while (lengthBuf.hasRemaining()) {
                int r = channel.read(lengthBuf);
                if (r == -1) throw new IOException("Connection closed prematurely");
                read += r;
            }
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            while (responseBuf.hasRemaining()) {
                if (channel.read(responseBuf) == -1) break;
            }
            responseBuf.flip();

            byte status = responseBuf.get(); // 0 = OK
            if (status != 0) {
                int dataLen = responseBuf.getInt();
                byte[] errorData = new byte[dataLen];
                responseBuf.get(errorData);
                throw new IOException("Server returned error: " + new String(errorData));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String nodeId = "node-1";
        String host = "127.0.0.1";
        int port = 8080;
        Path storageDir = Path.of("chunks");
        String metadataHost = "127.0.0.1";
        int metadataPort = 9090;
        long totalCapacityBytes = DEFAULT_TOTAL_CAPACITY_BYTES;
        long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            if ("--id".equals(args[i]) && i + 1 < args.length) {
                nodeId = args[++i];
            } else if ("--host".equals(args[i]) && i + 1 < args.length) {
                host = args[++i];
            } else if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if ("--storage".equals(args[i]) && i + 1 < args.length) {
                storageDir = Path.of(args[++i]);
            } else if ("--metadata".equals(args[i]) && i + 1 < args.length) {
                String val = args[++i];
                String[] parts = val.split(":");
                metadataHost = parts[0];
                if (parts.length > 1) {
                    metadataPort = Integer.parseInt(parts[1]);
                }
            } else if ("--heartbeat-interval".equals(args[i]) && i + 1 < args.length) {
                heartbeatInterval = Long.parseLong(args[++i]);
            } else if ("--capacity".equals(args[i]) && i + 1 < args.length) {
                totalCapacityBytes = Long.parseLong(args[++i]);
            } else if (!args[i].startsWith("--")) {
                // backward compatibility positional arguments
                if (i == 0) port = Integer.parseInt(args[0]);
                else if (i == 1) storageDir = Path.of(args[1]);
            }
        }

        ChunkServer server = new ChunkServer(nodeId, host, port, storageDir,
                metadataHost, metadataPort, totalCapacityBytes, heartbeatInterval);

        // Graceful shutdown on SIGINT/SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down storage node...");
            server.stop();
        }));

        server.start();
    }
}
