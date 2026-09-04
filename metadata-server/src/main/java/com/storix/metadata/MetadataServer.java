package com.storix.metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.Executors;

/**
 * TCP server that handles metadata requests and manages cluster state.
 * Uses Java 21 virtual threads for lightweight concurrency.
 */
public class MetadataServer {

    private final int port;
    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final RepairManager repairManager;
    private final HealthMonitor healthMonitor;
    private volatile boolean running = true;
    private ServerSocketChannel serverChannel;

    public MetadataServer(int port, Path metadataFile) throws IOException {
        this(port, metadataFile, 2, 6000, 2000);
    }

    public MetadataServer(int port, Path metadataFile, int replicationFactor,
                          long nodeTimeoutMillis, long healthCheckIntervalMillis) throws IOException {
        this.port = port;
        this.metadataStore = new MetadataStore(metadataFile);
        this.nodeRegistry = new NodeRegistry();
        this.placementManager = new PlacementManager(nodeRegistry, replicationFactor);
        this.repairManager = new RepairManager(metadataStore, nodeRegistry, placementManager);
        this.healthMonitor = new HealthMonitor(nodeRegistry, repairManager,
                nodeTimeoutMillis, healthCheckIntervalMillis);
    }

    public NodeRegistry getNodeRegistry() {
        return nodeRegistry;
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public PlacementManager getPlacementManager() {
        return placementManager;
    }

    public RepairManager getRepairManager() {
        return repairManager;
    }

    /**
     * Starts the server and begins accepting connections.
     */
    public void start() throws IOException {
        healthMonitor.start();

        try (ServerSocketChannel sc = ServerSocketChannel.open();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            this.serverChannel = sc;
            serverChannel.bind(new InetSocketAddress(port));
            System.out.println("Metadata server listening on port " + port);

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
        } finally {
            healthMonitor.stop();
        }
    }

    /**
     * Stops the server gracefully.
     */
    public void stop() {
        running = false;
        healthMonitor.stop();
        if (serverChannel != null && serverChannel.isOpen()) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handles a single client connection in a virtual thread.
     */
    private void handleClient(SocketChannel clientChannel) {
        MetadataHandler handler = new MetadataHandler(metadataStore, nodeRegistry,
                placementManager, repairManager);
        try (clientChannel) {
            handler.handle(clientChannel);
        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 9090;
        Path metadataFile = Path.of("metadata.json");
        int replicationFactor = 2;
        long timeout = 6000;
        long interval = 2000;

        // Parse arguments supporting both flags and positional
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if ("--metadata".equals(args[i]) && i + 1 < args.length) {
                metadataFile = Path.of(args[++i]);
            } else if ("--replication-factor".equals(args[i]) && i + 1 < args.length) {
                replicationFactor = Integer.parseInt(args[++i]);
            } else if ("--timeout".equals(args[i]) && i + 1 < args.length) {
                timeout = Long.parseLong(args[++i]);
            } else if (!args[i].startsWith("--")) {
                if (i == 0) port = Integer.parseInt(args[0]);
                else if (i == 1) metadataFile = Path.of(args[1]);
            }
        }

        MetadataServer server = new MetadataServer(port, metadataFile, replicationFactor, timeout, interval);

        // Graceful shutdown on SIGINT/SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down metadata server...");
            server.stop();
        }));

        server.start();
    }
}
