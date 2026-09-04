package com.storix.metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.Executors;

/**
 * TCP server that handles metadata requests.
 * Uses Java 21 virtual threads for lightweight concurrency.
 */
public class MetadataServer {

    private final int port;
    private final MetadataStore metadataStore;
    private volatile boolean running = true;

    public MetadataServer(int port, Path metadataFile) throws IOException {
        this.port = port;
        this.metadataStore = new MetadataStore(metadataFile);
    }

    /**
     * Starts the server and begins accepting connections.
     */
    public void start() throws IOException {
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {

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
        }
    }

    /**
     * Stops the server gracefully.
     */
    public void stop() {
        running = false;
    }

    /**
     * Handles a single client connection in a virtual thread.
     */
    private void handleClient(SocketChannel clientChannel) {
        MetadataHandler handler = new MetadataHandler(metadataStore);
        try (clientChannel) {
            handler.handle(clientChannel);
        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9090;
        Path metadataFile = Path.of(args.length > 1 ? args[1] : "metadata.json");

        MetadataServer server = new MetadataServer(port, metadataFile);

        // Graceful shutdown on SIGINT/SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            server.stop();
        }));

        server.start();
    }
}
