package com.storix.client;

import java.nio.file.Path;
import java.util.List;

/**
 * Command-line interface for Storix.
 *
 * Commands:
 *   storix put <file>
 *   storix get <object> <output>
 *   storix delete <object>
 *   storix info <object>
 *   storix list
 */
public class StorixCLI {

    private static final String DEFAULT_METADATA_HOST = "localhost";
    private static final int DEFAULT_METADATA_PORT = 9090;
    private static final String DEFAULT_STORAGE_HOST = "localhost";
    private static final int DEFAULT_STORAGE_PORT = 8080;
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

            System.out.println("Object: " + metadata.getObjectName());
            System.out.println("Size: " + metadata.getFileSize() + " bytes");
            System.out.println("Chunk size: " + metadata.getChunkSize() + " bytes");
            System.out.println("Chunks: " + metadata.getChunkCount());
            System.out.println();

            for (ChunkInfoDTO chunk : metadata.getChunks()) {
                System.out.println("  " + chunk.getChunkId() + " → " + chunk.getStorageNodeId());
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

    private static StorixClient createClient(int chunkSize) {
        return new StorixClient(
            DEFAULT_METADATA_HOST,
            DEFAULT_METADATA_PORT,
            DEFAULT_STORAGE_HOST,
            DEFAULT_STORAGE_PORT,
            chunkSize
        );
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
        System.out.println();
        System.out.println("Chunk size examples: 1M, 512K, 1048576");
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  Metadata Server: " + DEFAULT_METADATA_HOST + ":" + DEFAULT_METADATA_PORT);
        System.out.println("  Storage Node: " + DEFAULT_STORAGE_HOST + ":" + DEFAULT_STORAGE_PORT);
    }
}
