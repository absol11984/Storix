package com.storix.metadata;

import com.storix.metadata.raft.*;
import com.storix.metadata.wal.WAL;
import com.storix.metadata.wal.SnapshotManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * TCP server that handles metadata requests and manages cluster state.
 * Uses Java 21 virtual threads for lightweight concurrency.
 * Supports Raft consensus for high availability.
 */
public class MetadataServer {

    private final int port;
    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final RepairManager repairManager;
    private final HealthMonitor healthMonitor;
    private final RaftNode raftNode;
    private final MetadataStateMachine stateMachine;
    private final SnapshotManager snapshotManager;
    private volatile boolean running = true;
    private ServerSocketChannel serverChannel;

    /**
     * Creates a single-node metadata server (no Raft).
     */
    public MetadataServer(int port, Path metadataFile) throws IOException {
        this(port, metadataFile, 2, 6000, 2000, null, null);
    }

    /**
     * Creates a metadata server with optional Raft configuration.
     */
    public MetadataServer(int port, Path metadataFile, int replicationFactor,
                          long nodeTimeoutMillis, long healthCheckIntervalMillis,
                          ClusterConfig clusterConfig) throws IOException {
        this(port, metadataFile, replicationFactor, nodeTimeoutMillis, healthCheckIntervalMillis,
             clusterConfig, null);
    }

    /**
     * Creates a metadata server with optional Raft configuration and custom raft state directory.
     * @param raftStateDir Custom directory for raft state (WAL, snapshots). If null, uses default.
     */
    public MetadataServer(int port, Path metadataFile, int replicationFactor,
                          long nodeTimeoutMillis, long healthCheckIntervalMillis,
                          ClusterConfig clusterConfig, Path raftStateDir) throws IOException {
        this.port = port;
        this.metadataStore = new MetadataStore(metadataFile);
        this.nodeRegistry = new NodeRegistry();
        this.placementManager = new PlacementManager(nodeRegistry, replicationFactor);
        this.repairManager = new RepairManager(metadataStore, nodeRegistry, placementManager);
        this.healthMonitor = new HealthMonitor(nodeRegistry, repairManager,
                nodeTimeoutMillis, healthCheckIntervalMillis);

        // Initialize Raft if cluster config provided
        if (clusterConfig != null) {
            // Use provided raftStateDir or default to "raft-state-{nodeId}"
            Path resolvedRaftStateDir = (raftStateDir != null) ? raftStateDir
                : Path.of("raft-state-" + clusterConfig.nodeId());
            this.stateMachine = new MetadataStateMachine(metadataStore);

            // Create SnapshotManager for log compaction
            Path snapshotDir = resolvedRaftStateDir.resolve("snapshots");
            this.snapshotManager = new SnapshotManager(snapshotDir, metadataStore);

            // Create WAL for durability
            Path walFile = resolvedRaftStateDir.resolve("wal.dat");
            WAL wal = new WAL(walFile);
            RaftLog raftLog = new RaftLog(wal);

            // Recover from WAL: entries, term, votedFor, commitIndex, lastApplied
            // Use loadEntries() to restore without re-writing to WAL
            WAL.WALRecoveryResult recoveryData = wal.recover();

            // Try to restore from snapshot if exists (restores full state before log replay)
            try {
                Optional<SnapshotManager.Snapshot> latestSnapshot = snapshotManager.loadLatestSnapshot();
                if (latestSnapshot.isPresent()) {
                    SnapshotManager.Snapshot snap = latestSnapshot.get();
                    System.out.println("[SERVER] Found snapshot at index " + snap.lastIncludedIndex() +
                            ", term " + snap.lastIncludedTerm());

                    // Clear metadataStore first - we're going to restore from snapshot
                    // which provides the authoritative state. If metadata.json has any data,
                    // it would conflict with snapshot restoration.
                    for (String name : new ArrayList<>(metadataStore.listObjects())) {
                        metadataStore.deleteObjectDirect(name);
                    }
                    metadataStore.save();

                    // Restore state from snapshot
                    snapshotManager.restoreFromSnapshot(snap, metadataStore);

                    // Set snapshot boundary in RaftLog BEFORE loading entries
                    // This ensures entries <= snapshotIndex are treated as covered by snapshot
                    raftLog.setSnapshotBoundary(snap.lastIncludedIndex(), snap.lastIncludedTerm());

                    // Filter WAL entries to only include post-snapshot entries
                    // Entries <= lastIncludedIndex are already in the snapshot
                    List<LogEntry> postSnapshotEntries = recoveryData.entries.stream()
                            .filter(e -> e.index() > snap.lastIncludedIndex())
                            .toList();

                    System.out.println("[SERVER] Loading " + postSnapshotEntries.size() + " post-snapshot entries");

                    raftLog.loadEntries(
                        postSnapshotEntries,
                        recoveryData.commitIndex,
                        Math.min(recoveryData.lastApplied, recoveryData.commitIndex)
                    );

                    // Apply post-snapshot entries to state machine to reconstruct complete state
                    for (LogEntry entry : postSnapshotEntries) {
                        if (entry.index() <= recoveryData.commitIndex) {
                            stateMachine.apply(entry);
                        }
                    }
                    System.out.println("[SERVER] Applied " + postSnapshotEntries.size() + " entries to state machine");
                } else {
                    // No snapshot - clear any stale metadata from disk and rebuild from WAL
                    // metadataStore was loaded from metadata.json but may not match WAL state
                    if (recoveryData.commitIndex > 0 || !recoveryData.entries.isEmpty()) {
                        for (String name : new ArrayList<>(metadataStore.listObjects())) {
                            metadataStore.deleteObjectDirect(name);
                        }
                        metadataStore.save();
                    }

                    raftLog.loadEntries(
                        recoveryData.entries,
                        recoveryData.commitIndex,
                        recoveryData.lastApplied
                    );
                    // Apply all entries to state machine
                    for (LogEntry entry : recoveryData.entries) {
                        if (entry.index() <= recoveryData.commitIndex) {
                            stateMachine.apply(entry);
                        }
                    }
                    System.out.println("[SERVER] Applied " + recoveryData.entries.size() + " entries to state machine (no snapshot)");
                }
            } catch (Exception e) {
                System.err.println("[SERVER] Failed to load snapshot: " + e.getMessage());
                // Snapshot restoration failed. Try to recover from WAL if entries exist.
                // DON'T clear metadataStore - it was loaded from metadata.json and may have valid data.
                // If WAL entries exist and were committed, replay them to update metadataStore.
                if (recoveryData.commitIndex > 0 || !recoveryData.entries.isEmpty()) {
                    // Clear only if we need to rebuild from WAL
                    if (!recoveryData.entries.isEmpty()) {
                        for (String name : new ArrayList<>(metadataStore.listObjects())) {
                            metadataStore.deleteObjectDirect(name);
                        }
                    }
                    raftLog.loadEntries(
                        recoveryData.entries,
                        recoveryData.commitIndex,
                        recoveryData.lastApplied
                    );
                    // Apply committed entries to update state machine
                    for (LogEntry entry : recoveryData.entries) {
                        if (entry.index() <= recoveryData.commitIndex) {
                            stateMachine.apply(entry);
                        }
                    }
                }
                // metadata.json fallback: if metadataStore still has data, keep it
                // This provides a last-resort fallback when snapshot and WAL are both unreliable
                metadataStore.save();
            }

            this.raftNode = new RaftNode(clusterConfig, resolvedRaftStateDir, raftLog, wal,
                    recoveryData.term, recoveryData.votedFor);
            // Wire SnapshotManager into RaftNode for log compaction
            this.raftNode.setSnapshotManager(snapshotManager);
        } else {
            this.raftNode = null;
            this.stateMachine = null;
            this.snapshotManager = null;
        }
    }

    /**
     * Backward-compatible constructor (no Raft).
     */
    public MetadataServer(int port, Path metadataFile, int replicationFactor,
                          long nodeTimeoutMillis, long healthCheckIntervalMillis) throws IOException {
        this(port, metadataFile, replicationFactor, nodeTimeoutMillis, healthCheckIntervalMillis, null);
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
     * Returns true if this node is the Raft leader.
     */
    public boolean isLeader() {
        return raftNode != null && raftNode.isLeader();
    }

    /**
     * Returns the current leader's address, if known.
     */
    public Optional<RaftPeer> getLeader() {
        if (raftNode == null) {
            return Optional.of(new RaftPeer("local", "127.0.0.1", port));
        }
        return raftNode.getLeader();
    }

    /**
     * Returns the RaftNode, or null if not in cluster mode.
     */
    public RaftNode getRaftNode() {
        return raftNode;
    }

    /**
     * Returns the state machine for applying log entries.
     */
    public MetadataStateMachine getStateMachine() {
        return stateMachine;
    }

    /**
     * Starts the server and begins accepting connections.
     */
    public void start() throws IOException {
        // Start Raft if configured
        if (raftNode != null) {
            // Set up state machine applier
            raftNode.setLogEntryApplier(entry -> {
                if (stateMachine != null) {
                    try {
                        stateMachine.apply(entry);
                    } catch (IOException e) {
                        System.err.println("[SERVER] Failed to apply entry: " + e.getMessage());
                    }
                }
            });

            raftNode.start();
            raftNode.addLeadershipListener(() -> {
                System.out.println("[SERVER] Leadership changed, isLeader=" + isLeader());
            });
        }

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
            if (raftNode != null) {
                raftNode.stop();
            }
        }
    }

    /**
     * Stops the server gracefully.
     */
    public void stop() {
        running = false;
        healthMonitor.stop();
        if (raftNode != null) {
            raftNode.stop();
        }
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
                placementManager, repairManager, raftNode, stateMachine);
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
        ClusterConfig clusterConfig = null;

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
            } else if ("--interval".equals(args[i]) && i + 1 < args.length) {
                interval = Long.parseLong(args[++i]);
            } else if ("--cluster".equals(args[i]) && i + 1 < args.length) {
                // Parse cluster config: nodeId:host:port,peer1:host:port,peer2:host:port
                clusterConfig = parseClusterConfig(args[++i]);
            } else if (!args[i].startsWith("--")) {
                if (i == 0) port = Integer.parseInt(args[0]);
                else if (i == 1) metadataFile = Path.of(args[1]);
            }
        }

        MetadataServer server = new MetadataServer(port, metadataFile, replicationFactor, timeout, interval, clusterConfig);

        // Graceful shutdown on SIGINT/SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down metadata server...");
            server.stop();
        }));

        server.start();
    }

    private static ClusterConfig parseClusterConfig(String config) {
        // Format: nodeId:host:port,peer1:host:port,peer2:host:port
        String[] parts = config.split(",");
        if (parts.length == 0) {
            return null;
        }

        // First part is self
        String[] selfParts = parts[0].split(":");
        String nodeId = selfParts[0];
        String host = selfParts.length > 1 ? selfParts[1] : "127.0.0.1";
        int port = selfParts.length > 2 ? Integer.parseInt(selfParts[2]) : 9090;

        // Remaining parts are peers
        List<RaftPeer> peers = java.util.stream.IntStream.range(1, parts.length)
                .mapToObj(i -> {
                    String[] peerParts = parts[i].split(":");
                    String peerId = peerParts[0];
                    String peerHost = peerParts.length > 1 ? peerParts[1] : "127.0.0.1";
                    int peerPort = peerParts.length > 2 ? Integer.parseInt(peerParts[2]) : 9090;
                    return new RaftPeer(peerId, peerHost, peerPort);
                })
                .toList();

        return new ClusterConfig("storix", nodeId, host, port, peers);
    }
}
