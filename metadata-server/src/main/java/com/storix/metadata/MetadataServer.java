package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.raft.*;
import com.storix.metadata.wal.WAL;
import com.storix.metadata.wal.SnapshotManager;
import com.storix.metadata.wal.GenerationManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * TCP server that handles metadata requests and manages cluster state.
 * Uses Java 21 virtual threads for lightweight concurrency.
 * Supports Raft consensus for high availability.
 */
public class MetadataServer {

    private final int port;
    private final int raftPort;
    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final RepairManager repairManager;
    private final HealthMonitor healthMonitor;
    private final RaftNode raftNode;
    private final MetadataStateMachine stateMachine;
    private final SnapshotManager snapshotManager;
    private final GenerationManager generationManager;
    private final ClusterConfig clusterConfig;
    private volatile boolean running = false;
    private volatile long runningThreadId = -1;
    private final Object lifecycleLock = new Object();
    private ServerSocketChannel serverChannel;
    private ExecutorService clientExecutor;
    private final Set<SocketChannel> activeClientChannels = ConcurrentHashMap.newKeySet();

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
        // In cluster mode, use a separate port for Raft RPC (port + 10000 to avoid conflicts)
        // The client-facing server uses the main port.
        this.raftPort = (clusterConfig != null) ? distinctPort(port) : port;
        this.nodeRegistry = new NodeRegistry();
        this.clusterConfig = clusterConfig;

        // Initialize Raft if cluster config provided
        if (clusterConfig != null) {
            // Use provided raftStateDir or default to "raft-state-{nodeId}"
            Path resolvedRaftStateDir = (raftStateDir != null) ? raftStateDir
                : Path.of("raft-state-" + clusterConfig.nodeId());

            // Create GenerationManager FIRST - it is the ONLY authoritative persistence path
            // Pass resolvedRaftStateDir (parent); GenerationManager internally creates generations/ subdirectory
            this.generationManager = new GenerationManager(resolvedRaftStateDir);

            // Validate CURRENT generation on startup - if generation directory is corrupt,
            // this is FATAL. GenerationManager is authoritative, so corrupt generation = no startup.
            long currentGen;
            try {
                currentGen = generationManager.getCurrentGeneration();
                if (currentGen >= 0) {
                    System.out.println("[SERVER] Found current generation: " + currentGen);
                    // Validate generation integrity - this will throw if corrupt
                    generationManager.loadAuthoritativeState();
                    System.out.println("[SERVER] Generation " + currentGen + " validated successfully");
                } else {
                    System.out.println("[SERVER] No current generation (fresh start)");
                }
            } catch (IOException e) {
                throw new RuntimeException(
                    "FATAL: Generation validation failed. Generation data is corrupt. " +
                    "This may indicate disk corruption or incomplete InstallSnapshot. " +
                    "Manual intervention required. Error: " + e.getMessage(), e);
            }

            // Initialize first generation on fresh start (if no current generation exists)
            if (currentGen < 0) {
                generationManager.initializeFirstGeneration();
            }

            // Create MetadataStore with GenerationManager integration
            // MetadataStore will load from GenerationManager's current generation
            this.metadataStore = new MetadataStore(metadataFile, generationManager);
            this.placementManager = new PlacementManager(nodeRegistry, replicationFactor);
            this.repairManager = new RepairManager(metadataStore, nodeRegistry, placementManager);
            this.healthMonitor = new HealthMonitor(nodeRegistry, repairManager,
                    nodeTimeoutMillis, healthCheckIntervalMillis);
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

            // Get snapshot boundary from GenerationManager (ONLY authoritative source)
            // GenerationManager is the SOLE authority for snapshot state and boundaries.
            // SnapshotManager is a LOCAL optimization only and must NOT determine authoritative index/term.
            GenerationManager.SnapshotBoundary boundary = generationManager.getSnapshotBoundary();
            long snapshotBoundaryIndex = (boundary != null) ? boundary.lastIncludedIndex() : 0L;
            long snapshotBoundaryTerm = (boundary != null) ? boundary.lastIncludedTerm() : 0L;

            if (boundary != null) {
                System.out.println("[SERVER] GenerationManager snapshot boundary: index=" + snapshotBoundaryIndex +
                        ", term=" + snapshotBoundaryTerm);
            } else {
                System.out.println("[SERVER] No generation snapshot boundary (fresh start or empty)");
            }

            // State is already loaded from GenerationManager via MetadataStore constructor.
            // We only need to:
            // 1. Set snapshot boundary in RaftLog
            // 2. Apply post-snapshot WAL entries to bring state up to date

            // Set snapshot boundary in RaftLog for log management
            raftLog.setSnapshotBoundary(snapshotBoundaryIndex, snapshotBoundaryTerm);

            // Filter WAL entries to only include post-snapshot entries
            List<LogEntry> postSnapshotEntries = recoveryData.entries.stream()
                    .filter(e -> e.index() > snapshotBoundaryIndex)
                    .toList();

            raftLog.loadEntries(
                postSnapshotEntries,
                recoveryData.commitIndex,
                Math.min(recoveryData.lastApplied, recoveryData.commitIndex)
            );

            System.out.println("[SERVER] Loaded from generation, applying " + postSnapshotEntries.size() + " post-snapshot WAL entries");

            // Apply post-snapshot entries to state machine to reconstruct
            // complete state.
            //
            // IMPORTANT recovery semantics:
            // - GenerationManager baseline is authoritative for the durable state
            //   materialized into MetadataStore.
            // - WAL replay must NOT double-apply entries that are already
            //   represented by that baseline.
            // - However, if a CREATE_OBJECT collides with an existing object and
            //   the metadata differs, that is an actual recovery conflict and
            //   must fail (no silent swallowing).

            ObjectMapper recoveryMapper = new ObjectMapper();
            int appliedCount = 0;
            for (LogEntry entry : postSnapshotEntries) {
                if (entry.index() > recoveryData.commitIndex) {
                    break;
                }

                if (entry.opType() == LogEntry.OpType.CREATE_OBJECT) {
                    ObjectMetadata incoming = recoveryMapper.readValue(entry.data(), ObjectMetadata.class);
                    String name = incoming.getObjectName();

                    if (metadataStore.objectExists(name)) {
                        ObjectMetadata existing = metadataStore.getObject(name)
                            .orElseThrow(() -> new IllegalStateException(
                                "Object exists but cannot be retrieved during recovery: " + name));

                        if (objectMetadataEquivalent(existing, incoming)) {
                            // Already represented in the durable baseline; treat as applied.
                            raftLog.advanceLastAppliedTo(entry.index());
                            continue;
                        }

                        throw new IllegalStateException(
                            "Recovery conflict: CREATE_OBJECT for existing object has different metadata. " +
                                "objectName=" + name + ", entryIndex=" + entry.index());
                    }
                }

                stateMachine.apply(entry);
                raftLog.advanceLastAppliedTo(entry.index());
                appliedCount++;
            }

            System.out.println("[SERVER] Applied " + appliedCount + " WAL entries during recovery");

            // Note: we do not attempt to update metadataStore directly here; it is
            // updated inside MetadataStateMachine.apply(entry) during successful replays.
            // Cases where a CREATE_OBJECT is already represented in the authoritative
            // baseline are treated as logically-applied, so we only advance RaftLog
            // lastApplied progress.

            // TODO: objectMetadataEquivalent assumes stable JSON round-tripping;
            // if that proves too strict, we can switch to field-by-field comparison
            // while still treating true collisions as conflicts.


            ClusterConfig raftConfig = normalizeClusterConfig(clusterConfig, port, raftPort);
            this.raftNode = new RaftNode(raftConfig, resolvedRaftStateDir, raftLog, wal,
                    recoveryData.term, recoveryData.votedFor);
            // Wire SnapshotManager into RaftNode for log compaction
            this.raftNode.setSnapshotManager(snapshotManager);
            // Wire GenerationManager into RaftNode for immutable generation management
            this.raftNode.setGenerationManager(generationManager);
            // Wire MetadataStore for InstallSnapshot state restoration
            this.raftNode.setMetadataStore(metadataStore);
        } else {
            // Non-cluster mode: create MetadataStore without GenerationManager
            this.metadataStore = new MetadataStore(metadataFile);
            this.placementManager = new PlacementManager(nodeRegistry, replicationFactor);
            this.repairManager = new RepairManager(metadataStore, nodeRegistry, placementManager);
            this.healthMonitor = new HealthMonitor(nodeRegistry, repairManager, nodeTimeoutMillis, healthCheckIntervalMillis);
            this.snapshotManager = null;
            this.generationManager = null;
            this.stateMachine = null;
            this.raftNode = null;
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
     * Returns the generation manager for immutable generation management.
     * May be null if not in cluster mode.
     */
    public GenerationManager getGenerationManager() {
        return generationManager;
    }

    /**
     * Starts the server and begins accepting connections.
     */
    public void start() throws IOException {
        synchronized (lifecycleLock) {
            if (running) {
                return;
            }
            running = true;
        }

        try {
        // Start Raft if configured
        if (raftNode != null) {
            // Set up state machine applier
            raftNode.setLogEntryApplier(entry -> {
                if (stateMachine != null) {
                    try {
                        stateMachine.apply(entry);
                    } catch (Exception e) {
                        // Log full stack trace safely
                        System.err.println("[RAFT] Failed to apply entry " + entry.index()
                            + ", term=" + entry.term()
                            + ", opType=" + entry.opType()
                            + ", exceptionClass=" + e.getClass().getName()
                            + ", message=" + e.getMessage()
                            + ", cause=" + (e.getCause() != null ? e.getCause().toString() : "none"));
                        e.printStackTrace();
                        // Re-throw as RuntimeException to propagate failure to InstallSnapshot handler
                        // This is critical: snapshot restore failures must cause success=false
                        throw new RuntimeException("State machine apply failed: " + e.getMessage(), e);
                    }
                }
            });

            raftNode.start();
            raftNode.addLeadershipListener(() -> {
                System.out.println("[SERVER] Leadership changed, isLeader=" + isLeader());
            });
        }

        healthMonitor.start();

        // Always start the client-facing server to handle metadata requests.
        // In single-node mode, this is the only server needed.
        // In multi-node mode, client and Raft ports are independent.
        ServerSocketChannel sc = ServerSocketChannel.open();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        synchronized (lifecycleLock) {
            this.serverChannel = sc;
            this.clientExecutor = executor;
        }
        try {
            serverChannel.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, true);
            serverChannel.bind(new InetSocketAddress(port));
            System.out.println("Metadata server listening on port " + port);
            runningThreadId = Thread.currentThread().getId();

            while (running) {
                try {
                    SocketChannel clientChannel = serverChannel.accept();
                    if (clientChannel == null) {
                        continue;
                    }
                    activeClientChannels.add(clientChannel);
                    try {
                        executor.submit(() -> handleClient(clientChannel));
                    } catch (RejectedExecutionException e) {
                        activeClientChannels.remove(clientChannel);
                        clientChannel.close();
                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Accept failed: " + e.getMessage());
                    }
                }
            }
        } finally {
            stop();
        }
        } catch (IOException | RuntimeException e) {
            stop();
            throw e;
        }
    }

    /**
     * Stops the server gracefully and waits for all owned resources to terminate.
     */
    public void stop() {
        synchronized (lifecycleLock) {
            running = false;
            closeClientResources();
            healthMonitor.stop();
            if (raftNode != null) {
                raftNode.stop();
            }
            awaitClientExecutor();
        }
    }

    private void closeClientResources() {
        ServerSocketChannel channel = serverChannel;
        serverChannel = null;
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException ignored) {}
        }
        for (SocketChannel client : activeClientChannels) {
            try {
                client.close();
            } catch (IOException ignored) {}
        }
        activeClientChannels.clear();
    }

    private void awaitClientExecutor() {
        ExecutorService executor = clientExecutor;
        clientExecutor = null;
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        boolean interrupted = false;
        try {
            while (!executor.isTerminated()) {
                try {
                    if (executor.awaitTermination(2, TimeUnit.SECONDS)) {
                        break;
                    }
                    executor.shutdownNow();
                } catch (InterruptedException e) {
                    interrupted = true;
                    executor.shutdownNow();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Handles a single client connection in a virtual thread.
     */
    private void handleClient(SocketChannel clientChannel) {
        MetadataHandler handler = new MetadataHandler(metadataStore, nodeRegistry,
                placementManager, repairManager, raftNode, stateMachine, port);
        try (clientChannel) {
            handler.handle(clientChannel);
        } catch (IOException e) {
            if (running) {
                System.err.println("Client handler error: " + e.getMessage());
            }
        } finally {
            activeClientChannels.remove(clientChannel);
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 9090;
        Path metadataFile = Path.of("metadata.json");
        int replicationFactor = 2;
        long timeout = 6000;
        long interval = 2000;
        ClusterConfig clusterConfig = null;
        Path raftStateDir = null;

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
            } else if ("--raft-state-dir".equals(args[i]) && i + 1 < args.length) {
                raftStateDir = Path.of(args[++i]);
            } else if (!args[i].startsWith("--")) {
                if (i == 0) port = Integer.parseInt(args[0]);
                else if (i == 1) metadataFile = Path.of(args[1]);
            }
        }

        MetadataServer server = new MetadataServer(port, metadataFile, replicationFactor, timeout, interval, clusterConfig, raftStateDir);

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

        return new ClusterConfig("storix", nodeId, host, port, port + 10000, peers);
    }

    private static int distinctPort(int clientPort) {
        long higher = (long) clientPort + 10000L;
        if (higher <= 65535L) {
            return (int) higher;
        }
        long lower = (long) clientPort - 10000L;
        if (lower > 0) {
            return (int) lower;
        }
        return clientPort == 65535 ? 65534 : 65535;
    }

    /**
     * Keeps the client and Raft listeners distinct for legacy configurations
     * that used one port for both services. Explicit client/Raft assignments
     * are preserved unchanged.
     */
    private static ClusterConfig normalizeClusterConfig(
            ClusterConfig config, int clientPort, int defaultRaftPort) {
        // If config is null or ports are already distinct, return unchanged
        if (config == null || config.raftPort() != clientPort) {
            return config;
        }

        List<RaftPeer> peers = config.initialPeers();
        List<RaftPeer> normalizedPeers = peers == null
                ? null
                : peers.stream()
                        .map(peer -> new RaftPeer(
                                peer.nodeId(), peer.host(), distinctPort(peer.port())))
                        .toList();

        return new ClusterConfig(
                config.clusterId(),
                config.nodeId(),
                config.host(),
                clientPort,
                defaultRaftPort,
                normalizedPeers);
    }

    /**
     * Extracts the object name from a CREATE_OBJECT log entry.
     */
    private static String extractObjectName(LogEntry entry) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectMetadata metadata = mapper.readValue(entry.data(), ObjectMetadata.class);
            return metadata.getObjectName();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Strict equivalence check between two ObjectMetadata instances.
     *
     * Used only during startup recovery to decide whether a post-snapshot
     * CREATE_OBJECT entry is already represented by the authoritative
     * GenerationManager baseline.
     */
    private static boolean objectMetadataEquivalent(ObjectMetadata a, ObjectMetadata b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }

        if (!Objects.equals(a.getObjectName(), b.getObjectName())) {
            return false;
        }
        if (a.getFileSize() != b.getFileSize()) {
            return false;
        }
        if (a.getChunkSize() != b.getChunkSize()) {
            return false;
        }

        List<ChunkInfo> aChunks = a.getChunks();
        List<ChunkInfo> bChunks = b.getChunks();
        if (aChunks == null && bChunks == null) {
            return true;
        }
        if (aChunks == null || bChunks == null) {
            return false;
        }
        if (aChunks.size() != bChunks.size()) {
            return false;
        }

        // Compare chunks deterministically by chunkId + chunkIndex.
        List<ChunkInfo> aSorted = new ArrayList<>(aChunks);
        List<ChunkInfo> bSorted = new ArrayList<>(bChunks);

        aSorted.sort((x, y) -> {
            int c = Objects.toString(x.getChunkId(), "").compareTo(Objects.toString(y.getChunkId(), ""));
            if (c != 0) {
                return c;
            }
            return Integer.compare(x.getChunkIndex(), y.getChunkIndex());
        });
        bSorted.sort((x, y) -> {
            int c = Objects.toString(x.getChunkId(), "").compareTo(Objects.toString(y.getChunkId(), ""));
            if (c != 0) {
                return c;
            }
            return Integer.compare(x.getChunkIndex(), y.getChunkIndex());
        });

        for (int i = 0; i < aSorted.size(); i++) {
            ChunkInfo ax = aSorted.get(i);
            ChunkInfo by = bSorted.get(i);

            if (!Objects.equals(ax.getChunkId(), by.getChunkId())) {
                return false;
            }
            if (ax.getChunkIndex() != by.getChunkIndex()) {
                return false;
            }
            if (ax.getChunkSize() != by.getChunkSize()) {
                return false;
            }
            if (!Objects.equals(ax.getChecksum(), by.getChecksum())) {
                return false;
            }

            List<String> aRep = ax.getReplicaNodeIds() == null
                ? List.of()
                : new ArrayList<>(ax.getReplicaNodeIds());
            List<String> bRep = by.getReplicaNodeIds() == null
                ? List.of()
                : new ArrayList<>(by.getReplicaNodeIds());

            aRep.sort(Comparator.naturalOrder());
            bRep.sort(Comparator.naturalOrder());

            if (!aRep.equals(bRep)) {
                return false;
            }
        }

        return true;
    }
}
