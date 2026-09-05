package com.storix.metadata.raft;

import com.storix.metadata.wal.SnapshotManager;
import com.storix.metadata.wal.WAL;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.io.UncheckedIOException;
import java.util.zip.CRC32;

/**
 * Core Raft consensus implementation.
 * Handles leader election, log replication, and term management.
 */
public class RaftNode implements AutoCloseable {

    // Configuration
    private static final long ELECTION_TIMEOUT_MIN_MS = 150;
    private static final long ELECTION_TIMEOUT_MAX_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 50;

    // Persistent state files
    private static final String STATE_FILE = "raft-state.dat";

    // Fields
    private final String nodeId;
    private final String host;
    private final int port;
    private final List<RaftPeer> peers;
    private final boolean singleNode;

    // Persistent state
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile long persistedTerm = -1; // Track what term has been persisted to WAL
    private final RaftLog raftLog;
    private final Path stateDir;
    private WAL wal; // For persisting term/votedFor changes

    // Volatile state
    private volatile RaftState state = RaftState.FOLLOWER;
    private volatile String leaderId = null;

    // Leader-specific state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Threading
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running = false;

    // Election
    private final Random random = new Random();
    private volatile long lastHeartbeat = 0;
    private volatile long electionDeadline = 0; // When current election timeout expires
    private final ReentrantLock electionLock = new ReentrantLock();

    // Vote tracking - thread-safe set of voters who granted us their vote this term
    private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();

    // Log index counter
    private AtomicLong logIndexCounter = new AtomicLong(1);

    // RPC port for peer communication
    private ServerSocketChannel rpcServer;

    // Callbacks
    private final List<Runnable> leadershipListeners = new CopyOnWriteArrayList<>();
    private volatile Consumer<LogEntry> stateMachineApplier;

    // Pending futures for commit acknowledgements
    private final ConcurrentHashMap<Long, CompletableFuture<Boolean>> pendingCommits = new ConcurrentHashMap<>();

    // Snapshot manager for log compaction
    private volatile SnapshotManager snapshotManager;

    // Direct reference to MetadataStore for isolated candidate restoration
    private volatile com.storix.metadata.MetadataStore metadataStore;

    // Snapshot installation state machine for crash-safe protocol
    private enum InstallationState {
        NONE,           // No installation in progress
        RECEIVING,      // Receiving chunks
        VALIDATED,      // Snapshot validated (checksum passed)
        CANDIDATE,      // Snapshot persisted as candidate
        RESTORED,       // State machine restored from candidate
        COMMITTED       // Installation fully complete
    }

    private volatile InstallationState installationState = InstallationState.NONE;

    // Previous snapshot state for rollback on failure
    private volatile long previousSnapshotIndex = 0;
    private volatile long previousSnapshotIndexTerm = 0;

    // Candidate snapshot tracking (for cleanup and rollback)
    private volatile Path candidateSnapshotFile = null;

    // Candidate state for isolated restoration (prevents partial live state modification)
    private volatile Map<String, com.storix.metadata.ObjectMetadata> candidateState = null;

    // Backup file for metadata rollback - created before publishing candidate
    private volatile Path metadataBackupFile = null;

    // Track whether metadata was published during this installation attempt
    private volatile boolean metadataPublished = false;

    @FunctionalInterface
    public interface LogEntryApplier {
        /**
         * Applies a log entry to the state machine.
         * @throws IOException if application fails - this will cause InstallSnapshot to return failure
         */
        void apply(LogEntry entry) throws IOException;
    }

    // Additional constructor for when WAL is managed externally
    public RaftNode(ClusterConfig config, Path stateDir, RaftLog raftLog, WAL wal) {
        this.nodeId = config.nodeId();
        this.host = config.host();
        this.port = config.port();
        this.stateDir = stateDir;
        this.singleNode = config.isSingleNode();
        this.peers = new ArrayList<>(config.getOtherPeers());
        this.lastHeartbeat = System.currentTimeMillis();
        this.wal = wal;

        // Use provided RaftLog (which includes WAL already)
        this.raftLog = raftLog;

        // Initialize nextIndex for all peers
        for (RaftPeer peer : peers) {
            nextIndex.put(peer.nodeId(), 1L);
            matchIndex.put(peer.nodeId(), 0L);
        }

        // Load persisted state
        loadState();
    }

    /**
     * Constructor with recovered state from WAL.
     * Restores term, votedFor, and logIndexCounter from recovered data.
     */
    public RaftNode(ClusterConfig config, Path stateDir, RaftLog raftLog, WAL wal,
                     long recoveredTerm, String recoveredVotedFor) {
        this.nodeId = config.nodeId();
        this.host = config.host();
        this.port = config.port();
        this.stateDir = stateDir;
        this.singleNode = config.isSingleNode();
        this.peers = new ArrayList<>(config.getOtherPeers());
        this.lastHeartbeat = System.currentTimeMillis();
        this.wal = wal;

        // Use provided RaftLog (which includes WAL already)
        this.raftLog = raftLog;

        // Restore recovered state
        this.currentTerm = recoveredTerm;
        this.votedFor = recoveredVotedFor;
        // Track that this term has already been persisted to WAL
        this.persistedTerm = recoveredTerm;

        // Restore logIndexCounter to highest index + 1
        // Use getHighestIndex() to get the absolute highest index ever assigned
        // This is critical for correctly assigning new entry indexes after recovery
        long highestIndex = raftLog.getHighestIndex();
        this.logIndexCounter = new AtomicLong(highestIndex + 1);

        // Initialize nextIndex for all peers based on current log state
        for (RaftPeer peer : peers) {
            // Start at log end + 1 for each peer (standard Raft behavior)
            nextIndex.put(peer.nodeId(), highestIndex + 1);
            matchIndex.put(peer.nodeId(), 0L);
        }

        System.out.println("[RAFT] Recovered state: term=" + currentTerm + ", votedFor=" + votedFor +
                ", lastLogIndex=" + highestIndex + ", nextIndex=" + (highestIndex + 1));
    }

    public RaftNode(ClusterConfig config, Path stateDir, RaftLog raftLog) {
        this(config, stateDir, raftLog, null);
    }

    /**
     * Sets the snapshot manager for log compaction.
     */
    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    /**
     * Returns the snapshot manager for log compaction.
     */
    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    // ===== Public API =====

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RaftState getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public boolean isLeader() {
        return state == RaftState.LEADER;
    }

    public Optional<RaftPeer> getLeader() {
        if (leaderId == null) {
            return Optional.empty();
        }
        if (leaderId.equals(nodeId)) {
            return Optional.of(new RaftPeer(nodeId, host, port));
        }
        return peers.stream()
            .filter(p -> p.nodeId().equals(leaderId))
            .findFirst();
    }

    public void start() throws IOException {
        running = true;

        // Start RPC server for peer communication
        startRpcServer();

        // Start election timeout checker
        startElectionTimeoutLoop();

        // Start applying committed entries to state machine
        startApplyLoop();

        // If single node, become leader immediately
        if (singleNode) {
            becomeLeader();
        }
    }

    public void stop() {
        running = false;
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {}
        rpcExecutor.shutdownNow();
        try {
            rpcExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {}
        if (rpcServer != null && rpcServer.isOpen()) {
            try {
                rpcServer.close();
            } catch (IOException ignored) {}
        }
        saveState();
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Submits an entry for replication through Raft.
     * Only valid when this node is the leader.
     * Waits for majority commit before returning success.
     */
    public boolean submit(LogEntry entry) {
        if (state != RaftState.LEADER) {
            return false;
        }

        // Assign index and term
        long index = logIndexCounter.getAndIncrement();
        LogEntry indexedEntry = entry.withIndex(index);

        // Create future for commit acknowledgement
        CompletableFuture<Boolean> commitFuture = new CompletableFuture<>();
        pendingCommits.put(index, commitFuture);

        try {
            // Append to local log (writes to WAL)
            raftLog.append(indexedEntry);

            // For single-node clusters, immediately commit the entry
            // (no followers to replicate to, so we commit locally)
            if (singleNode || peers.isEmpty()) {
                // Commit the entry
                if (indexedEntry.term() == currentTerm) {
                    raftLog.advanceCommitIndex(index);
                }
                // Persist commit index so it survives restarts
                persistCommitIndex();
                // Apply to state machine SYNCHRONOUSLY for single-node
                // (avoids issues with async scheduler not running)
                if (stateMachineApplier != null) {
                    try {
                        stateMachineApplier.accept(indexedEntry);
                        raftLog.advanceLastApplied();
                    } catch (Exception e) {
                        System.err.println("[RAFT] Failed to apply entry: " + e.getMessage());
                    }
                }
                // Complete the future immediately
                commitFuture.complete(true);
                return true;
            }

            // Replicate to followers for multi-node clusters
            replicateToFollowers(indexedEntry);

            // Wait for majority commit with timeout
            boolean committed = commitFuture.get(5, TimeUnit.SECONDS);
            return committed;
        } catch (TimeoutException e) {
            // Timeout - entry may still be pending
            System.err.println("[RAFT] Submit timeout for index " + index);
            return false;
        } catch (InterruptedException | ExecutionException | UncheckedIOException e) {
            System.err.println("[RAFT] Submit failed for index " + index + ": " + e.getMessage());
            return false;
        } finally {
            pendingCommits.remove(index);
        }
    }

    /**
     * Adds a listener for leadership changes.
     */
    public void addLeadershipListener(Runnable listener) {
        leadershipListeners.add(listener);
    }

    /**
     * Sets the log entry applier for the state machine.
     */
    public void setLogEntryApplier(Consumer<LogEntry> applier) {
        this.stateMachineApplier = applier;
    }

    /**
     * Sets the MetadataStore for isolated candidate restoration.
     * Required for crash-safe InstallSnapshot with partial-restore protection.
     */
    public void setMetadataStore(com.storix.metadata.MetadataStore store) {
        this.metadataStore = store;
    }

    // ===== Election and State Transitions =====

    private void startElectionTimeoutLoop() {
        // Initialize election deadline when starting
        resetElectionDeadline();

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (!running) {
                    return;
                }
                if (state == RaftState.LEADER) {
                    return;
                }

                // Check if election timeout has expired
                long now = System.currentTimeMillis();
                if (now > electionDeadline) {
                    System.out.println("[RAFT] Node " + nodeId + " election timeout expired, starting election");
                    startElection();
                    resetElectionDeadline(); // Set next deadline after election starts
                }
            } catch (Exception e) {
                System.err.println("[RAFT] Node " + nodeId + " election loop error: " + e.getMessage());
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void resetElectionDeadline() {
        long timeout = ELECTION_TIMEOUT_MIN_MS +
                random.nextInt((int) (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS));
        electionDeadline = System.currentTimeMillis() + timeout;
    }

    private void startElection() {
        electionLock.lock();
        try {
            if (!running || state == RaftState.LEADER) {
                return;
            }

            // Transition to candidate
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = nodeId; // Vote for self
            votesReceived.clear();
            votesReceived.add(nodeId); // Count self-vote
            saveState();

            System.out.println("[RAFT] Node " + nodeId + " starting election for term " + currentTerm);

            // Send RequestVote to all peers
            long lastLogIndex = raftLog.getLastLogIndex();
            long lastLogTerm = raftLog.getLastLogTerm();

            for (RaftPeer peer : peers) {
                sendRequestVote(peer, lastLogIndex, lastLogTerm);
            }

            // For single node, immediately become leader
            if (singleNode) {
                becomeLeader();
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = nodeId;

        // Reset nextIndex and matchIndex for all peers
        long lastIndex = raftLog.getLastLogIndex();
        for (RaftPeer peer : peers) {
            nextIndex.put(peer.nodeId(), lastIndex + 1);
            matchIndex.put(peer.nodeId(), 0L);
        }

        System.out.println("[RAFT] Node " + nodeId + " became leader for term " + currentTerm);

        // Notify listeners
        notifyLeadershipChange();

        // Start sending heartbeats
        startHeartbeatLoop();
    }

    private void becomeFollower(long term, String leaderId) {
        this.currentTerm = term;
        this.state = RaftState.FOLLOWER;
        this.leaderId = leaderId;
        this.lastHeartbeat = System.currentTimeMillis();
        resetElectionDeadline(); // Reset timeout when becoming follower

        // Don't persist votedFor here - it's set by the vote request handler
        saveState();
    }

    private void notifyLeadershipChange() {
        for (Runnable listener : leadershipListeners) {
            try {
                listener.run();
            } catch (Exception e) {
                System.err.println("[RAFT] Leadership listener error: " + e.getMessage());
            }
        }
    }

    // ===== RPC Handlers =====

    /**
     * Handles RequestVote RPC.
     */
    public RaftMessage.RequestVoteResponse handleRequestVote(long candidateTerm, String candidateId,
                                                            long lastLogIndex, long lastLogTerm) {
        if (candidateTerm < currentTerm) {
            return new RaftMessage.RequestVoteResponse(currentTerm, false);
        }

        if (candidateTerm > currentTerm) {
            // Transition to follower
            currentTerm = candidateTerm;
            votedFor = null;
            saveState();
        }

        lastHeartbeat = System.currentTimeMillis();
        resetElectionDeadline(); // Reset timeout when receiving valid RPC

        // Check if we should grant vote
        boolean voteGranted = false;
        if (votedFor == null || votedFor.equals(candidateId)) {
            // Check if candidate's log is at least as up-to-date as ours
            if (raftLog.isAtLeastAsUpToDate(lastLogIndex, lastLogTerm)) {
                votedFor = candidateId;
                voteGranted = true;
                saveState();
            }
        }

        System.out.println("[RAFT] Node " + nodeId + " vote for " + candidateId + ": " + voteGranted +
                " (term=" + currentTerm + ")");

        return new RaftMessage.RequestVoteResponse(currentTerm, voteGranted);
    }

    /**
     * Handles AppendEntries RPC.
     */
    public RaftMessage.AppendEntriesResponse handleAppendEntries(
            long term, String leaderId, long prevLogIndex, long prevLogTerm,
            List<LogEntry> entries, long leaderCommit) {

        if (term < currentTerm) {
            return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
        }

        if (term > currentTerm) {
            currentTerm = term;
            saveState();
        }

        // Update heartbeat
        lastHeartbeat = System.currentTimeMillis();
        resetElectionDeadline(); // Reset timeout when receiving heartbeat
        this.leaderId = leaderId;

        // If we're a candidate, become follower
        if (state == RaftState.CANDIDATE) {
            becomeFollower(term, leaderId);
        }

        // Check if previous log entry matches
        if (prevLogIndex > 0) {
            // If prevLogIndex is within the snapshot range (before logStartIndex),
            // the follower has this state via the installed snapshot - consider it a match
            if (prevLogIndex >= raftLog.getLogStartIndex()) {
                if (prevLogIndex > raftLog.getLastLogIndex()) {
                    return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
                }
                if (!raftLog.containsEntry(prevLogIndex, prevLogTerm)) {
                    // Log inconsistency - truncate
                    return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
                }
            }
            // If prevLogIndex < logStartIndex, it's in the snapshot range - considered a match
        }

        // Append new entries (this handles conflicts by overwriting)
        if (entries != null && !entries.isEmpty()) {
            try {
                raftLog.appendEntries(prevLogIndex, prevLogTerm, entries);
            } catch (UncheckedIOException e) {
                System.err.println("[RAFT] Failed to append entries to WAL: " + e.getMessage());
                return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
            }
        }

        // Update commit index
        if (leaderCommit > raftLog.getCommitIndex()) {
            raftLog.advanceCommitIndex(Math.min(leaderCommit, raftLog.getLastLogIndex()));
        }

        return new RaftMessage.AppendEntriesResponse(currentTerm, true, raftLog.getLastLogIndex());
    }

    // Snapshot transfer state - for assembling multi-chunk snapshots
    private volatile Path pendingSnapshotFile = null;
    private volatile long pendingSnapshotOffset = 0;
    private volatile long pendingSnapshotIndex = 0;
    private volatile long pendingSnapshotTerm = 0;
    private volatile int pendingChecksum = 0;
    private volatile boolean transferInProgress = false;

    /**
     * Handles InstallSnapshot RPC with a crash-safe installation protocol.
     *
     * GENERATION/COMMIT-MARKER MODEL:
     * The commit marker (generation-<index>.committed) is the point of truth.
     * Only snapshots with a commit marker are authoritative on recovery.
     *
     * CRASH-SAFE PROTOCOL:
     * 1. RECEIVING: Assemble chunks into temp file
     * 2. VALIDATED: Validate checksum and data integrity
     * 3. PERSISTED: Write candidate snapshot (NOT yet committed)
     * 4. RESTORED: Restore state machine from candidate
     * 5. COMMITTED: Commit snapshot, publish state, write commit marker
     *
     * CRITICAL ORDER (commit marker is last):
     * - commitCandidateSnapshot() makes snapshot file authoritative
     * - publishCandidate() updates live metadata atomically
     * - commitGeneration() WRITES THE COMMIT MARKER (final step)
     *
     * On failure at any step:
     * - Before commit marker: old generation remains authoritative
     * - Old state is recoverable from old committed generation
     * - New candidate is discarded on restart (no commit marker = not authoritative)
     *
     * On crash after commit marker:
     * - New generation is authoritative and recovered
     *
     * @return response with success=false if any step fails
     */
    public RaftMessage.InstallSnapshotResponse handleInstallSnapshot(
            long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm,
            long offset, byte[] data, boolean done, int expectedChecksum) {

        if (term < currentTerm) {
            return new RaftMessage.InstallSnapshotResponse(currentTerm, false, 0);
        }

        if (term > currentTerm) {
            currentTerm = term;
            saveState();
        }

        lastHeartbeat = System.currentTimeMillis();
        resetElectionDeadline();
        this.leaderId = leaderId;

        // If we're a candidate, become follower
        if (state == RaftState.CANDIDATE) {
            state = RaftState.FOLLOWER;
        }

        // Start of new snapshot transfer
        if (offset == 0 && data != null && !transferInProgress) {
            // Clean up any previous pending transfer
            cleanupPendingSnapshot();
            installationState = InstallationState.NONE;

            try {
                // Ensure state directory exists
                Files.createDirectories(stateDir);

                // Save current snapshot state for potential rollback
                previousSnapshotIndex = raftLog.getLogStartIndex() - 1;
                previousSnapshotIndexTerm = raftLog.getSnapshotTerm();

                // Create temporary file for assembling snapshot
                String filename = "snapshot-transfer-" + System.nanoTime();
                pendingSnapshotFile = stateDir.resolve(filename);
                pendingSnapshotOffset = 0;
                pendingSnapshotIndex = lastIncludedIndex;
                pendingSnapshotTerm = lastIncludedTerm;
                pendingChecksum = expectedChecksum;
                transferInProgress = true;
                installationState = InstallationState.RECEIVING;

                System.out.println("[RAFT] Starting snapshot transfer: index=" + lastIncludedIndex +
                        ", term=" + lastIncludedTerm + ", expectedChecksum=" + expectedChecksum +
                        ", previousSnapshotIndex=" + previousSnapshotIndex);
            } catch (IOException e) {
                System.err.println("[RAFT] Failed to create snapshot transfer file: " + e.getMessage());
                return new RaftMessage.InstallSnapshotResponse(currentTerm, false, 0);
            }
        }

        // If no transfer in progress and not a new start, ignore
        if (!transferInProgress) {
            // This can happen if we receive a chunk but aren't in transfer mode
            // Return success=false to signal leader to restart
            return new RaftMessage.InstallSnapshotResponse(currentTerm, false, 0);
        }

        // Write chunk at correct offset
        if (data != null && data.length > 0) {
            try {
                Path tempFile = pendingSnapshotFile;

                // Validate offset is sequential (must match expected next offset)
                if (offset < 0 || offset > 100 * 1024 * 1024) { // Max 100MB
                    throw new IOException("Invalid offset: " + offset);
                }
                if (offset != pendingSnapshotOffset) {
                    throw new IOException("Out-of-order snapshot chunk: expected offset " +
                            pendingSnapshotOffset + ", got " + offset);
                }

                // Use RandomAccessFile to write at specific offset
                try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                    raf.seek(offset);
                    raf.write(data);
                }

                pendingSnapshotOffset = offset + data.length;
                System.out.println("[RAFT] Received snapshot chunk: offset=" + offset +
                        ", len=" + data.length + ", total=" + pendingSnapshotOffset);

            } catch (IOException e) {
                System.err.println("[RAFT] Failed to write snapshot chunk: " + e.getMessage());
                cleanupPendingSnapshot();
                installationState = InstallationState.NONE;
                return new RaftMessage.InstallSnapshotResponse(currentTerm, false, 0);
            }
        }

        // All chunks received - install the snapshot with crash-safe protocol
        if (done) {
            try {
                System.out.println("[RAFT] All chunks received, beginning crash-safe installation: index=" +
                        pendingSnapshotIndex + ", term=" + pendingSnapshotTerm);

                // ===== PHASE 1: VALIDATE =====
                System.out.println("[RAFT] [STATE: VALIDATING]");

                // Read complete snapshot data from temp file
                byte[] completeData = Files.readAllBytes(pendingSnapshotFile);

                // Validate the snapshot data
                if (completeData.length == 0) {
                    throw new IOException("Snapshot data is empty");
                }

                // Validate checksum if provided
                if (pendingChecksum != 0) {
                    int computedChecksum = computeChecksum(completeData);
                    if (computedChecksum != pendingChecksum) {
                        throw new IOException("Snapshot checksum mismatch: expected " +
                                pendingChecksum + ", computed " + computedChecksum);
                    }
                    System.out.println("[RAFT] Snapshot checksum validated: " + computedChecksum);
                }

                // ===== PHASE 2: PERSIST AS CANDIDATE =====
                // Persist BEFORE restoring state - but mark as candidate, not committed
                System.out.println("[RAFT] [STATE: PERSISTING_CANDIDATE]");
                if (snapshotManager == null) {
                    throw new IOException("SnapshotManager not configured");
                }

                // Write candidate snapshot to a temp location
                // This snapshot is NOT yet committed - it's a candidate for installation
                candidateSnapshotFile = snapshotManager.persistCandidateSnapshot(
                        completeData, pendingSnapshotIndex, pendingSnapshotTerm, pendingChecksum);
                installationState = InstallationState.CANDIDATE;
                System.out.println("[RAFT] Candidate snapshot persisted at: " + candidateSnapshotFile);

                // ===== PHASE 3: RESTORE TO ISOLATED CANDIDATE =====
                // CRITICAL: Do NOT modify live store until candidate is fully validated
                System.out.println("[RAFT] [STATE: RESTORING_CANDIDATE]");
                try {
                    if (metadataStore != null) {
                        // Use isolated candidate restoration - restores to separate map
                        candidateState = metadataStore.restoreToCandidate(completeData);
                        System.out.println("[RAFT] Candidate state restored: " + candidateState.size() + " objects");
                    } else if (stateMachineApplier != null) {
                        // Fallback to direct state machine applier
                        LogEntry snapshotEntry = new LogEntry(pendingSnapshotTerm, pendingSnapshotIndex,
                                System.currentTimeMillis(), LogEntry.OpType.SNAPSHOT_RESTORE, completeData);
                        stateMachineApplier.accept(snapshotEntry);
                    }
                } catch (Exception e) {
                    throw new IOException("Failed to restore to candidate: " + e.getMessage(), e);
                }
                installationState = InstallationState.RESTORED;
                System.out.println("[RAFT] Candidate state restoration complete");

                // ===== PHASE 4: COMMIT =====
                System.out.println("[RAFT] [STATE: COMMITTING]");

                // CRITICAL ORDERING: The commit marker is the atomic decision point.
                // Before the commit marker, the new generation is NOT authoritative.
                // After the commit marker, everything must be ready.
                //
                // Order:
                // 1. Publish candidate metadata FIRST - makes metadata durable
                // 2. Commit candidate snapshot SECOND - makes snapshot durable
                // 3. Write commit marker THIRD - this is the atomic commit point
                //
                // If we crash:
                // - Before step 1: Old generation remains authoritative
                // - Before step 3: Old generation remains authoritative (no commit marker)
                // - After step 3: New generation is authoritative (both snapshot and metadata are durable)

                // Step 1: Backup current metadata before publishing candidate.
                // This allows us to rollback if later steps fail.
                // We backup BEFORE any modification so we can restore old state.
                metadataBackupFile = null;
                metadataPublished = false;
                if (candidateState != null && !candidateState.isEmpty()) {
                    try {
                        metadataBackupFile = metadataStore.backupStorage();
                        if (metadataBackupFile != null) {
                            System.out.println("[RAFT] Metadata backed up to: " + metadataBackupFile.getFileName());
                        } else {
                            System.out.println("[RAFT] No existing metadata to backup (empty generation)");
                        }
                    } catch (IOException e) {
                        System.err.println("[RAFT] Failed to backup metadata: " + e.getMessage());
                        // Continue without backup - if we fail later, we'll restore from old snapshot
                    }
                }

                // Step 2: Publish candidate state to live store.
                // This makes the metadata durable for the new generation.
                // If this fails, we rollback to the backup.
                if (candidateState != null && !candidateState.isEmpty()) {
                    metadataStore.publishCandidate(candidateState, pendingSnapshotIndex);
                    metadataPublished = true;
                    System.out.println("[RAFT] Candidate metadata published to live store (generation=" + pendingSnapshotIndex + ")");
                }

                // Step 3: Commit the candidate snapshot as the new authoritative snapshot.
                // This makes the snapshot durable for the new generation.
                snapshotManager.commitCandidateSnapshot(candidateSnapshotFile,
                        pendingSnapshotIndex, pendingSnapshotTerm);
                System.out.println("[RAFT] Candidate snapshot committed as new authoritative snapshot");

                // Step 4: Write commit marker LAST - this is the atomic commit point.
                // Only after this marker exists is the new generation authoritative.
                // If we crash BEFORE this step, the old generation remains authoritative.
                // If we crash AFTER this step, the new generation is authoritative.
                snapshotManager.commitGeneration(pendingSnapshotIndex, pendingSnapshotTerm);
                System.out.println("[RAFT] Commit marker written for generation " + pendingSnapshotIndex);

                // Clean up metadata backup after successful commit
                if (metadataBackupFile != null) {
                    metadataStore.deleteBackup(metadataBackupFile);
                    metadataBackupFile = null;
                }

                // Now that commit marker exists, update Raft boundary
                raftLog.setSnapshotBoundary(pendingSnapshotIndex, pendingSnapshotTerm);

                // Advance commit index to include the snapshot
                if (pendingSnapshotIndex > raftLog.getCommitIndex()) {
                    raftLog.advanceCommitIndex(pendingSnapshotIndex);
                }
                raftLog.advanceLastApplied();

                // ===== PHASE 5: COMPACT WAL (best effort, non-fatal) =====
                // WAL compaction is a performance optimization, not a correctness requirement
                // If it fails, we can retry later - the snapshot is still valid
                System.out.println("[RAFT] [STATE: COMPACTING_WAL]");
                boolean walCompacted = false;
                if (wal != null) {
                    try {
                        wal.compact(pendingSnapshotIndex, raftLog.getCommitIndex(), currentTerm, votedFor);
                        walCompacted = true;
                        System.out.println("[RAFT] WAL compacted successfully");
                    } catch (Exception e) {
                        // WAL compaction failure is NOT fatal - snapshot is still valid
                        System.err.println("[RAFT] WAL compaction failed (non-fatal): " + e.getMessage());
                        System.err.println("[RAFT] Snapshot installation continues - WAL will be compacted later");
                    }
                }

                installationState = InstallationState.COMMITTED;
                System.out.println("[RAFT] [STATE: COMMITTED] - Snapshot installed successfully");
                System.out.println("[RAFT]   generation=" + pendingSnapshotIndex +
                        ", logStartIndex=" + raftLog.getLogStartIndex() +
                        ", commitIndex=" + raftLog.getCommitIndex() +
                        ", walCompacted=" + walCompacted);

                // Success!
                return new RaftMessage.InstallSnapshotResponse(currentTerm, true, completeData.length);

            } catch (Exception e) {
                System.err.println("[RAFT] Failed to install snapshot: " + e.getMessage());
                e.printStackTrace();

                // Rollback: Candidate snapshot is discarded, old state remains authoritative
                System.out.println("[RAFT] Rolling back to previous snapshot: index=" + previousSnapshotIndex);

                // Rollback strategy depends on where we failed in the commit sequence:
                //
                // 1. After metadata published but before snapshot committed:
                //    - Metadata has new generation but snapshot still old
                //    - Must NOT remove metadata (it's valid old data), but need to fix generation
                // 2. After snapshot committed but before commit marker:
                //    - Snapshot has new generation, metadata has new generation
                //    - Need to remove committed snapshot (rename it away)
                // 3. After commit marker:
                //    - Need to remove commit marker
                //
                // Common: always remove commit marker if it exists (step 3)

                // Step 1: Remove commit marker if it exists (handles cases 2 and 3)
                if (snapshotManager != null && pendingSnapshotIndex > 0) {
                    try {
                        Path commitMarker = snapshotManager.getSnapshotDir()
                            .resolve("generation-" + pendingSnapshotIndex + ".committed");
                        if (Files.exists(commitMarker)) {
                            Files.delete(commitMarker);
                            System.out.println("[RAFT] Commit marker removed during rollback: generation-" + pendingSnapshotIndex);
                        }
                    } catch (IOException ignored) {}
                }

                // Step 2: Remove committed snapshot if it exists (handles case 2)
                // The committed snapshot would be at snapshot-<index>
                if (snapshotManager != null && pendingSnapshotIndex > 0) {
                    try {
                        Path committedSnapshot = snapshotManager.getSnapshotDir()
                            .resolve("snapshot-" + pendingSnapshotIndex);
                        if (Files.exists(committedSnapshot)) {
                            Files.delete(committedSnapshot);
                            System.out.println("[RAFT] Committed snapshot removed during rollback: snapshot-" + pendingSnapshotIndex);
                        }
                    } catch (IOException ignored) {}
                }

                // Step 3: Discard candidate snapshot if it exists (handles case 1)
                if (candidateSnapshotFile != null && Files.exists(candidateSnapshotFile)) {
                    try {
                        Files.delete(candidateSnapshotFile);
                        System.out.println("[RAFT] Candidate snapshot discarded");
                    } catch (IOException ignored) {}
                }

                // Step 4: Restore metadata from backup if it was published
                // If metadata was published with new generation, we need to restore old state
                // The backup was created BEFORE publishing, so it contains the old state
                if (metadataPublished) {
                    if (metadataBackupFile != null) {
                        // Restore from backup (backup contains old state)
                        try {
                            metadataStore.restoreFromBackup(metadataBackupFile);
                            System.out.println("[RAFT] Metadata restored from backup (generation=" + previousSnapshotIndex + ")");
                        } catch (IOException ex) {
                            System.err.println("[RAFT] Failed to restore metadata from backup: " + ex.getMessage());
                            // Fall through - try to at least restore generation
                            try {
                                metadataStore.setGeneration(previousSnapshotIndex);
                            } catch (IOException ignored) {}
                        }
                    } else {
                        // No backup existed (old state was empty/no storage file)
                        // Delete the storage file to restore empty state
                        try {
                            metadataStore.deleteStorageFile();
                            System.out.println("[RAFT] Metadata storage file deleted (restoring empty state)");
                        } catch (IOException ex) {
                            System.err.println("[RAFT] Failed to delete metadata storage file: " + ex.getMessage());
                        }
                    }
                } else if (metadataStore != null && pendingSnapshotIndex > 0 && previousSnapshotIndex > 0) {
                    // No metadata was published, but ensure generation is reset
                    try {
                        metadataStore.setGeneration(previousSnapshotIndex);
                        System.out.println("[RAFT] Metadata generation reset to: " + previousSnapshotIndex);
                    } catch (IOException ignored) {}
                }

                installationState = InstallationState.NONE;

                // Return success=false to signal leader to retry
                return new RaftMessage.InstallSnapshotResponse(currentTerm, false, 0);
            } finally {
                // Clean up temp transfer file
                if (transferInProgress) {
                    cleanupPendingSnapshot();
                }
                candidateSnapshotFile = null;
                candidateState = null; // Clear candidate state
                // Clean up metadata backup if still exists (on failure path)
                if (metadataBackupFile != null && metadataPublished) {
                    metadataStore.deleteBackup(metadataBackupFile);
                }
                metadataBackupFile = null;
                metadataPublished = false;
            }
        }

        // Not done yet - acknowledge chunk
        return new RaftMessage.InstallSnapshotResponse(currentTerm, true,
                data != null ? data.length : 0);
    }

    /**
     * Cleans up pending snapshot transfer state.
     */
    private void cleanupPendingSnapshot() {
        transferInProgress = false;
        if (pendingSnapshotFile != null && Files.exists(pendingSnapshotFile)) {
            try {
                Files.delete(pendingSnapshotFile);
            } catch (IOException ignored) {}
        }
        pendingSnapshotFile = null;
        pendingSnapshotOffset = 0;
        pendingSnapshotIndex = 0;
        pendingSnapshotTerm = 0;
        pendingChecksum = 0;
    }

    /**
     * Computes CRC32 checksum of data.
     */
    private static int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    // ===== RPC Senders =====

    private void sendRequestVote(RaftPeer peer, long lastLogIndex, long lastLogTerm) {
        rpcExecutor.submit(() -> {
            try {
                RaftMessage.RequestVote request = new RaftMessage.RequestVote(
                        currentTerm, nodeId, lastLogIndex, lastLogTerm);

                RaftMessage.RequestVoteResponse response = sendRpc(peer, request);

                if (response == null) {
                    return;
                }

                // Update term if needed
                if (response.term() > currentTerm) {
                    currentTerm = response.term();
                    state = RaftState.FOLLOWER;
                    votedFor = null;
                    votesReceived.clear();
                    saveState();
                    return;
                }

                // Only process vote if we're still a candidate in the same term
                if (state == RaftState.CANDIDATE && response.term() == currentTerm && response.voteGranted()) {
                    // Record the vote
                    votesReceived.add(peer.nodeId());
                    int votes = countVotes();
                    int majority = calculateMajority();

                    System.out.println("[RAFT] Node " + nodeId + " received vote from " + peer.nodeId() +
                            " (votes=" + votes + "/" + majority + ")");

                    if (votes >= majority) {
                        becomeLeader();
                    }
                }
            } catch (Exception e) {
                System.err.println("[RAFT] RequestVote to " + peer + " failed: " + e.getMessage());
            }
        });
    }

    private void sendAppendEntries(RaftPeer peer) {
        rpcExecutor.submit(() -> {
            try {
                long prevLogIndex = nextIndex.getOrDefault(peer.nodeId(), 1L) - 1;
                long prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;

                List<LogEntry> entries = raftLog.getEntriesFrom(nextIndex.get(peer.nodeId()));

                RaftMessage.AppendEntries request = new RaftMessage.AppendEntries(
                        currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, raftLog.getCommitIndex());

                RaftMessage.AppendEntriesResponse response = sendRpc(peer, request);

                if (response == null) {
                    return;
                }

                // Update term if needed
                if (response.term() > currentTerm) {
                    currentTerm = response.term();
                    state = RaftState.FOLLOWER;
                    votedFor = null;
                    saveState();
                    return;
                }

                if (response.success()) {
                    // Update nextIndex and matchIndex
                    long lastEntryIndex = entries.isEmpty() ? prevLogIndex :
                            entries.get(entries.size() - 1).index();
                    nextIndex.put(peer.nodeId(), lastEntryIndex + 1);
                    matchIndex.put(peer.nodeId(), lastEntryIndex);

                    // Check if we can advance commit index
                    updateCommitIndex();
                } else {
                    // Decrement nextIndex and retry
                    nextIndex.computeIfPresent(peer.nodeId(), (k, v) -> Math.max(1, v - 1));
                }
            } catch (Exception e) {
                System.err.println("[RAFT] AppendEntries to " + peer + " failed: " + e.getMessage());
            }
        });
    }

    private int countVotes() {
        return votesReceived.size();
    }

    private int calculateMajority() {
        int clusterSize = peers.size() + 1; // Include self
        return (clusterSize / 2) + 1;
    }

    private void replicateToFollowers(LogEntry entry) {
        for (RaftPeer peer : peers) {
            sendAppendEntries(peer);
        }
    }

    /**
     * Sends InstallSnapshot RPC to a follower when their log is too stale.
     */
    private void sendInstallSnapshot(RaftPeer peer) {
        if (snapshotManager == null) {
            return;
        }

        rpcExecutor.submit(() -> {
            try {
                // Get current snapshot
                var snapshotOpt = snapshotManager.loadLatestSnapshot();
                if (snapshotOpt.isEmpty()) {
                    return;
                }

                var snapshot = snapshotOpt.get();
                // Use the extracted state data, not the raw file bytes (which include binary header)
                byte[] data = snapshot.stateData();

                // Compute checksum of state data (NOT file bytes)
                int checksum = computeChecksum(data);

                // Send snapshot in chunks
                long offset = 0;
                int chunkSize = 8192; // 8KB chunks
                while (offset < data.length) {
                    int len = (int) Math.min(chunkSize, data.length - offset);
                    byte[] chunk = new byte[len];
                    System.arraycopy(data, (int) offset, chunk, 0, len);
                    boolean done = offset + len >= data.length;

                    // Send checksum with FIRST chunk so follower knows expected checksum for complete snapshot
                    // For multi-chunk transfers, the follower needs the checksum upfront
                    int chunkChecksum = (offset == 0) ? checksum : 0;

                    // Use the snapshot's actual lastIncludedTerm, not current term approximation
                    RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                            currentTerm, nodeId,
                            snapshot.lastIncludedIndex(),
                            snapshot.lastIncludedTerm(),
                            offset, chunk, done, chunkChecksum);

                    RaftMessage.InstallSnapshotResponse response = sendRpc(peer, request);

                    if (response == null || !response.success()) {
                        return;
                    }

                    offset += len;
                }

                // Update nextIndex after successful snapshot install
                nextIndex.put(peer.nodeId(), snapshot.lastIncludedIndex() + 1);
                matchIndex.put(peer.nodeId(), snapshot.lastIncludedIndex());

            } catch (Exception e) {
                System.err.println("[RAFT] InstallSnapshot to " + peer + " failed: " + e.getMessage());
            }
        });
    }

    private void startHeartbeatLoop() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!running || state != RaftState.LEADER) {
                return;
            }

            for (RaftPeer peer : peers) {
                sendAppendEntries(peer);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts a loop that applies committed entries to the state machine.
     */
    private void startApplyLoop() {
        scheduler.scheduleWithFixedDelay(() -> {
            if (!running) {
                return;
            }
            applyCommittedEntries();
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * Applies committed entries to the state machine if one is set.
     */
    public void applyCommittedEntries() {
        if (stateMachineApplier == null) {
            return;
        }

        List<LogEntry> toApply = raftLog.getEntriesToApply();
        for (LogEntry entry : toApply) {
            try {
                stateMachineApplier.accept(entry);
                raftLog.advanceLastApplied();
            } catch (Exception e) {
                System.err.println("[RAFT] Failed to apply entry " + entry.index() + ": " + e.getMessage());
            }
        }

        // Persist commit index periodically after applying entries
        if (wal != null && !toApply.isEmpty()) {
            persistCommitIndex();
        }

        // Check if log compaction is needed
        maybeCompactLog();
    }

    // Maximum log size before compaction (configurable)
    private static final int MAX_LOG_ENTRIES = 1000;

    /**
     * Checks if log compaction should be performed.
     */
    private void maybeCompactLog() {
        if (snapshotManager == null) {
            return;
        }

        int logSize = raftLog.size();
        if (logSize <= MAX_LOG_ENTRIES) {
            return;
        }

        // Only leader can take snapshots
        if (state != RaftState.LEADER) {
            return;
        }

        // Only compact if we've applied most entries
        long lastApplied = raftLog.getLastApplied();
        if (lastApplied < logSize - 100) {
            return; // Not enough entries applied yet
        }

        try {
            compactLog(lastApplied);
        } catch (Exception e) {
            System.err.println("[RAFT] Log compaction failed: " + e.getMessage());
        }
    }

    /**
     * Compacts the log by taking a snapshot.
     * Uses compactThrough() to properly set snapshot boundary and preserve term.
     */
    public void compactLog(long lastIncludedIndex) throws IOException {
        if (lastIncludedIndex <= 0) {
            return;
        }

        // Get the term of the entry at lastIncludedIndex
        long lastIncludedTerm = raftLog.getTermAt(lastIncludedIndex);

        System.out.println("[RAFT] Starting log compaction, lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm);

        // Take a snapshot with the term of the last included entry
        SnapshotManager.Snapshot snapshot = snapshotManager.takeSnapshot(lastIncludedIndex, lastIncludedTerm);

        // Use compactThrough to properly set snapshot boundary
        // This removes entries <= lastIncludedIndex and sets logStartIndex = lastIncludedIndex + 1
        raftLog.compactThrough(lastIncludedIndex, lastIncludedTerm);

        // Compact the WAL
        if (wal != null) {
            // snapshotIndex = lastIncludedIndex, commitIndex = current commitIndex
            wal.compact(lastIncludedIndex, raftLog.getCommitIndex(), currentTerm, votedFor);
        }

        System.out.println("[RAFT] Log compaction complete, log entries now=" + raftLog.size() +
                ", logStartIndex=" + raftLog.getLogStartIndex());
    }

    private void updateCommitIndex() {
        int majority = calculateMajority();

        for (long index = raftLog.getCommitIndex() + 1; index <= raftLog.getLastLogIndex(); index++) {
            if (raftLog.getEntry(index).term() != currentTerm) {
                // Only commit entries from current term
                continue;
            }

            int replicationCount = 1; // Self
            for (long mi : matchIndex.values()) {
                if (mi >= index) {
                    replicationCount++;
                }
            }

            if (replicationCount >= majority) {
                raftLog.advanceCommitIndex(index);

                // Complete any pending commit futures
                CompletableFuture<Boolean> future = pendingCommits.get(index);
                if (future != null) {
                    future.complete(true);
                }
            }
        }
    }

    // ===== RPC Transport =====

    private void startRpcServer() throws IOException {
        rpcServer = ServerSocketChannel.open();
        rpcServer.bind(new InetSocketAddress(port));
        rpcServer.configureBlocking(false);

        rpcExecutor.submit(() -> {
            while (running) {
                try {
                    SocketChannel client = rpcServer.accept();
                    if (client != null) {
                        rpcExecutor.submit(() -> handleRpc(client));
                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[RAFT] RPC server error: " + e.getMessage());
                    }
                }
            }
        });

        System.out.println("[RAFT] RPC server listening on port " + port);
    }

    private void handleRpc(SocketChannel channel) {
        try (channel) {
            // Read message type
            ByteBuffer typeBuffer = ByteBuffer.allocate(1);
            if (readFully(channel, typeBuffer) != 1) {
                return;
            }
            typeBuffer.flip();
            byte msgType = typeBuffer.get();

            switch (msgType) {
                case 1 -> handleRequestVoteRpc(channel);
                case 2 -> handleAppendEntriesRpc(channel);
                case 3 -> handleInstallSnapshotRpc(channel);
            }
        } catch (IOException e) {
            System.err.println("[RAFT] RPC handler error: " + e.getMessage());
        }
    }

    private void handleRequestVoteRpc(SocketChannel channel) throws IOException {
        // Read request
        ByteBuffer termBuf = ByteBuffer.allocate(8);
        readFully(channel, termBuf);
        termBuf.flip();
        long term = termBuf.getLong();

        byte[] candidateIdBytes = readLengthPrefixedBytes(channel);
        String candidateId = new String(candidateIdBytes);

        ByteBuffer lastIndexBuf = ByteBuffer.allocate(8);
        readFully(channel, lastIndexBuf);
        lastIndexBuf.flip();
        long lastLogIndex = lastIndexBuf.getLong();

        ByteBuffer lastTermBuf = ByteBuffer.allocate(8);
        readFully(channel, lastTermBuf);
        lastTermBuf.flip();
        long lastLogTerm = lastTermBuf.getLong();

        // Handle request
        RaftMessage.RequestVoteResponse response = handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);

        // Send response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        respBuf.putLong(response.term());
        respBuf.put(response.voteGranted() ? (byte) 1 : (byte) 0);
        respBuf.flip();
        writeFully(channel, respBuf);
    }

    private void handleAppendEntriesRpc(SocketChannel channel) throws IOException {
        // Read term
        ByteBuffer termBuf = ByteBuffer.allocate(8);
        readFully(channel, termBuf);
        termBuf.flip();
        long term = termBuf.getLong();

        // Read leaderId
        byte[] leaderIdBytes = readLengthPrefixedBytes(channel);
        String leaderId = new String(leaderIdBytes);

        // Read prevLogIndex and prevLogTerm
        ByteBuffer prevIndexBuf = ByteBuffer.allocate(8);
        readFully(channel, prevIndexBuf);
        prevIndexBuf.flip();
        long prevLogIndex = prevIndexBuf.getLong();

        ByteBuffer prevTermBuf = ByteBuffer.allocate(8);
        readFully(channel, prevTermBuf);
        prevTermBuf.flip();
        long prevLogTerm = prevTermBuf.getLong();

        // Read entries count
        ByteBuffer countBuf = ByteBuffer.allocate(4);
        readFully(channel, countBuf);
        countBuf.flip();
        int entryCount = countBuf.getInt();

        // Read entries
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            entries.add(readLogEntry(channel));
        }

        // Read leaderCommit
        ByteBuffer commitBuf = ByteBuffer.allocate(8);
        readFully(channel, commitBuf);
        commitBuf.flip();
        long leaderCommit = commitBuf.getLong();

        // Handle request
        RaftMessage.AppendEntriesResponse response = handleAppendEntries(
                term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);

        // Send response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        respBuf.putLong(response.term());
        respBuf.put(response.success() ? (byte) 1 : (byte) 0);
        respBuf.putLong(response.matchIndex());
        respBuf.flip();
        writeFully(channel, respBuf);
    }

    private LogEntry readLogEntry(SocketChannel channel) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(25);
        readFully(channel, buf);
        buf.flip();

        long term = buf.getLong();
        long index = buf.getLong();
        long timestamp = buf.getLong();
        byte opTypeCode = buf.get();

        byte[] data = readLengthPrefixedBytes(channel);

        return new LogEntry(term, index, timestamp, LogEntry.OpType.fromCode(opTypeCode), data);
    }

    private <T> T sendRpc(RaftPeer peer, RaftMessage message) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(peer.host(), peer.port()))) {
            if (message instanceof RaftMessage.RequestVote rv) {
                return (T) sendRequestVoteRpc(channel, rv);
            } else if (message instanceof RaftMessage.AppendEntries ae) {
                return (T) sendAppendEntriesRpc(channel, ae);
            } else if (message instanceof RaftMessage.InstallSnapshot is) {
                return (T) sendInstallSnapshotRpc(channel, is);
            }
            return null;
        }
    }

    private RaftMessage.RequestVoteResponse sendRequestVoteRpc(SocketChannel channel, RaftMessage.RequestVote rv)
            throws IOException {
        // Send message type
        ByteBuffer typeBuf = ByteBuffer.allocate(1);
        typeBuf.put((byte) 1);
        typeBuf.flip();
        writeFully(channel, typeBuf);

        // Send request
        ByteBuffer reqBuf = ByteBuffer.allocate(1024);
        reqBuf.putLong(rv.term());
        writeLengthPrefixedBytes(reqBuf, rv.candidateId().getBytes());
        reqBuf.putLong(rv.lastLogIndex());
        reqBuf.putLong(rv.lastLogTerm());
        reqBuf.flip();
        writeFully(channel, reqBuf);

        // Read response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        readFully(channel, respBuf);
        respBuf.flip();

        long term = respBuf.getLong();
        boolean voteGranted = respBuf.get() == 1;

        return new RaftMessage.RequestVoteResponse(term, voteGranted);
    }

    private RaftMessage.AppendEntriesResponse sendAppendEntriesRpc(SocketChannel channel, RaftMessage.AppendEntries ae)
            throws IOException {
        // Send message type
        ByteBuffer typeBuf = ByteBuffer.allocate(1);
        typeBuf.put((byte) 2);
        typeBuf.flip();
        writeFully(channel, typeBuf);

        // Send request
        ByteBuffer reqBuf = ByteBuffer.allocate(8192);
        reqBuf.putLong(ae.term());
        writeLengthPrefixedBytes(reqBuf, ae.leaderId().getBytes());
        reqBuf.putLong(ae.prevLogIndex());
        reqBuf.putLong(ae.prevLogTerm());
        reqBuf.putInt(ae.entries() != null ? ae.entries().size() : 0);

        if (ae.entries() != null) {
            for (LogEntry entry : ae.entries()) {
                writeLogEntry(reqBuf, entry);
            }
        }

        reqBuf.putLong(ae.leaderCommit());
        reqBuf.flip();
        writeFully(channel, reqBuf);

        // Read response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        readFully(channel, respBuf);
        respBuf.flip();

        long term = respBuf.getLong();
        boolean success = respBuf.get() == 1;
        long matchIndex = respBuf.getLong();

        return new RaftMessage.AppendEntriesResponse(term, success, matchIndex);
    }

    private RaftMessage.InstallSnapshotResponse sendInstallSnapshotRpc(SocketChannel channel, RaftMessage.InstallSnapshot is)
            throws IOException {
        // Send message type
        ByteBuffer typeBuf = ByteBuffer.allocate(1);
        typeBuf.put((byte) 3);
        typeBuf.flip();
        writeFully(channel, typeBuf);

        // Send request
        ByteBuffer reqBuf = ByteBuffer.allocate(8192);
        reqBuf.putLong(is.term());
        writeLengthPrefixedBytes(reqBuf, is.leaderId().getBytes());
        reqBuf.putLong(is.lastIncludedIndex());
        reqBuf.putLong(is.lastIncludedTerm());
        reqBuf.putLong(is.offset());
        writeLengthPrefixedBytes(reqBuf, is.data());
        reqBuf.put(is.done() ? (byte) 1 : (byte) 0);
        // Send checksum - with FIRST chunk for multi-chunk, follower retains it for validation
        reqBuf.putInt(is.checksum());
        reqBuf.flip();
        writeFully(channel, reqBuf);

        // Read response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        readFully(channel, respBuf);
        respBuf.flip();

        long term = respBuf.getLong();
        boolean success = respBuf.get() == 1;
        long bytesAccepted = respBuf.getLong();

        return new RaftMessage.InstallSnapshotResponse(term, success, bytesAccepted);
    }

    private void handleInstallSnapshotRpc(SocketChannel channel) throws IOException {
        // Read term
        ByteBuffer termBuf = ByteBuffer.allocate(8);
        readFully(channel, termBuf);
        termBuf.flip();
        long term = termBuf.getLong();

        // Read leaderId
        byte[] leaderIdBytes = readLengthPrefixedBytes(channel);
        String leaderId = new String(leaderIdBytes);

        // Read snapshot metadata
        ByteBuffer indexBuf = ByteBuffer.allocate(8);
        readFully(channel, indexBuf);
        indexBuf.flip();
        long lastIncludedIndex = indexBuf.getLong();

        ByteBuffer termBuf2 = ByteBuffer.allocate(8);
        readFully(channel, termBuf2);
        termBuf2.flip();
        long lastIncludedTerm = termBuf2.getLong();

        ByteBuffer offsetBuf = ByteBuffer.allocate(8);
        readFully(channel, offsetBuf);
        offsetBuf.flip();
        long offset = offsetBuf.getLong();

        // Read data
        byte[] data = readLengthPrefixedBytes(channel);

        // Read done flag
        ByteBuffer doneBuf = ByteBuffer.allocate(1);
        readFully(channel, doneBuf);
        doneBuf.flip();
        boolean done = doneBuf.get() == 1;

        // Read checksum (4 bytes) - sent with FIRST chunk for multi-chunk transfers
        // so follower knows the expected checksum for the complete snapshot
        ByteBuffer checksumBuf = ByteBuffer.allocate(4);
        readFully(channel, checksumBuf);
        checksumBuf.flip();
        int checksum = checksumBuf.getInt();

        // Handle request
        RaftMessage.InstallSnapshotResponse response = handleInstallSnapshot(
                term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done, checksum);

        // Send response
        ByteBuffer respBuf = ByteBuffer.allocate(17);
        respBuf.putLong(response.term());
        respBuf.put(response.success() ? (byte) 1 : (byte) 0);
        respBuf.putLong(response.bytesAccepted());
        respBuf.flip();
        writeFully(channel, respBuf);
    }

    private void writeLogEntry(ByteBuffer buf, LogEntry entry) throws IOException {
        buf.putLong(entry.term());
        buf.putLong(entry.index());
        buf.putLong(entry.timestamp());
        buf.put(entry.opType().code());
        writeLengthPrefixedBytes(buf, entry.data());
    }

    private void writeLengthPrefixedBytes(ByteBuffer buf, byte[] data) throws IOException {
        buf.putInt(data.length);
        buf.put(data);
    }

    private byte[] readLengthPrefixedBytes(SocketChannel channel) throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        readFully(channel, lenBuf);
        lenBuf.flip();
        int len = lenBuf.getInt();

        if (len <= 0) {
            return new byte[0];
        }

        byte[] data = new byte[len];
        ByteBuffer dataBuf = ByteBuffer.wrap(data);
        readFully(channel, dataBuf);
        return data;
    }

    private int readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                return totalRead == 0 ? -1 : totalRead;
            }
            totalRead += read;
        }
        return totalRead;
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    // ===== Persistence =====

    private void loadState() {
        File stateFile = stateDir.resolve(STATE_FILE).toFile();
        if (!stateFile.exists()) {
            return;
        }

        try (DataInputStream dis = new DataInputStream(new FileInputStream(stateFile))) {
            currentTerm = dis.readLong();
            String vf = dis.readUTF();
            votedFor = vf.isEmpty() ? null : vf;
            System.out.println("[RAFT] Loaded state: term=" + currentTerm + ", votedFor=" + votedFor);
        } catch (IOException e) {
            System.err.println("[RAFT] Failed to load state: " + e.getMessage());
        }
    }

    private void saveState() {
        try {
            Files.createDirectories(stateDir);
            File stateFile = stateDir.resolve(STATE_FILE).toFile();

            try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(stateFile))) {
                dos.writeLong(currentTerm);
                dos.writeUTF(votedFor != null ? votedFor : "");
                dos.flush();
            }

            // Also persist to WAL for crash recovery
            persistTermToWal();
        } catch (IOException e) {
            System.err.println("[RAFT] Failed to save state: " + e.getMessage());
        }
    }

    /**
     * Persists term and votedFor to WAL.
     * WAL is the authoritative source for crash recovery.
     * Only writes if the term has actually changed from what's already persisted.
     */
    private void persistTermToWal() {
        if (wal != null) {
            try {
                // Only persist if term has actually changed from what's in the WAL
                // This prevents duplicate state records on startup when term is just
                // being incremented as part of the election protocol
                // We track persistedTerm to know what we've already written
                if (currentTerm != persistedTerm) {
                    wal.persistTerm(currentTerm, votedFor);
                    persistedTerm = currentTerm;
                }
            } catch (IOException e) {
                System.err.println("[RAFT] Failed to persist term to WAL: " + e.getMessage());
            }
        }
    }

    /**
     * Persists commit index to WAL.
     * Called periodically or before snapshots.
     */
    public void persistCommitIndex() {
        if (wal != null) {
            try {
                wal.persistCommitIndex(raftLog.getCommitIndex(), raftLog.getLastApplied());
            } catch (IOException e) {
                System.err.println("[RAFT] Failed to persist commit index to WAL: " + e.getMessage());
            }
        }
    }

    /**
     * Returns the raft log for state machine application.
     */
    public RaftLog getRaftLog() {
        return raftLog;
    }

    /**
     * Advances last applied index after state machine application.
     */
    public void advanceLastApplied() {
        raftLog.advanceLastApplied();
    }
}
