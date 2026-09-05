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

    @FunctionalInterface
    public interface LogEntryApplier {
        void apply(LogEntry entry);
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

        // Restore logIndexCounter to highest index + 1
        long highestIndex = raftLog.getLastLogIndex();
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

            // Replicate to followers
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
            if (prevLogIndex > raftLog.getLastLogIndex()) {
                return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
            }
            if (!raftLog.containsEntry(prevLogIndex, prevLogTerm)) {
                // Log inconsistency - truncate
                return new RaftMessage.AppendEntriesResponse(currentTerm, false, 0);
            }
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

    /**
     * Handles InstallSnapshot RPC for follower catch-up.
     * Restores the snapshot state and sets the snapshot boundary in the Raft log.
     */
    public RaftMessage.InstallSnapshotResponse handleInstallSnapshot(
            long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm,
            long offset, byte[] data, boolean done) {

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

        // Apply snapshot to state machine when all chunks are received
        if (done && data != null) {
            try {
                System.out.println("[RAFT] Installing snapshot: lastIncludedIndex=" + lastIncludedIndex +
                        ", lastIncludedTerm=" + lastIncludedTerm);

                // Set the snapshot boundary in RaftLog FIRST
                // This removes entries <= lastIncludedIndex
                raftLog.setSnapshotBoundary(lastIncludedIndex, lastIncludedTerm);

                // Apply snapshot to state machine via the applier
                // The applier should restore the snapshot state to the MetadataStore
                if (stateMachineApplier != null) {
                    // Create a special snapshot entry to trigger state machine restoration
                    LogEntry snapshotEntry = new LogEntry(lastIncludedTerm, lastIncludedIndex,
                            System.currentTimeMillis(), LogEntry.OpType.SNAPSHOT_RESTORE, data);
                    stateMachineApplier.accept(snapshotEntry);
                }

                // Advance commit index to include the snapshot
                if (lastIncludedIndex > raftLog.getCommitIndex()) {
                    raftLog.advanceCommitIndex(lastIncludedIndex);
                }

                // Compact WAL through snapshot boundary
                if (wal != null) {
                    wal.compact(lastIncludedIndex, raftLog.getCommitIndex(), currentTerm, votedFor);
                }

                System.out.println("[RAFT] Snapshot installed, logStartIndex=" + raftLog.getLogStartIndex() +
                        ", commitIndex=" + raftLog.getCommitIndex());

            } catch (Exception e) {
                System.err.println("[RAFT] Failed to apply snapshot: " + e.getMessage());
                e.printStackTrace();
            }
        }

        return new RaftMessage.InstallSnapshotResponse(currentTerm, true, data != null ? data.length : 0);
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
                byte[] data = Files.readAllBytes(snapshot.filePath());

                // Send snapshot in chunks
                long offset = 0;
                int chunkSize = 8192; // 8KB chunks
                while (offset < data.length) {
                    int len = (int) Math.min(chunkSize, data.length - offset);
                    byte[] chunk = new byte[len];
                    System.arraycopy(data, (int) offset, chunk, 0, len);
                    boolean done = offset + len >= data.length;

                    // Use the snapshot's actual lastIncludedTerm, not current term approximation
                    RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                            currentTerm, nodeId,
                            snapshot.lastIncludedIndex(),
                            snapshot.lastIncludedTerm(), // FIXED: use actual snapshot term
                            offset, chunk, done);

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

        // Handle request
        RaftMessage.InstallSnapshotResponse response = handleInstallSnapshot(
                term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done);

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
     */
    private void persistTermToWal() {
        if (wal != null) {
            try {
                wal.persistTerm(currentTerm, votedFor);
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
