# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

### Build

```bash
# Build all modules (storage-node, metadata-server, client)
mvn clean package
```

### Run tests

```bash
# Run the whole repo test suite
mvn clean test

# Run metadata-server tests only
mvn -pl metadata-server clean test

# Run a single test class (JUnit 5)
mvn -pl metadata-server -Dtest=MetadataServerLifecycleRestartTest test

# Run a single test method
mvn -pl metadata-server -Dtest=MetadataServerLifecycleRestartTest#startStopRestartOnSameInstanceShutsDownBackgroundWorkers test

# Run a specific raft integration test
mvn -pl metadata-server -Dtest=RaftClusterIntegrationTest test
```

### Run/verify with specific module

```bash
# storage-node tests
mvn -pl storage-node clean test

# client tests
mvn -pl client clean test
```

## Big-picture architecture

### Top-level modules

- **metadata-server** (`com.storix.metadata.*`)
  - Cluster coordinator: TCP server for metadata requests
  - Raft consensus + persistence orchestration
  - Background managers: health checks, repair, rebalancing, node recovery

- **storage-node** (`com.storix.storage.*`)
  - TCP server that stores chunks on disk
  - Heartbeat loop to keep node health telemetry current

- **client** (`com.storix.client.*`)
  - `StorixCLI` entrypoint
  - `StorixClient` that performs put/get/delete and replica-aware chunk operations

### metadata-server: key control-flow objects

#### `MetadataServer`
- Entry points:
  - `MetadataServer.start()` starts:
    - Raft (only when `ClusterConfig` is provided)
    - background workers: `RepairManager`, `HealthMonitor`, `RebalanceManager`, `NodeRecoveryManager`
    - the client-facing TCP accept loop
  - `MetadataServer.stop()` stops workers, client accept loop, and (if present) `raftNode.stop()`.

#### Persistence + recovery model (authoritative snapshot vs. WAL)
- **Authoritative durability path** is the `GenerationManager` / CURRENT generation.
- Components involved:
  - `GenerationManager`: owns the authoritative snapshot boundaries and generation switching (CURRENT is atomic/commit point)
  - `MetadataStore`: loads/materializes live metadata from the authoritative generation
  - `WAL`: replays post-snapshot mutations to bring Raft state machine up to date
  - `SnapshotManager`: log compaction helper (extracts state from `MetadataStore` for writing new generation snapshots)

When in cluster mode, `MetadataServer`:
1. Initializes/validates `GenerationManager` and loads the authoritative baseline into `MetadataStore`.
2. Recovers post-baseline entries from `wal.recover()` and applies them through `MetadataStateMachine`.
3. Creates `RaftNode` with recovered `term`, `votedFor`, and a `RaftLog` backed by the same WAL.

#### Raft implementation (`metadata-server/src/main/java/com/storix/metadata/raft`)
- `RaftNode`
  - Holds term/vote state, election state, and replication cursors (`nextIndex`, `matchIndex`).
  - Lifecycle:
    - `start()` sets up executors/schedulers, binds RPC server, and schedules election/heartbeat/apply loops.
    - `stop()` cancels work by flipping `running=false`, shuts down scheduler/executor, closes RPC server/channels, then persists state.
  - Critical loops:
    - election timeout loop: `scheduler.scheduleWithFixedDelay(...)` inside `startElectionTimeoutLoop()`
    - leader heartbeat loop: `scheduler.scheduleAtFixedRate(...)` inside `startHeartbeatLoop()`
    - apply loop: scheduled only when not `singleNode`
  - Snapshot transfer:
    - `handleInstallSnapshot()` coordinates crash-safe installation using `GenerationManager.switchCurrent()`.

- `RaftLog`, `RaftMessage`, `RaftPeer`, `RaftState`, `LogEntry`
  - `RaftLog` provides indexes/boundary reconciliation and log entry access.
  - `RaftMessage` contains wire payloads for RequestVote / AppendEntries / InstallSnapshot.

#### Background managers
- `HealthMonitor`
  - Scheduled periodic node health check.
  - Calls `RepairManager.repairAll()` when nodes become unhealthy.

- `RepairManager`
  - Bounded-concurrency repair executor (`repair-worker` threads).
  - `stop()` shuts down the repair executor.

- `NodeRecoveryManager`
  - Scheduled recovery scan (`node-recovery` thread).
  - Drives a failed node through reconnection/health/recovery before letting it re-enter ACTIVE placement.

- `RebalanceManager`
  - Periodic rebalancing scan (`rebalance-manager` thread).
  - In cluster mode it uses the presence of a `RaftNode` to submit durable metadata changes through Raft (instead of writing directly).

### storage-node: core objects
- `ChunkServer`
  - Lifecycle:
    - `start()` registers node with metadata server, starts heartbeat scheduler, binds TCP accept loop.
    - `stop()` shuts down heartbeat scheduler, closes server channel, and closes active client channels.

- `ChunkStorage`
  - Stores chunks on disk and tracks capacity/usage.

### client: core objects
- `StorixCLI`
  - Parses commands: `put/get/delete/info/list/status/repair`.

- `StorixClient`
  - Orchestrates chunking + replica-aware uploads/downloads.
  - Uses `ClusterMetadataClient` to request placement and update metadata.

## Test layout and lifecycle expectations

- Module-specific tests are under:
  - `metadata-server/src/test/java/com/storix/metadata/**`
  - `metadata-server/src/test/java/com/storix/metadata/raft/**`

- Metadata server lifecycle tests focus on ensuring background workers terminate cleanly and are restart-safe:
  - `MetadataServerLifecycleRestartTest`
  - `NodeRecoveryManagerLifecycleTest`
  - `RepairManagerStopTest`

- Raft cluster integration tests live under `com.storix.metadata.raft`:
  - `RaftClusterIntegrationTest`
  - `MultiNodeMetadataServerIntegrationTest`

- `metadata-server/pom.xml` configures Surefire to fork one JVM per test run
  with `reuseForks=false`. This gives each test class its own JVM, preventing
  port/state pollution between tests. The tradeoff is slower startup and higher
  JVM overhead; this is test-only isolation and does not change Raft election
  semantics or timeouts. Within a class you still need to ensure every
  server/node instance is stopped.

## Notes for future Claude Code work (where to look first)

- If investigating leader-election/executor duplication:
  - Start with `RaftNode.start()` / `RaftNode.stop()`.
  - Then check `MetadataServer.start()` / `MetadataServer.stop()` to confirm `raftNode` is only started once per logical node.
  - Finally check which tests spin up multiple `MetadataServer` / `RaftNode` instances and whether any `stop()` paths are missing.
