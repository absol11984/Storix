# Storix

A distributed object storage system built with Java 21, featuring multi-node replication, fault tolerance, and automatic repair.

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
                           ▼
                   ┌───────────────┐
                   │   Metadata    │
                   │    Server     │
                   │  (port 9090)  │
                   └───────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │  Node A  │   │  Node B  │   │  Node C  │
    │ (port    │   │ (port    │   │ (port    │
    │  9001)   │   │  9002)   │   │  9003)   │
    └──────────┘   └──────────┘   └──────────┘
```

### Metadata Persistence Architecture

The Metadata Server uses a crash-safe persistence model:

```
                    CURRENT
                       |
                       v
              AUTHORITATIVE SNAPSHOT
                       |
                       v
                 GENERATION N
                       |
              +--------+--------+
              |                 |
        metadata state       snapshot
              |
              v
             WAL
              |
              v
     mutations after snapshot
```

**Key Components:**
- **CURRENT**: Single authoritative generation pointer (atomic file)
- **GenerationManager**: Manages generation directories and CURRENT pointer
- **WAL**: Durable Write-Ahead Log for post-snapshot mutations
- **MetadataStateMachine**: Applies WAL/Raft mutations
- **MetadataStore**: Live/materialized metadata state

**Generation Format:**
```
generations/
    gen-1/
        metadata.json    # State snapshot
        snapshot.bin     # Binary snapshot with header
        manifest.json    # Generation metadata

    gen-2/
        metadata.json
        snapshot.bin
        manifest.json

CURRENT              # Atomic generation pointer
wal.dat             # Write-Ahead Log
```

## Features

### Distributed Storage
- Multiple independent Storage Nodes that register with the Metadata Server
- Configurable replication factor (default: 2) for fault tolerance
- Deterministic round-robin chunk placement across healthy nodes

### Fault Tolerance
- Node health monitoring with heartbeat-based detection
- Automatic failover: reads skip unhealthy replicas and use healthy ones
- Configurable timeout and health-check intervals

### Automatic Repair
- Background health monitor detects under-replicated chunks
- Automatic repair: copies chunks from source replicas to new targets
- Manual repair trigger available via CLI

### Cluster Management
- `status` command shows all nodes, health, and replication status
- `info` command shows per-replica health for each object
- `repair` command triggers manual repair cycle

## Components

### Storage Node (`storage-node`)
- TCP server handling PUT_CHUNK, GET_CHUNK, DELETE_CHUNK operations
- Stores chunks as individual files on disk
- Uses Java NIO and virtual threads
- Registers with metadata server on startup
- Sends heartbeat every 2 seconds

### Metadata Server (`metadata-server`)
- TCP server managing object metadata
- Tracks chunk locations, replica nodes, and checksums
- Persists metadata to JSON file
- **Node Registry**: tracks registered nodes with health status
- **Placement Manager**: selects replica nodes for new chunks
- **Repair Manager**: detects and repairs under-replicated chunks
- **Health Monitor**: scheduled task that checks node health

### Client (`client`)
- CLI for put, get, delete, info, list, status, repair operations
- Handles file chunking and reconstruction
- Communicates with both servers
- Replica-aware GET with checksum verification

## Requirements

- Java 21+
- Maven 3.9+

## Build

```bash
mvn clean package
```

## Start Services

### 1. Start Metadata Server

```bash
java -jar metadata-server/target/metadata-server-1.0.0.jar --port 9090 --metadata ./metadata.json --replication-factor 2 --timeout 6000 --interval 2000
```

Options:
- `--port` - Metadata server port (default: 9090)
- `--metadata` - Metadata persistence file (default: ./metadata.json)
- `--replication-factor` - Number of replicas per chunk (default: 2)
- `--timeout` - Node timeout in milliseconds (default: 6000)
- `--interval` - Health check interval in milliseconds (default: 2000)

### 2. Start Storage Nodes

Each storage node registers with the metadata server and sends heartbeats.

```bash
# Node A
java -jar storage-node/target/storage-node-1.0.0.jar --id node-a --host 127.0.0.1 --port 9001 --storage ./node-a-storage --metadata 127.0.0.1:9090

# Node B
java -jar storage-node/target/storage-node-1.0.0.jar --id node-b --host 127.0.0.1 --port 9002 --storage ./node-b-storage --metadata 127.0.0.1:9090

# Node C
java -jar storage-node/target/storage-node-1.0.0.jar --id node-c --host 127.0.0.1 --port 9003 --storage ./node-c-storage --metadata 127.0.0.1:9090
```

Options:
- `--id` - Unique node identifier (required)
- `--host` - Host address to bind (default: 127.0.0.1)
- `--port` - Port to listen on (required)
- `--storage` - Directory for chunk storage (required)
- `--metadata` - Metadata server address host:port (required)

## Usage

### Upload a file

```bash
java -jar client/target/client-1.0.0.jar put test.bin
```

### Check cluster status

```bash
java -jar client/target/client-1.0.0.jar status
```

Output:
```
Storix Cluster Status

Health: HEALTHY (Lifecycle: RUNNING)

Raft Consensus:
  Role: LEADER | Term: 1 | Leader: node-a | CommitIndex: 100 | LastApplied: 100

Storage Nodes:
--------------------------------------------------------------------------------
Node ID    Address          Status     Health     Chunks   Used/Total
--------------------------------------------------------------------------------
node-a     127.0.0.1:9001   ACTIVE     HEALTHY    10       500MB/1000MB
node-b     127.0.0.1:9002   ACTIVE     HEALTHY    10       510MB/1000MB
node-c     127.0.0.1:9003   ACTIVE     HEALTHY    5        400MB/1000MB
--------------------------------------------------------------------------------

Cluster Data:
  Objects: 10
  Chunks: 25
  Healthy nodes: 3/3
  Replication: Healthy=25, Degraded=0

Metrics Summary:
  Requests: Total=1000, Success=995, Failed=5, Active=2
  Repair: Active=0, Repaired=15, Failed=0
  Recovery: Active=0, Restored=0, Failed=0
  Rebalance: Active=0, Moved=0, Failed=0
```


### Get file info

```bash
java -jar client/target/client-1.0.0.jar info test.bin
```

Output:
```
Object: test.bin
Size: 5242880 bytes
Chunks: 5
Chunk Size: 1048576 bytes
Replication Factor: 2

Chunk 0:
  node-a ACTIVE
  node-b ACTIVE

Chunk 1:
  node-b ACTIVE
  node-c ACTIVE
...

Status: HEALTHY
```

### Download a file

```bash
java -jar client/target/client-1.0.0.jar get test.bin recovered.bin
```

### Delete a file

```bash
java -jar client/target/client-1.0.0.jar delete test.bin
```

### List all objects

```bash
java -jar client/target/client-1.0.0.jar list
```

### Trigger manual repair

```bash
java -jar client/target/client-1.0.0.jar repair
```

## Observability & Cluster Monitoring

The `status` command yields a comprehensive, deterministic health snapshot of the cluster in a lock-free, read-only map generated on demand by the Metadata Server.

### Health States
The cluster explicitly computes health across multiple layers:
- `HEALTHY`: Normal operations.
- `DEGRADED`: At least one storage node is in `recoveryHold`, or there is at least one under-replicated (degraded) chunk.
- `UNHEALTHY`: The metadata server lacks sufficient nodes to satisfy storage requirements, or a node has remained unregistered/unreachable past its timeouts.
- `STARTING` / `STOPPING`: The server is bound but still spinning up or gracefully shutting down operations (no incoming external queries are fully active).

### Metric Semantics
- **Requests**: Captures operations at `opcode` boundaries. Failure categories (e.g. `timeout`, `storage_failure`, `invalid_request`, `internal`) expose the nature of errors deterministically, avoiding stacktrace leakage.
- **Managers**: Tracks background work explicitly using `try/finally` increment/decrement logic.
  - *Repair*: Count of tasks that effectively found and copied chunks.
  - *Recovery*: Scans that synchronized a missing storage node successfully.
  - *Rebalance*: Tasks moving data away from full/evicted nodes.
- **Storage Telemetry**: Heartbeats contain in-line metrics. Storage nodes expose chunk read/write successes/failures dynamically, active connections, and explicit checksum/corruption failures observed via client verification.

### Unhealthy Node Diagnosis Example
When a client detects an anomaly or `get test.bin` hangs/fails, checking `status` is the first diagnostic step.

If a storage node crashes or has a network partition, `StorixCLI status` will show:
```
Storix Cluster Status

Health: DEGRADED (Lifecycle: RUNNING)

Storage Nodes:
--------------------------------------------------------------------------------
Node ID    Address          Status     Health     Chunks   Used/Total
--------------------------------------------------------------------------------
node-a     127.0.0.1:9001   ACTIVE     HEALTHY    10       500MB/1000MB
node-b     127.0.0.1:9002   ACTIVE     HEALTHY    10       510MB/1000MB
node-c     127.0.0.1:9003   INACTIVE   UNHEALTHY  5        400MB/1000MB
--------------------------------------------------------------------------------

Cluster Data:
  Objects: 10
  Chunks: 25
  Healthy nodes: 2/3
  Replication: Healthy=20, Degraded=5
```
Here, `node-c` reads `UNHEALTHY` and 5 chunks are `Degraded`. The administrator should:
1. Try restarting `node-c`. Wait for the node to enter `DEGRADED` (recovery hold) status and eventually `HEALTHY` once caught up.
2. If `node-c` is permanently lost, wait for the timeout, or trigger a manual repair using `storix repair`, which scans degraded chunks and instructs nodes `a` and `b` to replicate them.

## Protocol

### Storage Node Protocol

Request format:
```
[4 bytes: request length]
[1 byte: opcode]
[4 bytes: chunkId length][N bytes: chunkId]
[4 bytes: data length][N bytes: data] (PUT only)
```

Opcodes:
- `1` = PUT_CHUNK
- `2` = GET_CHUNK
- `3` = DELETE_CHUNK

Response format:
```
[4 bytes: response length]
[1 byte: status]
[4 bytes: data length][N bytes: data]
```

Status:
- `0` = OK
- `1` = ERROR

### Metadata Server Protocol

Request format:
```
[4 bytes: request length]
[1 byte: opcode]
[4 bytes: payload length][N bytes: JSON payload]
```

Opcodes (Prompt 1):
- `1` = CREATE_OBJECT
- `2` = GET_OBJECT
- `3` = UPDATE_OBJECT
- `4` = DELETE_OBJECT
- `5` = LIST_OBJECTS

Opcodes (Prompt 2 - Distributed):
- `10` = REGISTER_NODE
- `11` = HEARTBEAT
- `12` = GET_NODES
- `13` = GET_CLUSTER_STATUS
- `14` = GET_PLACEMENT
- `15` = REPAIR

Response format:
```
[4 bytes: response length]
[1 byte: status]
[4 bytes: payload length][N bytes: JSON payload]
```

Status:
- `0` = OK
- `1` = ERROR
- `2` = NOT_FOUND

## Configuration

Default ports:
- Storage Node: 8080
- Metadata Server: 9090

Default chunk size: 1 MB

## Run Tests

```bash
mvn test
```

### Test isolation
`metadata-server/pom.xml` configures Maven Surefire with `forkCount=1` and
`reuseForks=false`. Each metadata-server test class runs in its own JVM,
preventing cross-test port and Raft state pollution. The tradeoff is slower
startup and higher JVM overhead per test class. This is test-only isolation
and does not change Raft election semantics or production timeouts.

## What Was Implemented

### Phase 1: Distributed Storage with Crash Safety

#### Core Storage
- Storage Node TCP server with virtual threads
- Chunk storage on filesystem
- Binary protocol for PUT_CHUNK, GET_CHUNK, DELETE_CHUNK
- Object and chunk metadata models

#### Distributed Cluster
- **Node Registry**: Tracks registered nodes with health status
- **Node Registration**: Storage nodes register with metadata server on startup
- **Heartbeat System**: Nodes send heartbeats every 2 seconds
- **Health Monitor**: Scheduled background task checks node health
- **Placement Manager**: Deterministic round-robin chunk placement
- **Automatic Repair**: Detects under-replicated chunks and repairs them

#### Raft Consensus
- RaftNode for leader election and log replication
- AppendEntries RPC for log consistency
- InstallSnapshot RPC for state transfer
- Term-based leader election

#### Crash-Safe Persistence (Phase 1)
- **GenerationManager**: Manages immutable generation directories
- **CURRENT Pointer**: Atomic generation pointer (single source of truth)
- **WAL**: Write-Ahead Log for durability
- **SnapshotManager**: Log compaction with snapshots
- **Crash Recovery**: Atomic generation commits with rollback safety
- **Generation Immutability**: Committed generations cannot be modified

**Test Coverage (166 tests):**
- WAL tests (recovery, compaction, truncation)
- RaftLog tests (append, truncate, boundary)
- Generation tests (creation, immutability, recovery)
- InstallSnapshot tests (multi-chunk, checksum, failure)
- Crash recovery tests (before/after CURRENT switch)
- Cluster integration tests (registration, heartbeat, failover)

## Files Created/Modified

### storage-node
- `ChunkServer.java` - Added node ID, registration, heartbeat thread
- `pom.xml` - Added maven-shade-plugin for fat JAR

### metadata-server
- `NodeStatus.java` - ACTIVE/UNHEALTHY enum
- `NodeInfo.java` - Node info with heartbeat tracking
- `NodeRegistry.java` - Thread-safe node registry
- `PlacementManager.java` - Deterministic chunk placement
- `RepairManager.java` - Chunk repair logic
- `HealthMonitor.java` - Scheduled health checks
- `ChunkInfo.java` - Added replicaNodeIds and checksum fields
- `MetadataProtocol.java` - Added opcodes 10-15
- `MetadataHandler.java` - Added handlers for new opcodes
- `MetadataServer.java` - Added NodeRegistry, RepairManager, HealthMonitor
- `NodeRegistryTest.java` - Node registry tests
- `PlacementManagerTest.java` - Placement algorithm tests
- `pom.xml` - Added maven-shade-plugin

### client
- `NodeInfoDTO.java` - Node info DTO
- `ChunkInfoDTO.java` - Added replicaNodeIds and checksum
- `MetadataClient.java` - Added registerNode, heartbeat, getNodes, etc.
- `StorixClient.java` - Replica-aware put/get/delete
- `StorixCLI.java` - Added status and repair commands
- `EndToEndIntegrationTest.java` - Full integration tests
- `pom.xml` - Added test dependencies on metadata-server and storage-node

## Phase 1 Status: COMPLETE

Phase 1 of the Storix distributed storage system is complete with:
- ✓ Raft consensus for leader election and log replication
- ✓ Crash-safe persistence with atomic generation commits
- ✓ Generation immutability after commit
- ✓ WAL + snapshot recovery
- ✓ Multi-chunk InstallSnapshot with checksum validation
- ✓ Crash before/after CURRENT switch recovery
- ✓ Full test suite (166 tests)

## Future Enhancements (Not Yet Implemented)

- No authentication
- No encryption
- No load balancing
- No tiered storage
- No quota management

## Example Session

```bash
# Terminal 1: Start metadata server
$ java -jar metadata-server/target/metadata-server-1.0.0.jar --port 9090 --metadata ./metadata.json --replication-factor 2
[HEALTH] Monitor started (timeout=6000ms, interval=2000ms)
Metadata server listening on port 9090

# Terminal 2: Start storage node A
$ java -jar storage-node/target/storage-node-1.0.0.jar --id node-a --port 9001 --storage ./node-a-storage --metadata 127.0.0.1:9090
Registered with metadata server at 127.0.0.1:9090
Storage node node-a listening on 127.0.0.1:9001

# Terminal 3: Start storage node B
$ java -jar storage-node/target/storage-node-1.0.0.jar --id node-b --port 9002 --storage ./node-b-storage --metadata 127.0.0.1:9090

# Terminal 4: Start storage node C
$ java -jar storage-node/target/storage-node-1.0.0.jar --id node-c --port 9003 --storage ./node-c-storage --metadata 127.0.0.1:9090

# Check cluster status
$ java -jar client/target/client-1.0.0.jar status
Storix Cluster Status

Health: HEALTHY (Lifecycle: RUNNING)

Raft State: UNKNOWN (Leader: false)

Storage Nodes:
--------------------------------------------------------------------------------
Node ID    Address          Status     Health     Chunks   Used/Total
--------------------------------------------------------------------------------
node-a     127.0.0.1:9001   ACTIVE     HEALTHY    0        -
node-b     127.0.0.1:9002   ACTIVE     HEALTHY    0        -
node-c     127.0.0.1:9003   ACTIVE     HEALTHY    0        -
--------------------------------------------------------------------------------

Cluster Data:
  Objects: 0
  Chunks: 0
  Healthy nodes: 3/3
  Replication: Healthy=0, Degraded=0

Metrics Summary:
  Requests: Total=0, Success=0, Failed=0, Active=0
  Repair: Active=0, Repaired=0, Failed=0
  Recovery: Active=0, Restored=0, Failed=0
  Rebalance: Active=0, Moved=0, Failed=0

# Upload a file (replicated to 2 nodes)
$ java -jar client/target/client-1.0.0.jar put test.bin
Uploading test.bin
File size: 5242880 bytes
Uploading chunk 1/5
...
Upload successful

# Check object info
$ java -jar client/target/client-1.0.0.jar info test.bin
Object: test.bin
Size: 5242880 bytes
Chunks: 5
Chunk Size: 1048576 bytes
Replication Factor: 2

Chunk 0:
  node-a ACTIVE
  node-b ACTIVE

Chunk 1:
  node-b ACTIVE
  node-c ACTIVE
...

Status: HEALTHY

# Download (automatically uses healthy replicas)
$ java -jar client/target/client-1.0.0.jar get test.bin recovered.bin
Downloading test.bin
...
Download successful: recovered.bin

# Node failure and automatic repair (kill node-b)
$ kill %1  # kill node-b process

# Metadata server detects failure and repairs:
# [HEALTH] Node node-b marked UNHEALTHY
# [REPAIR] Triggering automatic repair...
# [REPAIR] chunk xxx-chunk-0 source=node-a destination=node-c SUCCESS

# GET still works (failover to node-c)
$ java -jar client/target/client-1.0.0.jar get test.bin recovered2.bin
[GET] node-b unavailable (not in active nodes)
Download successful: recovered2.bin

# Restart node-b
$ java -jar storage-node/target/storage-node-1.0.0.jar --id node-b ...

# Clean up
$ java -jar client/target/client-1.0.0.jar delete test.bin
```
