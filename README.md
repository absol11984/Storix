# Storix

A distributed object storage system built with Java 21, featuring multi-node replication, fault tolerance, and repair.

## Architecture

```
                 ┌─────────────┐
                 │   Client    │
                 └──────┬──────┘
                        │
                        ▼
                ┌───────────────┐
                │   Metadata     │
                │   Server       │
                │  (port 9090)   │
                └──────┬────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
   ┌──────────┐   ┌──────────┐   ┌──────────┐
   │  Node A  │   │  Node B  │   │  Node C  │
   │ (9001)   │   │ (9002)   │   │ (9003)   │
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
            +----------+----------+
            |                     |
 metadata state snapshot
            |
            v
          WAL
            |
            v
   mutations after snapshot
```

**Key Components**
- **CURRENT**: single authoritative generation pointer (atomic file)
- **GenerationManager**: creates/validates immutable generation directories
- **WAL**: durable write-ahead log for post-snapshot mutations
- **MetadataStateMachine**: applies WAL/Raft mutations to reconstruct state
- **MetadataStore**: live/materialized metadata state

### Cluster Health & Repair (high level)
- Storage nodes register with the Metadata Server and send periodic heartbeats.
- The Metadata Server tracks which nodes are **ACTIVE** (and which are unhealthy/dead).
- Placement chooses replica targets among healthy nodes.
- A background repair cycle scans for under-replicated chunks and repairs them.

## Components

### Metadata Server (`metadata-server`)
- TCP server for metadata requests.
- Maintains node registry, chunk/object metadata, and health.

### Storage Node (`storage-node`)
- TCP server that stores chunks on disk.
- Enforces an optional configured logical capacity.
- Registers with the Metadata Server and sends heartbeats.

### Client (`client`)
- CLI for `put`, `get`, `delete`, `info`, and `list`.
- Uploads are chunked and replicated.
- Downloads verify chunk integrity via checksum.

## Protocol (wire-level overview)

### Storage Node Protocol

Request format:

```text
[4 bytes: request length]
[1 byte: opcode]
[4 bytes: chunkId length][N bytes: chunkId]
[4 bytes: data length][N bytes: data] (PUT only)
```

Response format:

```text
[4 bytes: response length]
[1 byte: status]
[4 bytes: data length][N bytes: data]
```

Opcodes:
- `1` = PUT_CHUNK
- `2` = GET_CHUNK
- `3` = DELETE_CHUNK

Status:
- `0` = OK
- `1` = ERROR

### Metadata Server Protocol

Request format:

```text
[4 bytes: request length]
[1 byte: opcode]
[4 bytes: payload length][N bytes: JSON payload]
```

Opcodes:
- `1` = CREATE_OBJECT
- `2` = GET_OBJECT
- `3` = UPDATE_OBJECT
- `4` = DELETE_OBJECT
- `5` = LIST_OBJECTS
- `10` = REGISTER_NODE
- `11` = HEARTBEAT
- `12` = GET_NODES
- `13` = GET_CLUSTER_STATUS
- `14` = GET_PLACEMENT
- `15` = REPAIR

Response format:

```text
[4 bytes: response length]
[1 byte: status]
[4 bytes: payload length][N bytes: JSON payload]
```

Status:
- `0` = OK
- `1` = ERROR
- `2` = NOT_FOUND
- `3` = NOT_LEADER

## Requirements

- **Java 21+**
- **Maven 3.9+**
- **Git**

Verify your tools:

```bash
java -version
mvn -version
git --version
```

## Installation

### 1) Clone the repository

Use the GitHub URL with `git clone` (do not paste the URL directly):

```bash
git clone https://github.com/absol11984/Storix.git
cd Storix
```

## Build

```bash
mvn -DskipTests clean package
```

This:
- compiles/packages the project
- creates these runnable JARs:

```text
metadata-server/target/
storage-node/target/
client/target/
```

## Run Tests

Do **not** manually start the Storix cluster before running tests—Maven tests start their own temporary servers/nodes where applicable.

```bash
mvn clean test
```

If successful, the run ends with:

```text
BUILD SUCCESS
```

### Build vs Test (quick difference)

- `mvn -DskipTests clean package` = build/package only (no tests)
- `mvn clean test` = runs the automated test suite

## Quick Start (3-storage-node demo)

```bash
git clone https://github.com/absol11984/Storix.git
cd Storix
mvn -DskipTests clean package
```

Then start **four separate terminals**:
- Terminal 1: Metadata Server
- Terminal 2: Storage Node A
- Terminal 3: Storage Node B
- Terminal 4: Storage Node C

After nodes register, run this minimal end-to-end check:

```bash
echo "Hello from Storix" > test.txt
java -jar client/target/client-1.0.0.jar put test.txt
java -jar client/target/client-1.0.0.jar get test.txt recovered.txt
cmp test.txt recovered.txt
```

For full details (including `info`, `list`, and SHA-256), see **First Upload and Download** below.

## Running Storix (3-storage-node demonstration)

### Terminal setup (use FOUR separate terminals)

Terminal 1:

```bash
java -jar metadata-server/target/metadata-server-1.0.0.jar \
  --port 9090 \
  --metadata ./metadata.json \
  --replication-factor 2 \
  --timeout 6000 \
  --interval 2000
```

Terminal 2 (Storage Node A):

```bash
java -jar storage-node/target/storage-node-1.0.0.jar \
  --id node-a \
  --host 127.0.0.1 \
  --port 9001 \
  --storage ./node-a-storage \
  --metadata 127.0.0.1:9090 \
  --capacity 107374182400
```

Terminal 3 (Storage Node B):

```bash
java -jar storage-node/target/storage-node-1.0.0.jar \
  --id node-b \
  --host 127.0.0.1 \
  --port 9002 \
  --storage ./node-b-storage \
  --metadata 127.0.0.1:9090 \
  --capacity 107374182400
```

Terminal 4 (Storage Node C):

```bash
java -jar storage-node/target/storage-node-1.0.0.jar \
  --id node-c \
  --host 127.0.0.1 \
  --port 9003 \
  --storage ./node-c-storage \
  --metadata 127.0.0.1:9090 \
  --capacity 107374182400
```

### Startup order

1. Start Metadata Server.
2. Start Node A.
3. Start Node B.
4. Start Node C.
5. **Wait for the storage nodes to register with the Metadata Server.**
6. Only then use the client to upload files.

Uploading before enough nodes are registered can cause placement/replication errors.

### Optional: confirm nodes are healthy

In an additional terminal, you can check status:

```bash
java -jar client/target/client-1.0.0.jar status
```

You want to see enough **ACTIVE** nodes to satisfy the configured replication factor.

## First Upload and Download (end-to-end)

Run this from the repository root (same directory where you ran `mvn ...`).

### 1) Create a small file

```bash
echo "Hello from Storix" > test.txt
```

### 2) Upload

```bash
java -jar client/target/client-1.0.0.jar put test.txt
```

### 3) Inspect object metadata

```bash
java -jar client/target/client-1.0.0.jar info test.txt
```

### 4) List objects

```bash
java -jar client/target/client-1.0.0.jar list
```

### 5) Download (recover)

```bash
java -jar client/target/client-1.0.0.jar get test.txt recovered.txt
```

### 6) Verify exact file equality

- `cmp` prints nothing when files are identical:

```bash
cmp test.txt recovered.txt
```

- SHA-256 hashes should match:

```bash
sha256sum test.txt recovered.txt
```

If the two commands show matching hashes, the download reconstructed the original bytes correctly.

## Client Commands

Storix CLI supports the following commands:

- `put <file> [chunkSize]`
  - Example:
    ```bash
    java -jar client/target/client-1.0.0.jar put test.txt
    ```
  - `chunkSize` is optional and can be provided in bytes (integer) or with `K`/`M` suffix.

- `get <object> <output>`
  - Example:
    ```bash
    java -jar client/target/client-1.0.0.jar get test.txt recovered.txt
    ```

- `info <object>`
  - Example:
    ```bash
    java -jar client/target/client-1.0.0.jar info test.txt
    ```

- `list`
  - Example:
    ```bash
    java -jar client/target/client-1.0.0.jar list
    ```

- `delete <object>`
  - Example:
    ```bash
    java -jar client/target/client-1.0.0.jar delete test.txt
    ```

(Additional commands in this build: `status`, `repair`.)

## File Types

Storix stores **bytes**, so uploads are not limited to a particular file extension.

Examples:

- `.txt`
- `.pdf`
- `.jpg`
- `.png`
- `.mp3`
- `.mp4`
- `.zip`
- `.jar`
- `.java`

## Configuration

Defaults are taken from the current CLI/source; the table below shows the example values used in this README’s 3-node demo.

| Setting | Value (Metadata Server) |
|---|---|
| Metadata Server | `--port 9090` |
| Metadata persistence file | `--metadata ./metadata.json` |
| Replication factor | `--replication-factor 2` |
| Health timeout (`--timeout`) | `6000` ms |
| Health check interval (`--interval`) | `2000` ms |

| Setting | Value (Storage Node A) |
|---|---|
| Node ID (`--id`) | `node-a` |
| Host (`--host`) | `127.0.0.1` |
| Port (`--port`) | `9001` |
| Storage dir (`--storage`) | `./node-a-storage` |
| Metadata (`--metadata`) | `127.0.0.1:9090` |
| Capacity (`--capacity`) | `107374182400` bytes (100 GiB logical) |

| Setting | Value (Storage Node B) |
|---|---|
| Node ID (`--id`) | `node-b` |
| Host (`--host`) | `127.0.0.1` |
| Port (`--port`) | `9002` |
| Storage dir (`--storage`) | `./node-b-storage` |
| Metadata (`--metadata`) | `127.0.0.1:9090` |
| Capacity (`--capacity`) | `107374182400` bytes (100 GiB logical) |

| Setting | Value (Storage Node C) |
|---|---|
| Node ID (`--id`) | `node-c` |
| Host (`--host`) | `127.0.0.1` |
| Port (`--port`) | `9003` |
| Storage dir (`--storage`) | `./node-c-storage` |
| Metadata (`--metadata`) | `127.0.0.1:9090` |
| Capacity (`--capacity`) | `107374182400` bytes (100 GiB logical) |

| Setting | Value |
|---|---|
| Chunk size (client default) | `1048576` bytes (1 MiB) |

### What `--capacity` means

`--capacity 107374182400` configures a **100 GiB logical capacity** for the demo.

This enables capacity-related reporting (e.g., Used/Total) and lets the storage node enforce a maximum logical capacity when writing chunks.

It does **not** pre-allocate or reserve 100 GiB of physical disk space for you. It tracks capacity logically based on actual written chunk sizes.

## Troubleshooting

### Clean restart (stop old local processes)

If you run the demo repeatedly on your laptop, stop any existing Storix processes before restarting.

```bash
pkill -f 'metadata-server-1.0.0.jar' || true
pkill -f 'storage-node-1.0.0.jar' || true
```

Notes:
- This is only intended to stop **local demo** processes.
- If you want to preserve existing data, **do not delete** `metadata.json` or the node storage directories.

### Troubleshooting

#### Connection refused
Check:
- Metadata Server is running.
- Storage nodes are running.
- Storage nodes point to `127.0.0.1:9090` via `--metadata`.
- Required ports (`9090`, `9001`, `9002`, `9003`) are free.

#### Insufficient healthy nodes
Uploads require enough **healthy** nodes to satisfy the configured replication factor.

If you only have (for example) 1 node **ACTIVE** but `--replication-factor 2`, placement/replication can fail.

#### Used/Total shows `-`
Capacity reporting shows `-` when nodes are started without `--capacity` (the default capacity is unlimited/unknown).

Start nodes with `--capacity <bytes>` if you want Used/Total to appear in `status` output.

#### Maven test failure
If `mvn clean test` fails:

1. Stop leftover local Storix processes (if any).
2. Re-run:

```bash
mvn clean test
```

If it still fails, share the failing test output/logs so the underlying issue can be diagnosed.

## Implementation Details

- **Replication & Placement**: chunk replicas are placed deterministically across healthy nodes.
- **Health**: heartbeat-driven node health with timeouts and periodic checks.
- **Repair**: background scans detect under-replicated chunks and repair them.
- **Integrity**: the client verifies chunk checksums during reads.

## Limitations

(not part of this demo build):
- no authentication
- no encryption
- no load balancing
- no tiered storage
- no quota management beyond the demo capacity enforcement
