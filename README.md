# Storix

A minimal distributed object storage system built with Java 21.

## Architecture

```
Client
   ↓
Metadata Server
   ↓
Storage Node
   ↓
Chunk Storage
```

## Components

### Storage Node (`storage-node`)
- TCP server handling PUT_CHUNK, GET_CHUNK, DELETE_CHUNK operations
- Stores chunks as individual files on disk
- Uses Java NIO and virtual threads

### Metadata Server (`metadata-server`)
- TCP server managing object metadata
- Tracks chunk locations and sizes
- Persists metadata to JSON file

### Client (`client`)
- CLI for put, get, delete, info, list operations
- Handles file chunking and reconstruction
- Communicates with both servers

## Requirements

- Java 21+
- Maven 3.9+

## Build

```bash
mvn clean package
```

## Start Services

### 1. Start Storage Node

```bash
java -jar storage-node/target/storage-node-1.0.0.jar 8080 ./chunks
```

### 2. Start Metadata Server

```bash
java -jar metadata-server/target/metadata-server-1.0.0.jar 9090 ./metadata.json
```

## Usage

### Upload a file

```bash
java -jar client/target/client-1.0.0.jar put test.bin
```

### Get file info

```bash
java -jar client/target/client-1.0.0.jar info test.bin
```

Output:
```
Object: test.bin
Size: 5242880 bytes
Chunk size: 1048576 bytes
Chunks: 5

  uuid-chunk-0 → node-1
  uuid-chunk-1 → node-1
  uuid-chunk-2 → node-1
  uuid-chunk-3 → node-1
  uuid-chunk-4 → node-1
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

Opcodes:
- `1` = CREATE_OBJECT
- `2` = GET_OBJECT
- `3` = UPDATE_OBJECT
- `4` = DELETE_OBJECT
- `5` = LIST_OBJECTS

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

## What Was Implemented

### Already Implemented (from initial codebase)
- Storage Node TCP server (ChunkServer)
- Chunk storage on filesystem (ChunkStorage)
- Binary protocol for PUT_CHUNK and GET_CHUNK (Protocol)
- Request/Response handling (ChunkRequest, ChunkResponse, ChunkHandler)
- Virtual threads for concurrent connections
- Graceful shutdown

### Added in Prompt 1
- DELETE_CHUNK operation in Storage Node
- Metadata Server (MetadataServer, MetadataHandler)
- Metadata persistence with JSON (MetadataStore)
- Object and chunk metadata models (ObjectMetadata, ChunkInfo)
- Chunker for file splitting and reconstruction
- Client library (StorixClient, StorageNodeClient, MetadataClient)
- CLI with put/get/delete/info/list commands
- Comprehensive tests for chunking, storage, and metadata

## Files Created/Modified

### storage-node
- `Protocol.java` - Added DELETE_CHUNK opcode
- `ChunkStorage.java` - Added deleteChunk method
- `ChunkHandler.java` - Added DELETE_CHUNK handling
- `ChunkStorageTest.java` - Storage tests

### metadata-server
- `MetadataServer.java` - Main server
- `MetadataHandler.java` - Request handler
- `MetadataStore.java` - JSON persistence
- `ObjectMetadata.java` - Object metadata model
- `ChunkInfo.java` - Chunk information model
- `MetadataProtocol.java` - Protocol constants
- `MetadataStoreTest.java` - Metadata tests

### client
- `StorixCLI.java` - Command-line interface
- `StorixClient.java` - High-level client
- `StorageNodeClient.java` - Storage node communication
- `MetadataClient.java` - Metadata server communication
- `Chunker.java` - File splitting/reconstruction
- `ObjectMetadataDTO.java` - Metadata DTO
- `ChunkInfoDTO.java` - Chunk info DTO
- `ChunkerTest.java` - Chunking tests

## Limitations (By Design for Prompt 1)

- Single storage node only
- No replication
- No authentication
- No encryption
- No distributed consensus
- No failure recovery

These features are planned for future prompts.

## Example Session

```bash
# Terminal 1: Start storage node
$ java -jar storage-node/target/storage-node-1.0.0.jar 8080 ./chunks
Storage node listening on port 8080

# Terminal 2: Start metadata server
$ java -jar metadata-server/target/metadata-server-1.0.0.jar 9090 ./metadata.json
Metadata server listening on port 9090

# Terminal 3: Upload a file
$ dd if=/dev/urandom of=test.bin bs=1M count=5
$ java -jar client/target/client-1.0.0.jar put test.bin
Uploading test.bin
File size: 5242880 bytes
Chunks: 5
Uploading chunk 1/5
Uploading chunk 2/5
Uploading chunk 3/5
Uploading chunk 4/5
Uploading chunk 5/5
Upload successful

# Check metadata
$ java -jar client/target/client-1.0.0.jar info test.bin
Object: test.bin
Size: 5242880 bytes
Chunk size: 1048576 bytes
Chunks: 5

  abc123-chunk-0 → node-1
  abc123-chunk-1 → node-1
  abc123-chunk-2 → node-1
  abc123-chunk-3 → node-1
  abc123-chunk-4 → node-1

# Download and verify
$ java -jar client/target/client-1.0.0.jar get test.bin recovered.bin
Downloading test.bin
Chunks to download: 5
Downloading chunk 1/5
Downloading chunk 2/5
Downloading chunk 3/5
Downloading chunk 4/5
Downloading chunk 5/5
Download successful: recovered.bin

$ diff test.bin recovered.bin
# No output = files identical

# Delete
$ java -jar client/target/client-1.0.0.jar delete test.bin
Deleting test.bin
Delete successful

# Verify deletion
$ java -jar client/target/client-1.0.0.jar info test.bin
Object not found: test.bin
```
