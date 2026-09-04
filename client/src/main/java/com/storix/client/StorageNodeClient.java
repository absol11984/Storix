package com.storix.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Client for communicating with the Storage Node.
 * Handles PUT_CHUNK, GET_CHUNK, and DELETE_CHUNK operations.
 */
public class StorageNodeClient implements AutoCloseable {

    private final String host;
    private final int port;
    private SocketChannel channel;

    // Opcodes matching Protocol in storage-node
    private static final byte PUT_CHUNK = 1;
    private static final byte GET_CHUNK = 2;
    private static final byte DELETE_CHUNK = 3;

    // Status codes
    private static final byte OK = 0;
    private static final byte ERROR = 1;

    public StorageNodeClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the storage node.
     */
    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    /**
     * Stores a chunk on the storage node.
     */
    public void putChunk(String chunkId, byte[] data) throws IOException {
        ensureConnected();

        // Build request: [opcode:1][chunkIdLen:4][chunkId:N][dataLen:4][data:N]
        byte[] chunkIdBytes = chunkId.getBytes();
        int requestSize = 1 + 4 + chunkIdBytes.length + 4 + data.length;

        ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
        request.putInt(requestSize);
        request.put(PUT_CHUNK);
        writeLengthPrefixed(request, chunkIdBytes);
        writeLengthPrefixed(request, data);
        request.flip();

        writeFully(channel, request);

        // Read response
        ChunkResponse response = readResponse();
        if (response.status() != OK) {
            throw new IOException("PUT_CHUNK failed: " + new String(response.data()));
        }
    }

    /**
     * Retrieves a chunk from the storage node.
     */
    public byte[] getChunk(String chunkId) throws IOException {
        ensureConnected();

        // Build request: [opcode:1][chunkIdLen:4][chunkId:N]
        byte[] chunkIdBytes = chunkId.getBytes();
        int requestSize = 1 + 4 + chunkIdBytes.length;

        ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
        request.putInt(requestSize);
        request.put(GET_CHUNK);
        writeLengthPrefixed(request, chunkIdBytes);
        request.flip();

        writeFully(channel, request);

        // Read response
        ChunkResponse response = readResponse();
        if (response.status() != OK) {
            throw new IOException("GET_CHUNK failed: " + new String(response.data()));
        }
        return response.data();
    }

    /**
     * Deletes a chunk from the storage node.
     */
    public void deleteChunk(String chunkId) throws IOException {
        ensureConnected();

        // Build request: [opcode:1][chunkIdLen:4][chunkId:N]
        byte[] chunkIdBytes = chunkId.getBytes();
        int requestSize = 1 + 4 + chunkIdBytes.length;

        ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
        request.putInt(requestSize);
        request.put(DELETE_CHUNK);
        writeLengthPrefixed(request, chunkIdBytes);
        request.flip();

        writeFully(channel, request);

        // Read response
        ChunkResponse response = readResponse();
        if (response.status() != OK) {
            throw new IOException("DELETE_CHUNK failed: " + new String(response.data()));
        }
    }

    private void ensureConnected() throws IOException {
        if (channel == null || !channel.isOpen()) {
            connect();
        }
    }

    private void writeLengthPrefixed(ByteBuffer buffer, byte[] data) {
        buffer.putInt(data.length);
        buffer.put(data);
    }

    private ChunkResponse readResponse() throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        readFully(channel, lengthBuffer);
        lengthBuffer.flip();
        int responseSize = lengthBuffer.getInt();

        ByteBuffer responseBuffer = ByteBuffer.allocate(responseSize);
        readFully(channel, responseBuffer);
        responseBuffer.flip();

        byte status = responseBuffer.get();
        int dataLength = responseBuffer.getInt();
        byte[] data = new byte[dataLength];
        if (dataLength > 0) {
            responseBuffer.get(data);
        }

        return new ChunkResponse(status, data);
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Connection closed prematurely");
            }
        }
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private record ChunkResponse(byte status, byte[] data) {}
}
