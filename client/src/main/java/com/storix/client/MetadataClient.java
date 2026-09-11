package com.storix.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Client for communicating with the Metadata Server.
 */
public class MetadataClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SocketChannel channel;
    private long connectionTimeoutMs = 5000;
    private long requestTimeoutMs = 30000;
    private String clientId;
    private String currentRequestId;

    public MetadataClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void setConnectionTimeout(long timeoutMs) {
        this.connectionTimeoutMs = timeoutMs;
    }

    public void setRequestTimeout(long timeoutMs) {
        this.requestTimeoutMs = timeoutMs;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setCurrentRequestId(String requestId) {
        this.currentRequestId = requestId;
    }

    public String getCurrentRequestId() {
        return currentRequestId;
    }

    public void connect() throws IOException {
        if (channel == null || !channel.isOpen()) {
            try {
                channel = SocketChannel.open();
                channel.configureBlocking(true);
                channel.socket().setSoTimeout((int) connectionTimeoutMs);
                channel.connect(new InetSocketAddress(host, port));
            } catch (UnresolvedAddressException e) {
                throw new IOException("Cannot resolve address: " + host + ":" + port, e);
            }
        }
    }

    public boolean isConnected() {
        return channel != null && channel.isOpen() && channel.isConnected();
    }

    public void createObject(ObjectMetadataDTO metadata) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(metadata);
        sendRequest(MetadataProtocol.CREATE_OBJECT, payload, "CREATE_OBJECT");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("CREATE_OBJECT", response);
        }
    }

    public ObjectMetadataDTO getObject(String objectName) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(Map.of("objectName", objectName));
        sendRequest(MetadataProtocol.GET_OBJECT, payload, "GET_OBJECT");
        Response response = readResponse();
        if (response.status() == MetadataProtocol.NOT_FOUND) {
            return null;
        }
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("GET_OBJECT", response);
        }
        return objectMapper.readValue(response.data(), ObjectMetadataDTO.class);
    }

    public void updateObject(ObjectMetadataDTO metadata) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(metadata);
        sendRequest(MetadataProtocol.UPDATE_OBJECT, payload, "UPDATE_OBJECT");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("UPDATE_OBJECT", response);
        }
    }

    public boolean deleteObject(String objectName) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(Map.of("objectName", objectName));
        sendRequest(MetadataProtocol.DELETE_OBJECT, payload, "DELETE_OBJECT");
        Response response = readResponse();
        if (response.status() == MetadataProtocol.NOT_LEADER) {
            throw createIOException("DELETE_OBJECT", response);
        }
        return response.status() == MetadataProtocol.OK;
    }

    public String[] listObjects() throws IOException {
        ensureConnected();
        sendRequest(MetadataProtocol.LIST_OBJECTS, new byte[0], "LIST_OBJECTS");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("LIST_OBJECTS", response);
        }
        return objectMapper.readValue(response.data(), String[].class);
    }

    // New node management endpoints

    public void registerNode(NodeInfoDTO nodeInfo) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(nodeInfo);
        sendRequest(MetadataProtocol.REGISTER_NODE, payload, "REGISTER_NODE");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("REGISTER_NODE", response);
        }
    }

    public void heartbeat(String nodeId) throws IOException {
        ensureConnected();
        byte[] payload = objectMapper.writeValueAsBytes(Map.of("nodeId", nodeId));
        sendRequest(MetadataProtocol.HEARTBEAT, payload, "HEARTBEAT");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("HEARTBEAT", response);
        }
    }

    public NodeInfoDTO[] getNodes() throws IOException {
        ensureConnected();
        sendRequest(MetadataProtocol.GET_NODES, new byte[0], "GET_NODES");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("GET_NODES", response);
        }
        return objectMapper.readValue(response.data(), NodeInfoDTO[].class);
    }

    public NodeInfoDTO[] getPlacement(int chunkIndex, int chunkSizeBytes) throws IOException {
        ensureConnected();

        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("chunkIndex", chunkIndex);
        if (chunkSizeBytes > 0) {
            payloadMap.put("chunkSizeBytes", (long) chunkSizeBytes);
        }

        byte[] payload = objectMapper.writeValueAsBytes(payloadMap);
        sendRequest(MetadataProtocol.GET_PLACEMENT, payload, "GET_PLACEMENT");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("GET_PLACEMENT", response);
        }
        return objectMapper.readValue(response.data(), NodeInfoDTO[].class);
    }

    public NodeInfoDTO[] getPlacement(int chunkIndex) throws IOException {
        return getPlacement(chunkIndex, -1);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getClusterStatus() throws IOException {
        ensureConnected();
        sendRequest(MetadataProtocol.GET_CLUSTER_STATUS, new byte[0], "GET_CLUSTER_STATUS");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("GET_CLUSTER_STATUS", response);
        }
        return objectMapper.readValue(response.data(), Map.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> repair() throws IOException {
        ensureConnected();
        sendRequest(MetadataProtocol.REPAIR, new byte[0], "REPAIR");
        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw createIOException("REPAIR", response);
        }
        return objectMapper.readValue(response.data(), Map.class);
    }

    private void ensureConnected() throws IOException {
        if (!isConnected()) {
            connect();
        }
    }

    private void sendRequest(byte opcode, byte[] payload, String operation) throws IOException {
        // Wrap payload in request envelope if clientId and requestId are set
        byte[] envelopePayload;
        if (clientId != null && currentRequestId != null) {
            Object payloadObj;
            if (payload == null || payload.length == 0) {
                payloadObj = Map.of();
            } else {
                payloadObj = objectMapper.readValue(payload, Object.class);
            }
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("clientId", clientId);
            envelope.put("requestId", currentRequestId);
            envelope.put("operation", operation);
            envelope.put("payload", payloadObj);
            envelopePayload = objectMapper.writeValueAsBytes(envelope);
        } else {
            envelopePayload = payload;
        }

        int requestSize = 1 + 4 + envelopePayload.length;
        ByteBuffer buffer = ByteBuffer.allocate(4 + requestSize);
        buffer.putInt(requestSize);
        buffer.put(opcode);
        buffer.putInt(envelopePayload.length);
        if (envelopePayload.length > 0) {
            buffer.put(envelopePayload);
        }
        buffer.flip();
        writeFully(channel, buffer);
    }

    private Response readResponse() throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        if (!readFullyWithTimeout(channel, lengthBuffer)) {
            throw new IOException("Request timed out waiting for response length");
        }
        lengthBuffer.flip();
        int responseSize = lengthBuffer.getInt();

        ByteBuffer responseBuffer = ByteBuffer.allocate(responseSize);
        if (!readFullyWithTimeout(channel, responseBuffer)) {
            throw new IOException("Request timed out waiting for response body");
        }
        responseBuffer.flip();

        byte status = responseBuffer.get();
        int dataLength = responseBuffer.getInt();
        byte[] data = new byte[dataLength];
        if (dataLength > 0) {
            responseBuffer.get(data);
        }

        return new Response(status, data);
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Connection closed prematurely");
            }
        }
    }

    private boolean readFullyWithTimeout(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long deadline = System.currentTimeMillis() + requestTimeoutMs;
        while (buffer.hasRemaining()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return false; // Timeout
            }
            channel.socket().setSoTimeout((int) Math.min(remaining, Integer.MAX_VALUE));
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Connection closed prematurely");
            }
        }
        return true;
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } finally {
                channel = null;
            }
        }
    }

    /**
     * Creates an IOException with appropriate message based on response status.
     */
    private IOException createIOException(String operation, Response response) {
        String message = new String(response.data());
        if (response.status() == MetadataProtocol.NOT_LEADER) {
            return new IOException(operation + " NOT_LEADER: " + message);
        }
        return new IOException(operation + " failed: " + message);
    }

    private record Response(byte status, byte[] data) {}

    private static final class MetadataProtocol {
        static final byte CREATE_OBJECT = 1;
        static final byte GET_OBJECT = 2;
        static final byte UPDATE_OBJECT = 3;
        static final byte DELETE_OBJECT = 4;
        static final byte LIST_OBJECTS = 5;

        static final byte REGISTER_NODE = 10;
        static final byte HEARTBEAT = 11;
        static final byte GET_NODES = 12;
        static final byte GET_CLUSTER_STATUS = 13;
        static final byte GET_PLACEMENT = 14;
        static final byte REPAIR = 15;

        static final byte OK = 0;
        static final byte ERROR = 1;
        static final byte NOT_FOUND = 2;
        static final byte NOT_LEADER = 3;
    }
}
