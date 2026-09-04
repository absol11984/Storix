package com.storix.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;

/**
 * Client for communicating with the Metadata Server.
 */
public class MetadataClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SocketChannel channel;

    public MetadataClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the metadata server.
     */
    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    /**
     * Creates object metadata on the server.
     */
    public void createObject(ObjectMetadataDTO metadata) throws IOException {
        ensureConnected();

        byte[] payload = objectMapper.writeValueAsBytes(metadata);
        sendRequest(MetadataProtocol.CREATE_OBJECT, payload);

        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw new IOException("CREATE_OBJECT failed: " + new String(response.data()));
        }
    }

    /**
     * Retrieves object metadata from the server.
     */
    public ObjectMetadataDTO getObject(String objectName) throws IOException {
        ensureConnected();

        byte[] payload = objectMapper.writeValueAsBytes(Map.of("objectName", objectName));
        sendRequest(MetadataProtocol.GET_OBJECT, payload);

        Response response = readResponse();
        if (response.status() == MetadataProtocol.NOT_FOUND) {
            return null;
        }
        if (response.status() != MetadataProtocol.OK) {
            throw new IOException("GET_OBJECT failed: " + new String(response.data()));
        }

        return objectMapper.readValue(response.data(), ObjectMetadataDTO.class);
    }

    /**
     * Updates object metadata on the server.
     */
    public void updateObject(ObjectMetadataDTO metadata) throws IOException {
        ensureConnected();

        byte[] payload = objectMapper.writeValueAsBytes(metadata);
        sendRequest(MetadataProtocol.UPDATE_OBJECT, payload);

        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw new IOException("UPDATE_OBJECT failed: " + new String(response.data()));
        }
    }

    /**
     * Deletes object metadata from the server.
     */
    public boolean deleteObject(String objectName) throws IOException {
        ensureConnected();

        byte[] payload = objectMapper.writeValueAsBytes(Map.of("objectName", objectName));
        sendRequest(MetadataProtocol.DELETE_OBJECT, payload);

        Response response = readResponse();
        return response.status() == MetadataProtocol.OK;
    }

    /**
     * Lists all objects on the server.
     */
    public String[] listObjects() throws IOException {
        ensureConnected();

        sendRequest(MetadataProtocol.LIST_OBJECTS, new byte[0]);

        Response response = readResponse();
        if (response.status() != MetadataProtocol.OK) {
            throw new IOException("LIST_OBJECTS failed: " + new String(response.data()));
        }

        return objectMapper.readValue(response.data(), String[].class);
    }

    private void ensureConnected() throws IOException {
        if (channel == null || !channel.isOpen()) {
            connect();
        }
    }

    private void sendRequest(byte opcode, byte[] payload) throws IOException {
        int requestSize = 1 + 4 + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(4 + requestSize);
        buffer.putInt(requestSize);
        buffer.put(opcode);
        buffer.putInt(payload.length);
        if (payload.length > 0) {
            buffer.put(payload);
        }
        buffer.flip();
        writeFully(channel, buffer);
    }

    private Response readResponse() throws IOException {
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

    private record Response(byte status, byte[] data) {}

    /**
     * Protocol constants matching MetadataProtocol in metadata-server.
     */
    private static final class MetadataProtocol {
        static final byte CREATE_OBJECT = 1;
        static final byte GET_OBJECT = 2;
        static final byte UPDATE_OBJECT = 3;
        static final byte DELETE_OBJECT = 4;
        static final byte LIST_OBJECTS = 5;

        static final byte OK = 0;
        static final byte ERROR = 1;
        static final byte NOT_FOUND = 2;
    }
}
