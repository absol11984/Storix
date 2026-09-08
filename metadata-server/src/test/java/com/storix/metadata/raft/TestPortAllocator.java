package com.storix.metadata.raft;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Allocates a group of currently available local TCP ports for integration tests.
 * The lease is released before servers bind so each test can construct its
 * complete cluster configuration without reserving sockets for its lifetime.
 */
final class TestPortAllocator {
    private TestPortAllocator() {}

    static Lease lease(int count) throws IOException {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }

        List<Integer> ports = new ArrayList<>(count);
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                socket.setReuseAddress(true);
                sockets.add(socket);
                ports.add(socket.getLocalPort());
            }
            return new Lease(ports, sockets);
        } catch (IOException | RuntimeException failure) {
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Preserve the allocation failure.
                }
            }
            throw failure;
        }
    }

    static final class Lease implements AutoCloseable {
        private final List<Integer> ports;
        private final List<ServerSocket> sockets;
        private boolean released;

        private Lease(List<Integer> ports, List<ServerSocket> sockets) {
            this.ports = List.copyOf(ports);
            this.sockets = new ArrayList<>(sockets);
        }

        int port(int index) {
            if (index < 0 || index >= ports.size()) {
                throw new IndexOutOfBoundsException("index=" + index);
            }
            return ports.get(index);
        }

        synchronized void release() {
            if (released) {
                return;
            }
            released = true;
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best effort release; bind will report any remaining conflict.
                }
            }
            sockets.clear();
        }

        @Override
        public void close() {
            release();
        }
    }
}
