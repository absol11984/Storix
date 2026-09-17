package com.storix.storage.observability;

public final class LogHandler {
    private LogHandler() {}

    public static void info(String message) {
        System.out.println(message);
    }

    public static void error(String message) {
        System.err.println(message);
    }
}
