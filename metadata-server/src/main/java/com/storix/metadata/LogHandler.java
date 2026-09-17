package com.storix.metadata;

/**
 * Simple structured logging helper to replace System.out/err in critical paths.
 * Provides info and error levels. Allows future redirection or filtering.
 */
public final class LogHandler {
    private LogHandler() {}

    public static void info(String message) {
        System.out.println(message);
    }

    public static void error(String message) {
        System.err.println(message);
    }
}
