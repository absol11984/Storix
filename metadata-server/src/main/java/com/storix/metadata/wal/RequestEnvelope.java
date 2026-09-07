package com.storix.metadata.wal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Request envelope containing client identity and request tracking.
 * Used for true end-to-end request idempotency.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestEnvelope(
    String clientId,    // Client identity for deduplication
    String requestId,   // Request identity for idempotency
    String operation,   // Operation name
    byte[] payload     // Operation payload
) {
    /**
     * Creates a new request envelope.
     */
    public static RequestEnvelope of(String clientId, String requestId, String operation, byte[] payload) {
        return new RequestEnvelope(clientId, requestId, operation, payload);
    }

    /**
     * Creates a dedup key from clientId and requestId.
     */
    public String dedupKey() {
        return clientId + ":" + requestId;
    }
}
