package com.storix.metadata;

/**
 * Storage-node reintegration state.
 *
 * Used to keep a returning node from immediately participating in normal
 * placement/repair/rebalancing until its local chunk state is reconciled.
 */
public enum NodeRecoveryState {
    /**
     * Node is unavailable for ordinary cluster use.
     */
    FAILED,

    /**
     * Heartbeats resumed; a recovery worker should begin health verification.
     */
    RECONNECTING,

    /**
     * Recovery is verifying liveness and loading fresh capacity/inventory.
     */
    HEALTH_CHECK,

    /**
     * Recovery is reconciling expected chunks on this node.
     */
    RECOVERING,

    /**
     * Node is safe to participate in normal placement/repair/rebalancing.
     */
    READY
}
