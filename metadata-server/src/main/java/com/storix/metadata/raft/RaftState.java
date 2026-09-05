package com.storix.metadata.raft;

/**
 * Raft node states.
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
