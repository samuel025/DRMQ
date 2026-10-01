package com.drmq.broker.raft;

/**
 * Raft node roles as defined by the Raft consensus algorithm (Ongaro et al., 2014).
 *
 * A node is always in exactly one of these three states:
 * - FOLLOWER:  Passive; responds to RPCs from leaders and candidates
 * - CANDIDATE: Actively seeking votes to become leader
 * - LEADER:    Handles all client requests and replicates log to followers
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
