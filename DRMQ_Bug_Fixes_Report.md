# DRMQ Bug Fixes & Improvements Report

This document outlines the detailed, exhaustive list of bugs and issues that were recently addressed in the DRMQ repository, along with the technical implementations of their fixes.

---

## 1. Thread Interruption Swallowing in `ClientHandler.java`

### The Bug
In `ClientHandler.java` (around lines 465-473), the `handlePreVoteRequest` method submitted the PreVote execution to the `rpcExecutor` and waited on a bounded `Future.get(10, TimeUnit.SECONDS)`. The catch block simply caught `Exception e` (which includes `InterruptedException`), logged it, and sent back an error response. By doing so, it unlawfully swallowed the thread interrupt status, breaking standard Java concurrency paradigms for graceful shutdown and thread pooling.

### The Fix
Introduced a dedicated `catch (InterruptedException ie)` block *before* the generic `catch (Exception e)`. 
Inside this block:
1. Restored the interrupt flag immediately using `Thread.currentThread().interrupt()`.
2. Emitted the corresponding `BrokerMetrics` for the failed Raft RPC. 
3. Cancelled the pending `future`.
4. Returned a standard PreVote error response.

---

## 2. PreVote Log Rate-Limiting Collision in `RaftPeer.java`

### The Bug
In `RaftPeer.java`, the logging block for failed PreVote RPCs re-used the `lastRequestVoteFailureLogTime` timestamp variable to govern its `LOG_RATE_LIMIT_MS` backoff. Because both `RequestVote` and `PreVote` shared this rate limit bucket, a flood of failed `RequestVote` attempts could suppress the debugging logs for `PreVote` failures, making cluster partition debugging difficult.

### The Fix
Introduced a discrete `lastPreVoteFailureLogTime` member variable to track PreVote failure log events separately. The PreVote catch block now polls and updates *this* variable instead of the RequestVote one, allowing both RPC error loggers to function independently on their own 1-second cadence.

---

## 3. Single-Node Election Deadlock in `RaftNode.java`

### The Bug
In `RaftNode.java`, the server evaluated the quorum count (`votesReceived >= votesNeeded`) strictly *inside* the asynchronous callbacks of the peer RPCs (`CompletableFuture.thenAcceptAsync`). In a single-node cluster (where `peers.isEmpty()`), the peer loop never executes, meaning the quorum success logic was completely bypassed. As a result, a standalone `RaftNode` would continuously call `startPreVote()` via timeouts but never successfully transition into `startElection()` or `becomeLeader()`.

### The Fix
In both `startPreVote()` and `startElection()`, immediately after initializing `votesReceived` to `1` (for the self-vote) and calculating `votesNeeded`, a synchronous check was added *before* iterating over the peer list. If the quorum is instantly satisfied (e.g., 1 vote needed, 1 vote received), the node immediately executes the transition logic without waiting for non-existent peer callbacks.

---

## 4. Concurrent Election Race Conditions in `RaftNode.java`

### The Bug
If a `RaftNode` fired off multiple parallel RequestVote or PreVote RPCs to peers, and the responses arrived almost simultaneously on different threads, multiple threads could theoretically satisfy the `if (votes >= votesNeeded)` condition. This caused the node to execute `startElection()` or `becomeLeader()` multiple times for a single term, leading to erratic state resets and wasted resources.

### The Fix
Introduced `AtomicBoolean` guards (`electionStarted` in `startPreVote` and `electionWon` in `startElection()`). The state transition is now safely wrapped in a Compare-And-Swap (CAS) operation:
```java
if (electionStarted.compareAndSet(false, true)) {
    startElection();
}
```
This guarantees strict idempotency—only the exact thread that tips the quorum past the required threshold is allowed to execute the state transition.

---

## 5. Stale Candidate "Zombie" Bug in `RaftNode.java`

### The Bug
During the Pre-Vote phase, the candidate was ignoring the returned terms from `PreVoteResponse`. If a candidate proposed a PreVote but received a rejection from a peer that had a *higher* term, the candidate ignored this vital information from the cluster and blindly retried later. It failed to update its own term and officially step down.

### The Fix
Updated the `PreVoteResponse` callback in `startPreVote()` to inspect `response.getTerm()`. If a peer replies with a term strictly greater than `currentTerm`, the active node will immediately call `stepDown(response.getTerm())`, jumping immediately to the newest cluster epoch and ceasing further candidate operations for its stale term.

---

## 6. Unit Test Mocking Inconsistencies in `RaftNodeTest.java`

### The Bug
After implementing the correct step-down behavior for higher terms in `RaftNode.java`'s PreVote loop, the `failedPreVoteDoesNotIncrementTerm` unit test started failing. This happened because the mock framework was automatically replying to the `PreVoteRequest` with `req.getTerm()`. Because Pre-Votes send `currentTerm + 1` (a proposed future term), the mock was replying with a term higher than the node's current term, accidentally tricking the local test node into immediately stepping down.

### The Fix
Updated the `RaftNodeTest.java` mock handlers so that PreVote and RequestVote responses echo `req.getTerm() - 1` (mimicking a peer operating on the current stable term, rather than the proposed future term). This correctly exercises the PreVote rejection paths without forcing a term bump and step-down mechanism.

---

## 7. Hyperbolic Documentation Claims in `README.md`

### The Bug
The `README.md` contained absolute, unprovable claims such as ensuring "zero data loss" and promising to "shutdown gracefully." In a complex distributed architecture subject to hardware faults or OS-level `SIGKILL`, absolute architectural guarantees are often misleading or semantically inaccurate.

### The Fix
Adjusted the language in the "Persistent Storage" section to reflect responsible engineering constraints. Replaced "ensure zero data loss" with "designed to minimize data loss", and modified the grammar to fluently say "and handle shutdowns gracefully".