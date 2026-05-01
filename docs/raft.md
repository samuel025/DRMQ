# DRMQ — Distributed Reliable Message Queue
### A Complete Guide (Written for Intro CS Students)

---

## Table of Contents

1. [What Is This Project?](#1-what-is-this-project)
2. [The Big Picture: Why Does This Problem Exist?](#2-the-big-picture-why-does-this-problem-exist)
3. [Project Structure — The Modules](#3-project-structure--the-modules)
4. [The Protocol: How Machines Talk to Each Other](#4-the-protocol-how-machines-talk-to-each-other)
5. [Stage 1 — Single-Node Mode: Producing and Consuming Messages](#5-stage-1--single-node-mode-producing-and-consuming-messages)
6. [Stage 2 — Cluster Mode: The Problem of Replication](#6-stage-2--cluster-mode-the-problem-of-replication)
7. [The Raft Consensus Algorithm — The Heart of It All](#7-the-raft-consensus-algorithm--the-heart-of-it-all)
8. [Code Deep Dive — File by File](#8-code-deep-dive--file-by-file)
9. [Simulated Example: A Message's Journey Through the Cluster](#9-simulated-example-a-messages-journey-through-the-cluster)
10. [Failure Scenario: What Happens When a Node Dies?](#10-failure-scenario-what-happens-when-a-node-dies)
11. [Putting It All Together: The Full Data Flow](#11-putting-it-all-together-the-full-data-flow)
12. [How to Run DRMQ](#12-how-to-run-drmq)

---

## 1. What Is This Project?

**DRMQ** stands for **Distributed Reliable Message Queue**.

### What is a message queue?

Think of a Post Office. A sender (called a **Producer**) drops off a letter (a **message**) addressed to a box (a **topic**). A receiver (called a **Consumer**) comes by later and picks up their mail. The post office is the **Broker** — it holds the mail in between.

This is exactly what DRMQ does, but for software programs:

```
[Producer App]  ──sends──▶  [DRMQ Broker]  ──delivers──▶  [Consumer App]
                              (stores it)
```

Message queues are used everywhere in industry:
- A website sends a "new order" message to a queue → a warehouse system picks it up
- Uber sends trip data to a queue → analytics systems process it later
- Twitter sends new tweets to a queue → follower feeds get updated

### What does "Distributed" and "Reliable" mean?

- **Distributed**: Instead of ONE broker, we run THREE (or more). They work together.
- **Reliable**: If one broker crashes, the others keep going without losing any messages.

The technology that makes multiple brokers agree on the same data is called **consensus**. This project implements a specific consensus algorithm called **Raft**.

---

## 2. The Big Picture: Why Does This Problem Exist?

### The single-node problem

Imagine your message queue is running on one computer:

```
[Producer] ──▶ [Broker on Computer A] ──▶ [Consumer]
```

What happens if Computer A's hard drive dies? **All your messages are gone.** What if Computer A's CPU melts? **No one can send or receive messages.**

This is called a **Single Point of Failure** (SPOF).

### The distributed solution

What if we ran the broker on THREE computers?

```
                   ┌──────────────────────────────────────────┐
                   │        DRMQ CLUSTER (3 brokers)          │
[Producer] ──▶     │  ┌─────────┐  ┌─────────┐  ┌─────────┐  │  ──▶ [Consumer]
                   │  │Broker A │  │Broker B │  │Broker C │  │
                   │  │(LEADER) │  │follower │  │follower │  │
                   │  └─────────┘  └─────────┘  └─────────┘  │
                   └──────────────────────────────────────────┘
```

Now even if Broker B AND Broker C both crash, Broker A keeps going. As long as a **majority** (2 out of 3) of the brokers agree, the system works.

But here is the hard new question: **How do three computers always have the exact same data?**

That is the problem Raft solves.

---

## 3. Project Structure — The Modules

The project is organized into **Maven modules** (think of each module as a separate mini-project that produces a `.jar` file).

```
DRMQ/
├── drmq-protocol/          ← "The Language" — shared message definitions (.proto)
├── drmq-broker/            ← "The Post Office" — the actual server
├── drmq-client/            ← "The Mailbox Service" — library for apps to use
├── drmq-integration-tests/ ← "The Test Suite" — automated tests
└── docs/                   ← You are here
```

### Module dependency diagram

```
drmq-protocol
      │
      ├──────────────────▶ drmq-broker
      │                         │
      └──────────────────▶ drmq-client
                                │
                         drmq-integration-tests ◀────────────────────┘
```

`drmq-protocol` defines the common language. Both the broker and the client depend on it. The integration tests depend on both.

---

## 4. The Protocol: How Machines Talk to Each Other

### What is a protocol?

A protocol is an agreed-upon language format. When your browser loads a webpage, it uses the **HTTP** protocol. When you send email, it uses **SMTP**. DRMQ uses **Protocol Buffers** (protobuf), which is Google's binary format for structured data.

**File**: `drmq-protocol/src/main/proto/messages.proto`

### The wire format

Every message sent over TCP is wrapped in a `MessageEnvelope`:

```
┌──────────────────────────────────────────────────────────┐
│                    TCP Stream                             │
│                                                          │
│  ┌──────────┬──────────────────────────────────────────┐ │
│  │ 4 bytes  │         N bytes                          │ │
│  │ (length) │  (serialized MessageEnvelope protobuf)   │ │
│  └──────────┴──────────────────────────────────────────┘ │
│                                                          │
│  MessageEnvelope = {                                     │
│    type:    MessageType   (which message is inside?)     │
│    payload: bytes         (the actual inner message)     │
│  }                                                       │
└──────────────────────────────────────────────────────────┘
```

### The message types

| MessageType | Direction | Purpose |
|---|---|---|
| `PRODUCE_REQUEST` | Client → Broker | "Please store this message" |
| `PRODUCE_RESPONSE` | Broker → Client | "OK, stored at offset 42" |
| `CONSUME_REQUEST` | Client → Broker | "Give me messages from offset 10" |
| `CONSUME_RESPONSE` | Broker → Client | "Here are your messages" |
| `COMMIT_OFFSET_REQUEST` | Client → Broker | "I've processed up to offset 15" |
| `FETCH_OFFSET_REQUEST` | Client → Broker | "Where did I leave off?" |
| `REQUEST_VOTE_REQUEST` | Broker → Broker | Raft: "Vote for me as leader!" |
| `REQUEST_VOTE_RESPONSE` | Broker → Broker | Raft: "OK, you have my vote" |
| `APPEND_ENTRIES_REQUEST` | Broker → Broker | Raft: "Replicate these log entries" |
| `APPEND_ENTRIES_RESPONSE` | Broker → Broker | Raft: "Done / Failed" |

> **Key Insight**: Raft peer traffic (`REQUEST_VOTE`, `APPEND_ENTRIES`) uses the **same TCP port** and **same framing** as client traffic. Brokers don't need a separate internal port — the `ClientHandler` handles both.

---

## 5. Stage 1 — Single-Node Mode: Producing and Consuming Messages

Before understanding the distributed cluster, let's understand how a single broker works.

### The single-node architecture

```
┌─────────────────────────────────────────────────────────┐
│                    BrokerServer                         │
│                                                         │
│  ServerSocket (port 9092)                               │
│       │                                                 │
│       ├── Thread 1 ──▶ ClientHandler ──▶ MessageStore   │
│       ├── Thread 2 ──▶ ClientHandler ──▶ MessageStore   │
│       └── Thread N ──▶ ClientHandler ──▶ MessageStore   │
│                                                         │
│  MessageStore                                           │
│  ├── topicIndex: Map<topic, Map<offset, filePosition>>  │
│  ├── messageCache: Map<topic, BoundedMessageCache>      │
│  └── LogManager (on-disk WAL files)                     │
└─────────────────────────────────────────────────────────┘
```

### Producing a message (single-node flow)

```
DRMQProducer.send("orders", "Order #123")
        │
        │ 1. Build ProduceRequest protobuf
        │ 2. Wrap in MessageEnvelope(type=PRODUCE_REQUEST)
        │ 3. Send [4-byte length][bytes] over TCP
        ▼
ClientHandler.run()
        │
        │ 4. Read 4-byte length
        │ 5. Read N bytes → parse MessageEnvelope
        │ 6. Dispatch to handleProduceRequest()
        ▼
ClientHandler.handleProduceRequest()
        │
        │ 7. No RaftNode (single-node mode)
        │ 8. Call messageStore.append("orders", payload, key, ts)
        ▼
MessageStore.append()
        │
        │ 9.  Assign globalOffset (e.g., 42) via AtomicLong.getAndIncrement()
        │ 10. Build StoredMessage protobuf
        │ 11. Write to disk (LogSegment WAL file)
        │ 12. Update topicIndex: "orders" → {42 → filePosition}
        │ 13. Add to BoundedMessageCache
        │ 14. Return offset=42
        ▼
ClientHandler → sends ProduceResponse(success=true, offset=42)
        ▼
DRMQProducer.send() → returns SendResult(success=true, offset=42)
```

### Consuming messages (single-node flow)

```
DRMQConsumer.poll("orders", fromOffset=42, maxMessages=10)
        │
        ▼
ClientHandler.handleConsumeRequest()
        │
        ├── 1. Check BoundedMessageCache first (fast path)
        │         If found in cache → return immediately
        │
        └── 2. Cache miss → look up topicIndex
                    │
                    ├── topicIndex.get("orders").tailMap(42)
                    │         → {42 → filePos, 51 → filePos, ...}
                    │
                    └── For each entry: LogSegment.read(filePosition)
                              → returns StoredMessage from disk
        │
        ▼
Returns ConsumeResponse(messages=[msg42, msg51, ...])
```

### What is the Offset?

An **offset** is just a number that says "where in the queue is this message." Think of it like page numbers in a book:
- Offset 0 = first message ever stored
- Offset 42 = the 43rd message stored (zero-indexed)
- Offsets are **global** across all topics in this implementation

Consumers track which offset they've processed. If a consumer crashes and restarts, it can ask "where did I leave off?" and resume from the right place.

---

## 6. Stage 2 — Cluster Mode: The Problem of Replication

### Starting in cluster mode

When you start three brokers with the `--peers` flag, each broker knows about the others:

```bash
# Broker 1
java -jar drmq-broker.jar \
  --id broker1 --port 9092 \
  --peers broker2:localhost:9093,broker3:localhost:9094 \
  --data-dir ./data-1

# Broker 2
java -jar drmq-broker.jar \
  --id broker2 --port 9093 \
  --peers broker1:localhost:9092,broker3:localhost:9094 \
  --data-dir ./data-2

# Broker 3
java -jar drmq-broker.jar \
  --id broker3 --port 9094 \
  --peers broker1:localhost:9092,broker2:localhost:9093 \
  --data-dir ./data-3
```

### What BrokerConfig parses

**File**: `BrokerConfig.java`

```java
// After parsing --peers broker2:localhost:9093,broker3:localhost:9094
// We get a List<PeerAddress>:
[
  PeerAddress(id="broker2", host="localhost", port=9093),
  PeerAddress(id="broker3", host="localhost", port=9094)
]
```

### Cluster mode startup sequence

```
BrokerServer.constructor()
        │
        ├── isClusterMode() == true  (peers list not empty)
        │
        ├── new RaftNode("broker1", 9092, peers, messageStore, dataDir)
        │         └── loads persistent state (term, votedFor) from disk
        │         └── opens/recovers RaftLog from disk
        │
        ├── For each peer:
        │     new RaftPeer(peerAddress)
        │     raftNode.registerVoteHandler(peerId, raftPeer::sendRequestVote)
        │     raftNode.registerAppendHandler(peerId, raftPeer::sendAppendEntries)
        │
        └── BrokerServer.start()
                └── raftNode.start()
                        └── resetElectionTimer()  ← begins the Raft lifecycle
```

---

## 7. The Raft Consensus Algorithm — The Heart of It All

Raft solves one question: **How do multiple servers agree on what data to store, even when some servers crash or become unreachable?**

### The Three Roles

Every Raft node (broker) is always in one of three states:

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   FOLLOWER ──(timeout: no heartbeat)──▶  CANDIDATE              │
│      ▲                                       │                  │
│      │                          (got majority votes)            │
│      │                                       ▼                  │
│      └──────────────────────────────── LEADER                   │
│                       (got higher term / stepped down)          │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

| Role | Behavior |
|---|---|
| **FOLLOWER** | Passive. Waits for heartbeats from the leader. If no heartbeat comes within a random timeout (150–300ms), becomes a CANDIDATE. |
| **CANDIDATE** | Asks everyone to vote for it. If it gets votes from a majority, becomes LEADER. |
| **LEADER** | The one in charge. Accepts all writes from clients and replicates data to followers. Sends heartbeats every 75ms to keep followers from triggering elections. |

**Key Rule**: There is at most **ONE** leader per **term**. A term is an epoch number that everyone keeps track of. Think of terms like "seasons" of a TV show — each election starts a new term.

### Part A: Leader Election (§5.2)

When a follower doesn't hear from the leader for too long:

```
RaftNode.resetElectionTimer()
        │
        └── Schedules startElection() after random(150ms, 300ms)
                (randomness prevents all followers from calling
                 elections at the exact same time)
```

```
RaftNode.startElection()
        │
        ├── currentTerm++          (e.g., term becomes 2)
        ├── state = CANDIDATE
        ├── votedFor = "broker1"   (vote for yourself)
        ├── savePersistentState()  (written to disk!)
        │
        ├── Build RequestVoteRequest:
        │   {
        │     term:         2,
        │     candidateId:  "broker1",
        │     lastLogIndex: 5,    ← How long is my log?
        │     lastLogTerm:  1     ← What term is my last entry?
        │   }
        │
        └── For each peer, send RequestVoteRequest ASYNCHRONOUSLY
                └── On response:
                      if voteGranted AND still candidate AND same term:
                            votesReceived++
                            if votes >= majority → becomeLeader()
```

### Part B: How Followers Vote

```
RaftNode.handleRequestVote(request)
        │
        ├── If request.term > currentTerm:
        │     stepDown(request.term)  ← update term, become follower
        │
        ├── Grant vote if ALL of:
        │   1. request.term >= currentTerm
        │   2. votedFor == null  OR  votedFor == request.candidateId
        │   3. Candidate's log is "at least as up-to-date" as ours:
        │        - Higher lastLogTerm wins
        │        - If same lastLogTerm, longer log wins
        │
        └── If granted: votedFor = candidateId, save to disk, reset timer
```

> **Why do we check the candidate's log?** Raft must not elect a leader that is MISSING committed messages. This check ensures only a node with all committed data can win.

### Part C: Becoming Leader

```
RaftNode.becomeLeader()
        │
        ├── state = LEADER
        ├── leaderId = "broker1"
        │
        ├── Initialize per-peer tracking:
        │     nextIndex["broker2"]  = lastLogIndex + 1  (optimistic: send from here next)
        │     nextIndex["broker3"]  = lastLogIndex + 1
        │     matchIndex["broker2"] = 0  (we haven't confirmed anything yet)
        │     matchIndex["broker3"] = 0
        │
        ├── Cancel election timer (we are the leader now)
        │
        ├── sendHeartbeats()  (immediate heartbeat to assert authority)
        │
        └── Schedule heartbeatTimer every 75ms → sendHeartbeats()
```

### Part D: Log Replication (§5.3)

This is how a message gets stored on ALL three brokers:

```
RaftNode.propose("orders", payload, key, timestamp)    ← called by ClientHandler
        │
        ├── If not LEADER: throw NOT_LEADER error
        │
        ├── index = raftLog.getLastIndex() + 1   (e.g., index = 6)
        │
        ├── entry = RaftEntry {
        │     term:    2,         ← current leader term
        │     index:   6,         ← position in log
        │     topic:   "orders",
        │     payload: [...bytes],
        │     key:     null,
        │     timestamp: 1234567890
        │   }
        │
        ├── raftLog.append(entry)  ← write to leader's local RaftLog
        │       └── writes [4-byte length][protobuf bytes] to raft/raft.log
        │       └── calls fsync() — data is on disk!
        │
        ├── pendingProposals[6] = new CompletableFuture()
        │       (the calling thread will BLOCK here until this future completes)
        │
        ├── sendHeartbeats()  ← immediately push to followers, don't wait
        │
        └── future.get(5 seconds)  ← WAIT for majority to confirm
```

Now, for each follower:

```
RaftNode.replicateTo(broker2)
        │
        ├── peerNextIndex = nextIndex["broker2"]   = 6
        ├── prevLogIndex = 5,  prevLogTerm = 1
        │
        ├── entries = raftLog.getEntriesFrom(6)    = [entry at index 6]
        │
        ├── Build AppendEntriesRequest:
        │   {
        │     term:         2,
        │     leaderId:     "broker1",
        │     prevLogIndex: 5,
        │     prevLogTerm:  1,
        │     entries:      [RaftEntry{index=6, topic="orders", ...}],
        │     leaderCommit: 5         ← our current commitIndex
        │   }
        │
        └── Send to broker2 via RaftPeer.sendAppendEntries()
```

### Part E: The Follower Processes AppendEntries

```
ClientHandler.handleAppendEntriesRequest()
        └── raftNode.handleAppendEntries(request)
                │
                ├── If request.term > currentTerm: stepDown()
                ├── If request.term < currentTerm: reject (stale leader)
                │
                ├── state = FOLLOWER, leaderId = "broker1"
                ├── resetElectionTimer()  ← "Leader is alive!"
                │
                ├── Log consistency check:
                │     Does my log contain an entry at prevLogIndex with prevLogTerm?
                │     If NO → return success=false  (my log is behind)
                │
                ├── Append each entry:
                │     If conflict (same index, different term) → truncate log first
                │     raftLog.append(entry)  ← write to disk on follower
                │
                ├── If request.leaderCommit > commitIndex:
                │     commitIndex = min(leaderCommit, myLastLogIndex)
                │     applyCommitted()  ← deliver to MessageStore
                │
                └── Return AppendEntriesResponse(success=true, matchIndex=6)
```

### Part F: Committing (Majority Agreement)

When the leader hears back from followers:

```
RaftNode.replicateTo() ← processes response from broker2

        ├── matchIndex["broker2"] = 6  (broker2 has up to index 6)
        ├── nextIndex["broker2"]  = 7
        │
        └── advanceCommitIndex()
                │
                ├── For n = 6 (highest log index):
                │     Is log[6].term == currentTerm (2)?  YES
                │     Count replicas with matchIndex >= 6:
                │         self:    YES  (1)
                │         broker2: YES  (2)  ← matchIndex[broker2] = 6
                │         broker3: NO   (still pending)
                │     majority = (3+1)/2 + 1 = 2
                │     replicaCount(2) >= majority(2)?  YES → COMMIT!
                │
                ├── commitIndex = 6
                │
                └── applyCommitted()
                        │
                        ├── lastApplied = 5 < commitIndex = 6
                        ├── lastApplied++ → 6
                        ├── entry = raftLog.getEntry(6)
                        │
                        ├── messageStore.append("orders", payload, key, timestamp)
                        │         ← Message is NOW visible to consumers!
                        │
                        └── pendingProposals[6].complete(6)
                                  ← Unblocks the ClientHandler thread!
                                  ← propose() returns 6
                                  ← ProduceResponse(success=true, offset=...) sent to client
```

> **The Safety Guarantee**: A message is visible to consumers ONLY after a majority of nodes have written it to their disk. If the leader crashes right after returning "success" to your producer, at least one other node has the data.

---

## 8. Code Deep Dive — File by File

### File Map

```
drmq-broker/src/main/java/com/drmq/broker/
│
├── BrokerServer.java      ← Main entry point, wires everything together
├── BrokerConfig.java      ← Parses CLI args, holds cluster configuration
├── ClientHandler.java     ← Handles ONE client TCP connection (runs in its own thread)
├── MessageStore.java      ← Stores and retrieves messages (WAL + in-memory index)
├── OffsetManager.java     ← Tracks consumer group offsets (bookmark system)
│
└── raft/
    ├── RaftNode.java      ← The complete Raft state machine (leader election + replication)
    ├── RaftLog.java       ← Persistent log of RaftEntry records (the source of truth)
    ├── RaftPeer.java      ← TCP connection to ONE remote Raft peer broker
    └── RaftState.java     ← Enum: FOLLOWER | CANDIDATE | LEADER

drmq-client/src/main/java/com/drmq/client/
├── DRMQProducer.java      ← Client library to SEND messages
└── DRMQConsumer.java      ← Client library to RECEIVE messages

drmq-protocol/src/main/proto/
└── messages.proto         ← All message type definitions (the shared language)
```

### `BrokerConfig.java` — Configuration

**Role**: Parses command-line arguments and holds the cluster membership list.

```java
// The PeerAddress record: what we know about each sibling broker
public record PeerAddress(String id, String host, int port) {
    // Parsed from "broker2:localhost:9093"
}

// isClusterMode() is the key switch
public boolean isClusterMode() {
    return !peers.isEmpty();
}
```

**Input**: CLI args like `--id broker1 --port 9092 --peers broker2:localhost:9093,...`
**Output**: A `BrokerConfig` object with `nodeId`, `port`, `dataDir`, `List<PeerAddress>`

---

### `BrokerServer.java` — The Wiring

**Role**: Creates all the components and connects them. Listens for TCP connections.

**Key decision at startup**:
```java
if (config.isClusterMode()) {
    // Create RaftNode
    this.raftNode = new RaftNode(...);
    // Connect it to each peer via RaftPeer
    for (PeerAddress peer : config.getPeers()) {
        RaftPeer raftPeer = new RaftPeer(peer);
        raftNode.registerVoteHandler(peer.id(), raftPeer::sendRequestVote);
        raftNode.registerAppendHandler(peer.id(), raftPeer::sendAppendEntries);
    }
} else {
    this.raftNode = null; // Single-node mode
}
```

**Input**: A socket connection from any client (producer, consumer, or a peer broker)
**Output**: Creates a `ClientHandler` thread for each connection

---

### `ClientHandler.java` — The Request Router

**Role**: Runs in its own thread for each connected client. Reads messages off the TCP stream and dispatches them.

```java
// The main dispatch switch
return switch (envelope.getType()) {
    case PRODUCE_REQUEST        -> handleProduceRequest(envelope);
    case CONSUME_REQUEST        -> handleConsumeRequest(envelope);
    case COMMIT_OFFSET_REQUEST  -> handleCommitOffsetRequest(envelope);
    case FETCH_OFFSET_REQUEST   -> handleFetchOffsetRequest(envelope);
    // Raft RPCs arrive on the SAME port!
    case REQUEST_VOTE_REQUEST   -> handleRequestVoteRequest(envelope);
    case APPEND_ENTRIES_REQUEST -> handleAppendEntriesRequest(envelope);
};
```

**Cluster-mode Produce handling** (the most important code path):

```java
if (raftNode != null) {
    if (!raftNode.isLeader()) {
        // Not the leader — tell the client who the leader is
        return createProduceErrorResponse("NOT_LEADER:" + leaderAddr);
    }
    // Leader: run through Raft consensus. Blocks until majority commits.
    raftNode.propose(topic, payload, key, timestamp);
    offset = messageStore.getCurrentOffset() - 1;
}
```

**Input**: Raw bytes from TCP stream
**Output**: Raw bytes back to the client, and/or triggers Raft consensus

---

### `RaftLog.java` — The Source of Truth

**Role**: A persistent, append-only log of `RaftEntry` records. Lives at `data/raft/raft.log`.

**On-disk format** (simple and crash-safe):
```
[4 bytes: length of entry 1][protobuf bytes of RaftEntry 1]
[4 bytes: length of entry 2][protobuf bytes of RaftEntry 2]
[4 bytes: length of entry 3][protobuf bytes of RaftEntry 3]
...
```

After each `append()`, it calls `fsync()` (writes to the actual physical disk, not just OS buffer).

**Recovery** (what happens when the broker restarts):
```java
private void recover() throws IOException {
    // Walk the file from the beginning
    while (filePointer < fileLength) {
        int length = raf.readInt();          // Read length header
        byte[] data = new byte[length];
        raf.readFully(data);                 // Read the bytes
        RaftEntry entry = RaftEntry.parseFrom(data);
        entries.add(entry);                  // Rebuild in-memory list
    }
}
```

**Key methods**:
| Method | What it does |
|---|---|
| `append(entry)` | Write entry to disk + memory. Calls fsync. |
| `getEntry(index)` | Get the entry at Raft log index `index` (1-based) |
| `getEntriesFrom(index)` | Get all entries from `index` onwards (for replication) |
| `getLastIndex()` | What is the highest log index we have? |
| `getLastTerm()` | What term is the latest entry in? |
| `truncateFrom(index)` | Delete entries from `index` onwards (conflict resolution) |

---

### `RaftNode.java` — The State Machine

**Role**: The entire Raft implementation in one class (~700 lines). Controls leader election, heartbeats, log replication, and applies committed entries to the `MessageStore`.

**Persistent state** (survives restarts, stored in `data/raft/state.properties`):
```
currentTerm = 2
votedFor    = broker1
```

**Volatile state** (rebuilt on startup):
```java
RaftState state;      // FOLLOWER | CANDIDATE | LEADER
long commitIndex;     // Highest log index known to be committed
long lastApplied;     // Highest log index applied to MessageStore
String leaderId;      // Who do we think the leader is?
```

**Leader-only state**:
```java
Map<String, Long> nextIndex;   // "Send broker2 starting from index 6"
Map<String, Long> matchIndex;  // "broker2 has confirmed up to index 5"
```

---

### `RaftPeer.java` — The Network Bridge

**Role**: Manages the TCP connection to ONE remote broker. Sends `RequestVote` and `AppendEntries` RPCs.

```
RaftNode                    RaftPeer                  Remote Broker
   │                            │                           │
   │── voteRpcHandlers.get(     │                           │
   │       "broker2").apply(req)│                           │
   │                            │── socket.connect() ──────▶│
   │                            │── sendEnvelope(req) ─────▶│
   │                            │◀─ receiveEnvelope(resp) ──│
   │◀─ returns response ────────│                           │
```

**Auto-reconnect**: If the connection drops, the next RPC call to `ensureConnected()` re-establishes it automatically.

---

### `MessageStore.java` — The Actual Storage

**Role**: Stores and retrieves `StoredMessage` objects. Two-layer: in-memory cache + disk WAL.

```
MessageStore
├── BoundedMessageCache (per topic)
│     └── Up to 1000 most recent messages in memory
│           (backed by ArrayDeque for LRU eviction)
│
└── topicIndex: ConcurrentSkipListMap<offset, filePosition>
      └── "For topic 'orders', offset 42 is at byte 1024 in the WAL file"
            (ConcurrentSkipListMap keeps offsets sorted for range queries)
```

**Read path** (`getMessages`):
```
1. Try BoundedMessageCache (fast, no I/O)
2. If cache miss → look up topicIndex for file position
3. Read from LogSegment (disk I/O)
```

**Write path** (`append`):
```
1. Assign global offset (atomic increment — thread safe)
2. Write to LogSegment WAL file
3. Update topicIndex with file position
4. Add to BoundedMessageCache
```

---

### `DRMQProducer.java` — The Client Library

**Role**: The library your application uses to send messages. Handles connection, serialization, and leader redirection.

**Leader redirection** (the clever part):
```java
if (errorMsg.startsWith("NOT_LEADER:")) {
    String leaderAddr = errorMsg.substring("NOT_LEADER:".length());
    // e.g., "localhost:9092"
    // Close current connection, reconnect to the leader, retry
    return redirectToLeader(leaderAddr, topic, payload, key);
}
```

This means your app code stays simple:
```java
DRMQProducer producer = new DRMQProducer("localhost", 9093); // talk to any broker
producer.connect();
// Even if 9093 is a follower, it auto-redirects to the leader!
SendResult result = producer.send("orders", "Order #123");
```

---

## 9. Simulated Example: A Message's Journey Through the Cluster

Let's trace **exactly** what happens when you call `producer.send("orders", "Hello World")` in a running 3-node cluster.

### Initial State

```
broker1 (port 9092): LEADER,   term=1, log=[1,2,3,4,5], commitIndex=5
broker2 (port 9093): FOLLOWER, term=1, log=[1,2,3,4,5], commitIndex=5
broker3 (port 9094): FOLLOWER, term=1, log=[1,2,3,4,5], commitIndex=5
```

### Step 1: Producer connects and sends

```java
DRMQProducer producer = new DRMQProducer("localhost", 9092);
producer.send("orders", "Hello World");
```

**What `DRMQProducer.send()` does**:

```
ProduceRequest {
  topic:     "orders"
  payload:   [72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100]  (UTF-8 of "Hello World")
  timestamp: 1742070865000
}

Wrapped in:
MessageEnvelope {
  type:    PRODUCE_REQUEST (= 1)
  payload: <serialized ProduceRequest bytes>
}

Sent over TCP as:
[0x00, 0x00, 0x00, 0x1F]  ← 4-byte big-endian length (31 bytes)
[0x08, 0x01, 0x12, ...]   ← serialized MessageEnvelope bytes
```

---

### Step 2: ClientHandler on broker1 receives it

```
broker1: ClientHandler.run()
   ├── in.readInt()          → 31
   ├── in.readFully(31 bytes) → envelope bytes
   ├── MessageEnvelope.parseFrom(bytes) → envelope
   └── handleMessage(envelope)
           └── case PRODUCE_REQUEST → handleProduceRequest(envelope)
```

---

### Step 3: Broker1 checks if it's the leader

```
broker1: ClientHandler.handleProduceRequest()
   ├── raftNode != null  → cluster mode
   ├── raftNode.isLeader() → true  ✓ (broker1 is leader)
   └── raftNode.propose("orders", payload, null, 1742070865000)
```

---

### Step 4: RaftNode.propose() writes to the Raft log

```
broker1: RaftNode.propose()
   ├── LOCK acquired
   ├── state == LEADER ✓
   ├── index = raftLog.getLastIndex() + 1 = 6
   │
   ├── Build RaftEntry {
   │     term:      1,
   │     index:     6,
   │     topic:     "orders",
   │     payload:   [72, 101, ...]  ("Hello World" bytes)
   │     timestamp: 1742070865000
   │   }
   │
   ├── raftLog.append(entry)
   │       ├── Serialize → bytes
   │       ├── Write [0x00, 0x00, 0x00, 0x??][entry bytes] to raft.log
   │       └── fsync() → data is safely on disk
   │
   ├── pendingProposals[6] = new CompletableFuture<>()
   ├── LOCK released
   │
   ├── sendHeartbeats()  ← push to followers RIGHT NOW
   │
   └── future.get(5s)  ← THREAD BLOCKS HERE, waiting for majority
```

**Broker1's Raft log is now**:
```
Index: [1][2][3][4][5][6]
Terms: [1][1][1][1][1][1]
                       ▲
                   NEW ENTRY (not yet committed)
```

---

### Step 5: Broker1 replicates to broker2 and broker3

```
broker1: replicateTo(broker2)
   │
   ├── Build AppendEntriesRequest {
   │     term:         1,
   │     leaderId:     "broker1",
   │     prevLogIndex: 5,
   │     prevLogTerm:  1,
   │     entries:      [RaftEntry{index=6, topic="orders", ...}],
   │     leaderCommit: 5
   │   }
   │
   └── RaftPeer("broker2").sendAppendEntries(request)
           ├── ensureConnected() → TCP to broker2:9093
           ├── sendEnvelope(...)  → write to socket
           └── receiveEnvelope() → wait for response

broker1: replicateTo(broker3)  ← also happens, concurrently
   └── (same process, sends to broker3:9094)
```

**On the wire to broker2**:
```
[4-byte length][MessageEnvelope{
  type:    APPEND_ENTRIES_REQUEST (= 12)
  payload: <AppendEntriesRequest bytes>
}]
```

---

### Step 6: Broker2 handles AppendEntries

```
broker2: ClientHandler.run()  ← broker2's server port receives this
   └── handleMessage() → case APPEND_ENTRIES_REQUEST
           └── handleAppendEntriesRequest()
                   └── raftNode.handleAppendEntries(request)
```

```
broker2: RaftNode.handleAppendEntries(request)
   ├── request.term (1) == currentTerm (1) ✓
   ├── state = FOLLOWER, leaderId = "broker1"
   ├── resetElectionTimer() ← "leader is alive, don't start election"
   │
   ├── Log consistency check:
   │     prevLogIndex=5, prevLogTerm=1
   │     raftLog.getTermAt(5) == 1 ✓  (logs match up to index 5)
   │
   ├── Append entries:
   │     entry.index (6) > raftLog.getLastIndex() (5) → append!
   │     raftLog.append(RaftEntry{index=6, ...})
   │          └── write to broker2's raft.log file
   │          └── fsync()
   │
   ├── request.leaderCommit (5) == commitIndex (5) → no update yet
   │
   └── Return AppendEntriesResponse {
         term:       1,
         success:    true,
         matchIndex: 6    ← "I now have up to index 6"
       }
```

**Broker2's Raft log is now**:
```
Index: [1][2][3][4][5][6]
Terms: [1][1][1][1][1][1]
                       ▲
                   REPLICATED (waiting for leader to commit)
```

---

### Step 7: Broker1 learns broker2 has it → COMMIT!

```
broker1: replicateTo(broker2) receives response:
   AppendEntriesResponse {success=true, matchIndex=6}

   ├── matchIndex["broker2"] = 6
   ├── nextIndex["broker2"]  = 7
   │
   └── advanceCommitIndex()
           │
           ├── n = 6 (last log index)
           ├── log[6].term == currentTerm (1) ✓
           │
           ├── Count replicas with matchIndex >= 6:
           │     self:    1
           │     broker2: 2   ← matchIndex["broker2"] = 6 ≥ 6 ✓
           │     broker3: 2   (or 3 if broker3 also responded)
           │
           ├── majority = (3+1)/2 + 1 = 2
           ├── replicaCount=2 >= majority=2  →  COMMIT!
           │
           ├── commitIndex = 6
           │
           └── applyCommitted()
```

---

### Step 8: applyCommitted() — The message becomes real

```
broker1: RaftNode.applyCommitted()
   │
   ├── lastApplied (5) < commitIndex (6)
   ├── lastApplied = 6
   │
   ├── entry = raftLog.getEntry(6)
   │     → RaftEntry{topic="orders", payload=[72,101,...], ts=1742070865000}
   │
   ├── messageStore.append("orders", payload, null, 1742070865000)
   │       ├── offset = globalOffset.getAndIncrement()  → e.g., 42
   │       ├── Build StoredMessage{offset=42, topic="orders", payload=...}
   │       ├── Write to data/orders.log WAL file
   │       ├── topicIndex["orders"][42] = filePosition
   │       └── BoundedMessageCache["orders"].add(message)
   │           ← MESSAGE IS NOW VISIBLE TO CONSUMERS! ✓
   │
   └── pendingProposals[6].complete(6)
           └── Unblocks the thread that was waiting in propose()!
```

---

### Step 9: Producer gets its answer

```
broker1: propose() unblocks
   └── returns 6 (the Raft log index)

broker1: ClientHandler.handleProduceRequest()
   ├── offset = messageStore.getCurrentOffset() - 1  → 42
   │
   └── Send response to producer:
         MessageEnvelope {
           type:    PRODUCE_RESPONSE (= 2)
           payload: ProduceResponse{success=true, offset=42}
         }

DRMQProducer.send()
   └── returns SendResult{success=true, offset=42}
```

---

### Step 10: When broker3 also returns (confirmation)

Broker3 also processes the AppendEntries and returns its response. Broker1 updates:
```
matchIndex["broker3"] = 6
```

And on the NEXT heartbeat (75ms later), broker1 includes `leaderCommit=6` in the AppendEntriesRequest to broker3. Broker3 then applies the entry to its own `MessageStore`, making the message visible from ALL three brokers.

### Timeline Diagram

```
Time →     0ms      10ms      20ms      30ms      40ms
           │         │         │         │         │
Producer   │──send──▶│         │         │         │
           │         │         │         │         │
broker1    │ propose()         │         │         │
           │ log.append(6) ───▶│ result! │         │
           │ replicate ──────▶ │         │         │
           │         │         │         │         │
broker2    │         │ recv ───│         │         │
           │         │ log.append(6)     │         │
           │         │ respond ▶│        │         │
           │         │         │         │         │
broker1    │         │    advanceCommitIndex()      │
           │         │    apply to MessageStore     │
           │         │    future.complete()          │
           │         │         │         │         │
Producer   │         │         │ ◀─response(ok,42)─│
           │         │         │         │         │
broker3    │         │ recv ───│         │         │
           │         │ log.append(6)     │         │
           │         │         next heartbeat ─────│
           │         │         │ apply to           │
           │         │         │ MessageStore        │
```

---

## 10. Failure Scenario: What Happens When a Node Dies?

### Scenario: The Leader (broker1) crashes

```
Initial:     broker1 (LEADER), broker2 (FOLLOWER), broker3 (FOLLOWER)
t=0ms:       broker1 crashes! Power cut, OS killed, network gone.
t=0ms–150ms: broker2 and broker3 don't get heartbeats
t=~200ms:    broker2's election timer fires first (random!)
```

**broker2 starts an election**:
```
broker2: term++ → term=2
         state  = CANDIDATE
         votedFor = "broker2"

         → RequestVote{term=2, candidateId="broker2", lastLogIndex=6, lastLogTerm=1}
                    → to broker3 (broker1 is down, connection fails)
```

**broker3 votes for broker2**:
```
broker3: term (1) < request.term (2) → stepDown to term=2
         votedFor == null ✓
         broker2's log (index=6, term=1) is at least as up-to-date as mine ✓
         → voteGranted = true
```

**broker2 becomes leader**:
```
broker2: votes = 2 (self + broker3) >= majority (2)  → becomeLeader()
         state = LEADER, term=2
         Sends heartbeats to broker3 (broker1 still down)
```

**The cluster is operational again** with just 2 nodes. Producers can connect to broker2 and messages flow normally. When broker1 comes back online, it will:
1. Receive a heartbeat from broker2 with term=2
2. Its term (1) < term (2) → `stepDown(2)` → become FOLLOWER
3. Sync up any missing log entries from broker2

### Why does this work?

**Majority rule**: In a 3-node cluster, you need at least 2 nodes to agree. With 1 node down, the other 2 form a majority and keep working. The system can **tolerate `⌊N/2⌋` failures** for an N-node cluster:

| Cluster Size | Can Tolerate |
|---|---|
| 3 nodes | 1 failure |
| 5 nodes | 2 failures |
| 7 nodes | 3 failures |

### What about a "split-brain" (network partition)?

Imagine a network switch fails and broker1 can talk to no one, while broker2 and broker3 can talk to each other.

- broker2 and broker3 elect a new leader (majority of 2) → `term=2`
- broker1 thinks it's still leader at `term=1`

If a producer connects to broker1 and sends a message:
- broker1 calls `propose()`, tries to get majority → **can only get 1 vote (itself)**
- It never reaches majority → `propose()` times out after 5 seconds → **returns error**
- The producer does NOT get a success response
- No false "committed" messages!

Meanwhile, broker2 and broker3 continue working normally at term=2. When the network heals:
- broker1 gets a heartbeat from the new leader (broker2) with term=2
- broker1 sees term=2 > its own term=1 → `stepDown(2)` → follower
- Any uncommitted entries broker1 might have are overwritten by the new leader's log

---

## 11. Putting It All Together: The Full Data Flow

### Diagram: All components and connections

```
╔═══════════════════════════════════════════════════════════════════════╗
║                       Producer Application                           ║
║   DRMQProducer.send("orders", "Hello World")                         ║
╚═════════════════════════════════╤═════════════════════════════════════╝
                                  │  TCP connection (port 9092)
                                  │  MessageEnvelope{PRODUCE_REQUEST}
                                  ▼
╔═══════════════════════════════════════════════════════════════════════╗
║                         BROKER 1 (LEADER)                           ║
║                                                                       ║
║  ┌─────────────┐    ┌──────────────────────────────────────────────┐ ║
║  │BrokerServer │    │             ClientHandler                    │ ║
║  │             │──▶│  handleMessage()                             │ ║
║  │ServerSocket │    │  └─▶ handleProduceRequest()                 │ ║
║  │port 9092    │    │        └─▶ raftNode.propose(...)  ◀────────┐ │ ║
║  └─────────────┘    └──────────────────────────────────────────┐─┘ ║
║                                                                │   ║
║  ┌──────────────────────────────────────────────────────────┐  │   ║
║  │                       RaftNode                           │  │   ║
║  │                                                          │  │   ║
║  │  propose()                                               │  │   ║
║  │    ├─▶ raftLog.append(RaftEntry{index=6, ...})          │  │   ║
║  │    ├─▶ sendHeartbeats() ──────────────────────────────┐ │  │   ║
║  │    └─▶ future.get()  [BLOCKS]                         │ │  │   ║
║  │                                                       │ │  │   ║
║  │  advanceCommitIndex() ◀── responses from peers ───────┘ │  │   ║
║  │    └─▶ applyCommitted()                                  │  │   ║
║  │           ├─▶ messageStore.append(...)  ─────────────────┘  │   ║
║  │           └─▶ future.complete() ───────────────────────────▶│   ║
║  └──────────────────────────────────────────────────────────┘      ║
║                                                                       ║
║  ┌────────────┐    ┌─────────────────────────────────────────────┐  ║
║  │ RaftLog    │    │ MessageStore                                 │  ║
║  │ raft.log   │    │  ├─ topicIndex                              │  ║
║  │ (fsync'd)  │    │  ├─ messageCache                            │  ║
║  └────────────┘    │  └─ LogManager (WAL on disk)               │  ║
║                    └─────────────────────────────────────────────┘  ║
╚═════════════════════════════════╤═════════════════════════════════════╝
                  Raft RPC calls  │
        ┌─────────────────────────┴─────────────────────┐
        ▼                                               ▼
╔═══════════════════════╗                 ╔═══════════════════════╗
║   BROKER 2 (FOLLOWER) ║                 ║   BROKER 3 (FOLLOWER) ║
║                        ║                 ║                        ║
║   ClientHandler        ║                 ║   ClientHandler        ║
║     └─▶ RaftNode       ║                 ║     └─▶ RaftNode       ║
║          handleAE()    ║                 ║          handleAE()    ║
║          ├─ validate   ║                 ║          ├─ validate   ║
║          ├─ raftLog    ║                 ║          ├─ raftLog    ║
║          │  .append()  ║                 ║          │  .append()  ║
║          └─ respond✓   ║                 ║          └─ respond✓   ║
╚═══════════════════════╝                 ╚═══════════════════════╝
        ▲                                               ▲
        │  TCP connections via RaftPeer                 │
        └───────────────────────────────────────────────┘
```

### The Guarantee Summarized

| Event | What Raft Guarantees |
|---|---|
| Producer sends message | Returns success **only after** majority has written to disk |
| Leader crashes | New leader elected within ~300ms; no data lost if it was committed |
| Follower crashes | Cluster keeps going; recovers automatically when it restarts |
| Network partition | Minority partition cannot accept writes; no split-brain |
| Node restart | Loads term/votedFor from disk; replays Raft log; catches up from leader |

### State Machine Summary

```
Client Request
      │
      ▼
 Is this node
 the LEADER?
      │
   YES │                       NO
      ▼                        ▼
propose() →              Return NOT_LEADER
write to               + leader address
RaftLog                        │
      │                        │
      ▼                  Client reconnects
replicate to             to real leader
all peers
      │
      ▼
Got majority                Timed out
ACKs?                      (no majority)
      │                        │
   YES ▼                    NO ▼
commit entry →           Return error
apply to                 to client
MessageStore
      │
      ▼
Return success +
offset to client
```

---

## Summary

DRMQ is built in layers:

1. **Protocol Layer** (`messages.proto`) — defines the language every component speaks
2. **Storage Layer** (`MessageStore`, `RaftLog`) — durably persists data to disk with WAL
3. **Consensus Layer** (`RaftNode`, `RaftPeer`, `RaftLog`) — ensures all brokers agree on the same data, even through failures
4. **Network Layer** (`BrokerServer`, `ClientHandler`, `RaftPeer`) — handles TCP connections for both clients and peer brokers over the same port
5. **Client Layer** (`DRMQProducer`, `DRMQConsumer`) — the simple API for application developers, with automatic leader redirection

The key insight of Raft (and DRMQ) is: **a message is "committed" only when a majority of nodes have physically written it to disk**. This gives you a system where you can lose a minority of your servers and never lose a single confirmed message.

---

## 12. How to Run DRMQ

### Prerequisites

Before anything, make sure you have these installed:

| Tool | Version Required | Check with |
|---|---|---|
| **Java JDK** | 17 or higher | `java --version` |
| **Maven** | 3.8 or higher | `mvn --version` |

If `java --version` shows version 17+, you are good. If not, install [OpenJDK 17](https://adoptium.net/).

---

### Step 1: Build the Project

All four modules are built with a single command from the project root:

```bash
cd /home/samuel/Documents/DRMQ
mvn clean package -DskipTests
```

What this does:

```
mvn clean package
│
├── Cleans old build artifacts (target/ directories)
├── Compiles drmq-protocol → generates Java classes from messages.proto
├── Compiles drmq-broker
├── Compiles drmq-client
├── Compiles drmq-integration-tests
└── Packages each module into a .jar file:
      drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar
      drmq-client/target/drmq-client-1.0.0-SNAPSHOT.jar
```

Expected output (last few lines):
```
[INFO] BUILD SUCCESS
[INFO] Total time: ~15 seconds
```

> **Note**: The first build downloads dependencies from Maven Central (~50 MB). Subsequent builds are much faster.

---

### Step 2A: Run a Single-Node Broker

The simplest way to start — one broker, no Raft, no cluster:

```bash
cd drmq-broker
mvn exec:java
```

Or equivalently, passing arguments explicitly:

```bash
mvn exec:java -Dexec.args="--port 9092 --data-dir ./data"
```

Expected output:
```
INFO  BrokerServer - Single-node mode (no Raft)
INFO  MessageStore - Starting message store recovery...
INFO  MessageStore - Recovery complete. Global offset set to 0
INFO  BrokerServer - DRMQ Broker started on port 9092 with data directory ./data
```

The broker is now running and waiting for connections on **port 9092**. Your data is stored in `drmq-broker/data/`.

To stop it: press `Ctrl+C`.

---

### Step 2B: Run a 3-Node Cluster

Open **three separate terminal windows**. Run one command in each:

**Terminal 1 — Broker 1 (will become leader or follower)**:
```bash
cd /home/samuel/Documents/DRMQ/drmq-broker
mvn exec:java -Dexec.args="--id broker1 --port 9092 --peers broker2:localhost:9093,broker3:localhost:9094 --data-dir ./data-1"
```

**Terminal 2 — Broker 2**:
```bash
cd /home/samuel/Documents/DRMQ/drmq-broker
mvn exec:java -Dexec.args="--id broker2 --port 9093 --peers broker1:localhost:9092,broker3:localhost:9094 --data-dir ./data-2"
```

**Terminal 3 — Broker 3**:
```bash
cd /home/samuel/Documents/DRMQ/drmq-broker
mvn exec:java -Dexec.args="--id broker3 --port 9094 --peers broker1:localhost:9092,broker2:localhost:9093 --data-dir ./data-3"
```

Within ~300ms of starting all three, you'll see one of them print:
```
INFO  RaftNode - [broker1] ★ Became LEADER for term 1 (lastLogIndex=0)
```

And the other two:
```
INFO  RaftNode - [broker2] Raft node started (term=0, state=FOLLOWER, peers=2)
```

#### What the CLI flags mean

| Flag | Example | Meaning |
|---|---|---|
| `--id` | `broker1` | Unique name for this node in the cluster |
| `--port` | `9092` | TCP port this broker listens on |
| `--peers` | `broker2:localhost:9093,...` | Comma-separated list of ALL other nodes (not yourself) |
| `--data-dir` | `./data-1` | Folder where this broker stores its data and Raft log |

> **Important**: Each broker must have a **different `--data-dir`**. If two brokers share a data directory, they will conflict.

---

### Step 3: Use the Interactive Producer CLI

Open a **new terminal**. The Producer CLI lets you type messages interactively:

```bash
cd /home/samuel/Documents/DRMQ/drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp"
```

Expected startup:
```
╔════════════════════════════════════════╗
║     DRMQ Interactive Producer CLI     ║
╚════════════════════════════════════════╝

✓ Connected to broker at localhost:9092

Commands:
  send <topic> <message>  - Send a message to a topic
  exit                    - Quit the application
  help                    - Show this help
```

**Sending messages**:
```
producer> send orders Book Order #101
✓ Sent to [orders] at offset 0

producer> send orders Book Order #102
✓ Sent to [orders] at offset 1

producer> send payments Payment of $25.50
✓ Sent to [payments] at offset 2

producer> exit
✓ Goodbye!
```

The producer connects to `localhost:9092` by default. In cluster mode, it will **automatically redirect** to the real leader if it connects to a follower.

---

### Step 4: Use the Interactive Consumer CLI

Open another **new terminal**:

```bash
cd /home/samuel/Documents/DRMQ/drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp"
```

Or launch with a **custom consumer group name** (groups can be anything — they track your progress independently):

```bash
mvn exec:java \
  -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" \
  -Dexec.args="my-service"
```

Expected startup:
```
╔════════════════════════════════════════╗
║     DRMQ Interactive Consumer CLI     ║
╚════════════════════════════════════════╝
  Consumer Group: [my-service]

✓ Connected to broker at localhost:9092
✓ Offsets tracked by broker for group 'my-service'
```

**A typical consumer session**:
```
consumer[my-service]> subscribe orders
✓ Subscribed to [orders] (resuming from broker offset 0)

consumer[my-service]> poll
Long-polling (waiting up to 1000ms for messages)...
  Received 2 message(s):

  ┌─ [orders] Offset: 0
  │  Message: Book Order #101
  │  Timestamp: 1742070865000
  └─
  ┌─ [orders] Offset: 1
  │  Message: Book Order #102
  │  Timestamp: 1742070866000
  └─

  ✓ Offsets committed to broker automatically

consumer[my-service]> poll
Long-polling (waiting up to 1000ms for messages)...
  No new messages

consumer[my-service]> stream
📡 Streaming... (press Ctrl+C to stop)
  ┌─ [orders] Offset: 2     ← appears live when producer sends "Book Order #103"
  │  Message: Book Order #103
  └─
```

#### Consumer CLI commands summary

| Command | What it does |
|---|---|
| `subscribe <topic>` | Start reading a topic from where you last left off (offset stored on broker) |
| `subscribe <topic> <offset>` | Start reading from a specific offset (rewind or skip) |
| `poll` | Fetch up to 100 messages, wait up to 1 second for new ones |
| `poll <max> <timeout_ms>` | Customise both limits (e.g. `poll 50 5000`) |
| `poll 100 0` | Short poll — return immediately even if empty |
| `stream` | Continuous mode — auto-polls forever until Ctrl+C |
| `status` | Show current group info |
| `exit` | Disconnect and quit |

---

### Step 5: Writing Your Own Java Client

Add `drmq-client` as a Maven dependency in your app's `pom.xml`:

```xml
<dependency>
    <groupId>com.drmq</groupId>
    <artifactId>drmq-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

#### Producer example

```java
import com.drmq.client.DRMQProducer;

public class MyProducer {
    public static void main(String[] args) throws Exception {

        // Connect to any broker in the cluster (auto-redirects to leader)
        try (DRMQProducer producer = new DRMQProducer("localhost", 9092)) {
            producer.connect();

            // Send a plain string message
            var result = producer.send("orders", "New order: Widget x3");
            System.out.println("Sent at offset: " + result.getOffset());
            // Output: Sent at offset: 0

            // Send with a key (for future partitioning support)
            var result2 = producer.send("orders", "New order: Gadget x1".getBytes(), "user-42");
            System.out.println(result2); // SendResult{success=true, offset=1}
        }
        // try-with-resources auto-closes the connection
    }
}
```

#### Consumer example

```java
import com.drmq.client.DRMQConsumer;
import java.util.List;

public class MyConsumer {
    public static void main(String[] args) throws Exception {

        // "warehouse-service" is our consumer group name
        // Each unique group name independently tracks its own offsets
        try (DRMQConsumer consumer = new DRMQConsumer("warehouse-service")) {
            consumer.connect();

            // Subscribe — automatically resumes from where this group left off
            consumer.subscribe("orders");

            // Poll in a loop
            while (true) {
                // Wait up to 2000ms for new messages before returning empty
                List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(100, 2000);

                for (DRMQConsumer.ConsumedMessage msg : messages) {
                    System.out.printf("[offset=%d] %s%n", msg.offset(), msg.payloadAsString());
                    // Process your message here...
                }
                // Offsets are automatically committed to the broker after each poll()
            }
        }
    }
}
```

#### Connecting to a specific broker in the cluster

```java
// You can connect to ANY broker — even a follower
// The producer auto-redirects if it lands on a follower
DRMQProducer producer = new DRMQProducer("localhost", 9093);

// Consumers can read from any broker (data is replicated to all)
DRMQConsumer consumer = new DRMQConsumer("localhost", 9094, "my-group");
```

---

### Step 6: Run the Tests

#### Run ALL tests (unit + integration)

```bash
cd /home/samuel/Documents/DRMQ
mvn test
```

#### Run only the Raft integration tests

```bash
cd drmq-integration-tests
mvn test -Dtest=RaftIntegrationTest
```

#### Run a specific test by name

```bash
mvn test -Dtest="RaftIntegrationTest#threeNodesElectSingleLeader"
```

The integration test suite (`RaftIntegrationTest.java`) covers these scenarios automatically:

| Test | What it checks |
|---|---|
| `threeNodesElectSingleLeader` | Exactly 1 leader is elected from 3 nodes within 3 seconds |
| `produceViaLeaderReplicatesToAll` | A message sent to the leader appears in all 3 MessageStores |
| `followerRejectsWrite` | Sending to a follower triggers auto-redirect to the leader |
| `leaderFailoverElectsNewLeader` | Killing the leader causes a new one to be elected within 2 seconds |
| `newLeaderAcceptsWrites` | The new leader can still accept messages after failover |
| `singleNodeModeWorks` | A standalone broker (no Raft) works exactly as before |
| `consumerReadsFromFollower` | After replication, consumers can read from any node |

Expected output:
```
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

### Step 7: Simulating a Cluster Failure

This is the most impressive thing you can do to understand what Raft gives you. Run a 3-node cluster, then kill the leader mid-stream.

**Terminal 1, 2, 3**: Start the 3-node cluster as in Step 2B.

**Terminal 4**: Start the producer in stream/loop mode:
```bash
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp"
```
Send a few messages:
```
producer> send events msg-1
✓ Sent to [events] at offset 0
producer> send events msg-2
✓ Sent to [events] at offset 1
```

**Now simulate a crash**: Look at which terminal shows `★ Became LEADER`. Press `Ctrl+C` in that terminal to kill it.

Within ~300ms, one of the other two brokers will print:
```
INFO  RaftNode - [broker2] Starting election for term 2
INFO  RaftNode - [broker2] ★ Became LEADER for term 2 (lastLogIndex=2)
```

**Back in the producer terminal**, keep sending:
```
producer> send events msg-3
✓ Sent to [events] at offset 2
```

It just works — the producer automatically connects to the new leader. **No messages were lost.**

---

### Monitoring: What to Watch in the Logs

When running the cluster, the logs tell you exactly what Raft is doing:

| Log message | What it means |
|---|---|
| `★ Became LEADER for term N` | This node won the election |
| `Starting election for term N` | No heartbeat received, starting vote |
| `Granted vote to broker2 for term 2` | This node voted for another |
| `Stepping down: term 1 → 2` | Saw a higher term, becoming follower |
| `Advanced commitIndex to 6` | Entry 6 confirmed by majority |
| `Applied raft entry 6 to MessageStore` | Message is now visible to consumers |
| `AppendEntries to broker3 failed` | Network issue or broker3 is down |
| `Raft proposal timed out` | No majority reached within 5 seconds |

---

### Data Directory Structure

After running the cluster, each broker's `--data-dir` will look like:

```
data-1/
├── raft/
│   ├── raft.log        ← The Raft log (RaftEntry records, binary protobuf)
│   └── state.properties ← Persistent state: currentTerm + votedFor
│
├── orders.log          ← MessageStore WAL (Write-Ahead Log) for topic "orders"
├── payments.log        ← MessageStore WAL for topic "payments"
└── offsets.properties  ← Consumer group committed offsets
```

**`raft/raft.log`** — the most important file:
```
[4-byte length][RaftEntry protobuf: term=1, index=1, topic="orders", payload=...]
[4-byte length][RaftEntry protobuf: term=1, index=2, topic="orders", payload=...]
...
```

**`raft/state.properties`**:
```properties
currentTerm=2
votedFor=broker2
```

**`offsets.properties`**:
```properties
warehouse-service::orders=3
my-service::payments=1
```
(format: `<group>::<topic>=<nextOffsetToRead>`)

---

### Common Issues and Fixes

#### `Connection refused` when starting the producer

```
❌ Failed to connect to broker: Connection refused
Make sure the broker is running:
  cd drmq-broker && mvn exec:java
```

**Fix**: The broker isn't running. Start it first (Step 2A or 2B).

---

#### `Address already in use` when starting a broker

```
java.net.BindException: Address already in use
```

**Fix**: Something else is already using that port. Either:
- Kill the existing process: `kill $(lsof -t -i:9092)`
- Or use a different port: `--port 9095`

---

#### `Failed to execute mojo` during `mvn package`

If the protobuf code generation step fails:

```bash
# Install protoc manually
sudo apt-get install protobuf-compiler   # Ubuntu/Debian
brew install protobuf                     # macOS
```

Or skip code generation (if Java sources are already generated):
```bash
mvn package -DskipTests -Dprotoc.skip=true
```

---

#### Raft keeps restarting elections and never elects a leader

This usually means the brokers can't reach each other. Check:
1. All three brokers are actually running (check all three terminals)
2. The `--peers` flags are correct — each broker must list the OTHER two
3. No firewall is blocking ports 9092–9094: `telnet localhost 9093`

---

#### Old data causing issues on restart

If you restart a cluster and see unexpected behaviour, the old Raft log might be conflicting with new configuration. Reset by deleting the data directories:

```bash
# ⚠️ This deletes all stored messages!
rm -rf drmq-broker/data-1 drmq-broker/data-2 drmq-broker/data-3
```

---

### Quick-Start Cheat Sheet

```bash
# 1. Build everything
mvn clean package -DskipTests

# 2A. Single-node (simplest)
cd drmq-broker && mvn exec:java

# 2B. 3-node cluster (open 3 terminals)
mvn exec:java -Dexec.args="--id broker1 --port 9092 --peers broker2:localhost:9093,broker3:localhost:9094 --data-dir ./data-1"
mvn exec:java -Dexec.args="--id broker2 --port 9093 --peers broker1:localhost:9092,broker3:localhost:9094 --data-dir ./data-2"
mvn exec:java -Dexec.args="--id broker3 --port 9094 --peers broker1:localhost:9092,broker2:localhost:9093 --data-dir ./data-3"

# 3. Interactive Producer (new terminal)
cd drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp"

# 4. Interactive Consumer (new terminal)
cd drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp"

# 5. Run all tests
cd .. && mvn test

# 6. Run only Raft integration tests
cd drmq-integration-tests && mvn test -Dtest=RaftIntegrationTest
```
