import { useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { dracula } from 'react-syntax-highlighter/dist/esm/styles/prism';

// ─── Shared sub-components ─────────────────────────────────────────────────

function SectionHeader({ id, title }: { id: string; title: string }) {
  return (
    <h2 id={id} className="text-2xl font-bold text-white mt-16 mb-6 pt-4 border-t border-white/10 scroll-mt-8">
      {title}
    </h2>
  );
}

function SubHeader({ title }: { title: string }) {
  return <h3 className="text-lg font-semibold text-slate-200 mt-8 mb-3">{title}</h3>;
}

function P({ children, className }: { children: React.ReactNode; className?: string }) {
  return <p className={`text-slate-400 leading-relaxed mb-4 ${className || ''}`}>{children}</p>;
}


function CodeBlock({ lang, children }: { lang: string; children: string }) {
  // Map doc lang labels to Prism language identifiers
  const prismLang: Record<string, string> = {
    java: 'java', bash: 'bash', text: 'text', json: 'json', protobuf: 'protobuf',
  };
  return (
    <div className="rounded-lg overflow-hidden border border-white/10 my-4">
      <div className="flex items-center bg-[#0d1117] px-4 py-2 border-b border-white/5">
        <span className="text-xs font-mono text-zinc-500 uppercase tracking-widest">{lang}</span>
      </div>
      <SyntaxHighlighter
        language={prismLang[lang] ?? 'text'}
        style={dracula}
        customStyle={{ margin: 0, padding: '1rem', background: '#0d1117', fontSize: '0.85rem' }}
        wrapLongLines={false}
      >
        {children.trim()}
      </SyntaxHighlighter>
    </div>
  );
}

function InfoBox({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border border-cyan-500/20 bg-cyan-500/5 rounded-lg p-4 my-4">
      <div className="text-xs font-bold tracking-widest text-cyan-500 uppercase mb-2">{title}</div>
      <div className="text-sm text-slate-400 leading-relaxed">{children}</div>
    </div>
  );
}

function WarnBox({ children }: { children: React.ReactNode }) {
  return (
    <div className="border border-amber-500/20 bg-amber-500/5 rounded-lg p-4 my-4">
      <div className="text-xs font-bold tracking-widest text-amber-500 uppercase mb-2">Important</div>
      <div className="text-sm text-slate-400 leading-relaxed">{children}</div>
    </div>
  );
}

// ─── Table of Contents ─────────────────────────────────────────────────────

const TOC = [
  { id: 'introduction',   label: '1. Introduction' },
  { id: 'architecture',   label: '2. Architecture' },
  { id: 'quickstart',     label: '3. Getting Started' },
  { id: 'configuration',  label: '4. Configuration' },
  { id: 'raft',           label: '5. Raft Consensus' },
  { id: 'producer',       label: '6. Producer API' },
  { id: 'consumer',       label: '7. Consumer API' },
  { id: 'storage',        label: '8. Storage Engine' },
  { id: 'groups',         label: '9. Consumer Groups' },
  { id: 'telemetry',      label: '10. Telemetry' },
  { id: 'production',     label: '11. Production' },
  { id: 'faults',         label: '12. Fault Tolerance' },
  { id: 'cli',            label: '13. CLI Reference' },
];

// ─── Main Page ─────────────────────────────────────────────────────────────

export default function Documentation() {
  const [activeId, setActiveId] = useState('introduction');

  return (
    <div className="bg-[#0a0a0b] text-slate-300 min-h-full">
      <div className="max-w-7xl mx-auto flex gap-0">

        {/* Sticky ToC */}
        <aside className="hidden xl:block w-64 shrink-0">
          <div className="sticky top-0 h-screen overflow-y-auto py-10 px-6 border-r border-white/5">
            <p className="text-xs font-bold tracking-widest text-zinc-600 uppercase mb-4">On This Page</p>
            <nav className="flex flex-col gap-1">
              {TOC.map(({ id, label }) => (
                <a
                  key={id}
                  href={`#${id}`}
                  onClick={() => setActiveId(id)}
                  className={`text-sm py-1 px-2 rounded transition-colors ${
                    activeId === id
                      ? 'text-cyan-400 bg-cyan-500/10'
                      : 'text-zinc-500 hover:text-slate-300 hover:bg-white/5'
                  }`}
                >
                  {label}
                </a>
              ))}
            </nav>
          </div>
        </aside>

        {/* Content */}
        <main className="flex-1 min-w-0 py-10 px-8 lg:px-12">

          {/* Page Header */}
          <div className="mb-12">
            <div className="inline-block text-xs font-mono tracking-widest text-cyan-500 border border-cyan-500/30 bg-cyan-500/10 rounded px-3 py-1 mb-4">
              DRMQ v1.0
            </div>
            <h1 className="text-4xl font-bold text-white mb-4">Documentation</h1>
            <p className="text-lg text-slate-400 max-w-3xl">
              Complete technical reference for the Distributed Reliable Message Queue — a Raft-based,
              append-only event streaming system with linearizable guarantees and a custom storage engine.
            </p>
          </div>

          {/* ── Section 1: Introduction ─────────────────────────────────── */}
          <SectionHeader id="introduction" title="1. Introduction" />
          <P>
            DRMQ (Distributed Reliable Message Queue) is a from-scratch implementation of a distributed
            event streaming broker built on the Raft consensus algorithm. It is designed for workloads
            that demand strict, globally ordered message delivery with durability guarantees.
          </P>
          <P>
            Unlike partition-based models, DRMQ replicates the entire topic log across all nodes
            in the Raft cluster. This sacrifices horizontal write scalability in exchange for
            much simpler operational semantics — every message is linearizable, every consumer sees the
            same global order, and there is no concept of partition reassignment or consumer rebalancing.
          </P>

          <SubHeader title="When to use DRMQ" />
          <div className="bg-white/3 border border-white/5 rounded-lg divide-y divide-white/5 my-4 overflow-hidden">
            {[
              ['Strictly ordered event logs',     'Audit trails, state-machine replication.'],
              ['Small-to-medium throughput',       'Single-partition write path means writes serialize through the Raft leader.'],
              ['Simple operational model',         'No partition maps, no ISR, no consumer group rebalancing protocol.'],
              ['High availability with 3+ nodes', 'Survives minority node failures with automatic leader failover.'],
            ].map(([title, desc]) => (
              <div key={title} className="flex gap-4 px-5 py-3 text-sm">
                <span className="text-white font-medium w-52 shrink-0">{title}</span>
                <span className="text-slate-500">{desc}</span>
              </div>
            ))}
          </div>

          <InfoBox title="Key Guarantee">
            A message is only acknowledged to the producer after a <strong>quorum (majority)</strong> of
            Raft nodes have durably written it to their local log. A single node failure cannot cause
            data loss for any acknowledged message.
          </InfoBox>

          {/* ── Section 2: Architecture ─────────────────────────────────── */}
          <SectionHeader id="architecture" title="2. Architecture" />
          <P>
            DRMQ is structured in three independent layers that interact through well-defined internal
            interfaces. Understanding these boundaries is critical for operational and debugging work.
          </P>

          <SubHeader title="The Three Layers" />
          <div className="space-y-3 my-4">
            {[
              {
                name: 'Protocol Layer',
                color: 'cyan',
                desc: `All client and peer communication uses a single Netty-based TCP server. Messages are 
                       framed with a 4-byte length prefix and encoded as Protobuf binary. Client requests 
                       (ProduceRequest, ConsumeRequest, CommitOffsetRequest) and Raft RPCs 
                       (AppendEntries, RequestVote, InstallSnapshot) share the same transport. The Netty 
                       LengthFieldBasedFrameDecoder is configured with a 256MB maximum frame size to 
                       accommodate large snapshots.`,
              },
              {
                name: 'Consensus Layer',
                color: 'emerald',
                desc: `RaftNode implements the full Raft protocol: leader election with Pre-Vote, log 
                       replication with batched AppendEntries (capped at MAX_ENTRIES_PER_RPC = 500 entries), 
                       InstallSnapshot for severely lagging followers, and durable persistence of currentTerm, 
                       votedFor, commitIndex, and lastApplied across restarts. The consensus layer is the 
                       gatekeeper — no write reaches the storage layer without first being committed by a quorum.`,
              },
              {
                name: 'Storage Layer',
                color: 'purple',
                desc: `MessageStore manages the on-disk topic data using a segmented, append-only log. 
                       Each topic is a directory of .log files (100MB each) with corresponding .idx sparse 
                       index files. The RaftLog itself uses a separate binary-encoded file to persist Raft 
                       log entries. Consumer group offsets are also persisted inside the Raft log as 
                       CommitOffsetCommand entries, giving them the same durability guarantee as messages.`,
              },
            ].map(({ name, color, desc }) => (
              <div key={name} className={`border border-${color}-500/20 bg-${color}-500/5 rounded-lg p-5`}>
                <div className={`text-sm font-bold text-${color}-400 mb-2`}>{name}</div>
                <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
              </div>
            ))}
          </div>

          <SubHeader title="Request lifecycle: Producer write" />
          <div className="my-6 space-y-0">
            {[
              { step: '1', actor: 'Client', bg: 'bg-slate-800', border: 'border-slate-700', desc: 'Sends a ProduceRequest (topic + payload bytes) over TCP to any broker node.' },
              { step: '2', actor: 'Broker — not leader', bg: 'bg-zinc-900', border: 'border-zinc-700', desc: 'If the receiving broker is a follower, it immediately responds NOT_LEADER:<addr>. The client SDK transparently redirects to the leader.' },
              { step: '3', actor: 'Leader — RaftNode', bg: 'bg-cyan-950', border: 'border-cyan-800', desc: 'Appends the entry to its local RaftLog, then fires parallel AppendEntries RPCs to all followers. The request is held open.' },
              { step: '4', actor: 'Followers — RaftNode', bg: 'bg-zinc-900', border: 'border-zinc-700', desc: 'Each follower writes the entry to its local log and replies AppendEntriesResponse(success=true).' },
              { step: '5', actor: 'Leader — Quorum reached', bg: 'bg-cyan-950', border: 'border-cyan-800', desc: 'Once a majority of nodes have acknowledged, the Leader advances its commitIndex, applies the entry to the MessageStore, and assigns a monotonically increasing global offset.' },
              { step: '6', actor: 'Client', bg: 'bg-emerald-950', border: 'border-emerald-800', desc: 'Receives ProduceResponse(success=true, offset=N). The message is now durable and visible to consumers.' },
            ].map(({ step, actor, bg, border, desc }) => (
              <div key={step} className="flex gap-0">
                <div className="flex flex-col items-center mr-4 shrink-0">
                  <div className="w-8 h-8 rounded-full bg-zinc-800 border border-white/10 flex items-center justify-center text-xs font-bold text-slate-300">{step}</div>
                  <div className="w-px flex-1 bg-white/5 my-1" />
                </div>
                <div className={`flex-1 rounded-lg border ${border} ${bg} p-4 mb-2`}>
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">{actor}</div>
                  <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
                </div>
              </div>
            ))}
          </div>

          {/* ── Section 3: Getting Started ──────────────────────────────── */}
          <SectionHeader id="quickstart" title="3. Getting Started" />

          <SubHeader title="Prerequisites" />
          <div className="bg-white/3 border border-white/5 rounded-lg divide-y divide-white/5 my-4 overflow-hidden">
            {[
              ['Java', '17+', 'The broker and client are written in Java 17.'],
              ['Maven', '3.8+', 'Used to build all modules.'],
              ['Node.js', '18+', 'Required only for the dashboard (optional).'],
            ].map(([dep, ver, note]) => (
              <div key={dep as string} className="flex gap-4 px-5 py-3 text-sm">
                <span className="text-cyan-400 font-mono w-20 shrink-0">{dep}</span>
                <span className="text-white w-16 shrink-0">{ver}</span>
                <span className="text-slate-500">{note}</span>
              </div>
            ))}
          </div>

          <SubHeader title="Build" />
          <P>Clone the repository and build all Maven modules from the project root:</P>
          <CodeBlock lang="bash">
{`git clone https://github.com/your-org/drmq.git
cd drmq

# Build all modules (broker + client + protocol)
mvn clean install -DskipTests`}
          </CodeBlock>

          <SubHeader title="Start a single-node broker" />
          <P>
            The fastest way to get running. The broker starts on port 9092 and stores data in{' '}
            <code className="text-cyan-400 text-sm">./data</code> by default.
          </P>
          <CodeBlock lang="bash">
{`cd drmq-broker

# Default: node-id=1, port=9092, data-dir=./data, no peers (standalone)
mvn exec:java`}
          </CodeBlock>

          <SubHeader title="Start a 3-node cluster (local)" />
          <P>
            Open three separate terminals. Each node must know the addresses of its peers.
            The cluster will elect a leader once a quorum (2 of 3) establishes connectivity.
          </P>
          <CodeBlock lang="bash">
{`# Terminal 1 — Node 1
cd drmq-broker
mvn exec:java -Dexec.args="1 9092 ./data/node1 localhost:9093,localhost:9094"

# Terminal 2 — Node 2
cd drmq-broker
mvn exec:java -Dexec.args="2 9093 ./data/node2 localhost:9092,localhost:9094"

# Terminal 3 — Node 3
cd drmq-broker
mvn exec:java -Dexec.args="3 9094 ./data/node3 localhost:9092,localhost:9093"`}
          </CodeBlock>

          <WarnBox>
            Peers must be specified as a comma-separated list of <code>host:port</code> pairs
            excluding the current node's own address. Providing the node's own address in the peer
            list is harmless but generates a connection warning on startup.
          </WarnBox>

          <SubHeader title="Verify the cluster" />
          <P>
            Watch the logs. Within 2-3 seconds you should see one node win the election and print:
          </P>
          <CodeBlock lang="text">
{`[raft-timer] INFO  RaftNode - [1] Became LEADER for term 1
[raft-timer] INFO  RaftNode - [1] Sending heartbeats to 2 peers`}
          </CodeBlock>

          <SubHeader title="Start the dashboard" />
          <CodeBlock lang="bash">
{`cd drmq-dashboard
npm install

# Connect to all three broker nodes
VITE_USE_WEBSOCKET=true npm run dev`}
          </CodeBlock>
          <P>
            Open <code className="text-cyan-400 text-sm">http://localhost:5173</code> in your browser.
            The dashboard connects to all broker nodes simultaneously via WebSocket and merges their
            telemetry into a single unified view.
          </P>

          {/* ── Section 4: Configuration ────────────────────────────────── */}
          <SectionHeader id="configuration" title="4. Broker Configuration Reference" />
          <P>
            The broker is configured entirely through command-line arguments. There is no configuration
            file; all settings are passed as explicit flags at startup.
          </P>

          <SubHeader title="Startup arguments" />
          <div className="rounded-lg border border-white/10 overflow-hidden my-4">
            <table className="w-full text-sm text-left">
              <thead className="bg-white/5 text-white border-b border-white/10">
                <tr>
                  <th className="px-5 py-3 font-medium w-36">Argument</th>
                  <th className="px-5 py-3 font-medium w-28">Type</th>
                  <th className="px-5 py-3 font-medium w-24">Default</th>
                  <th className="px-5 py-3 font-medium">Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {[
                  ['node-id',   'String',  'Required', 'Unique identifier for this broker within the Raft cluster. Must be stable across restarts — changing it will cause the node to be treated as a new, unknown peer.'],
                  ['port',      'Integer', '9092',     'TCP port on which the Netty server listens for both client connections and inbound Raft RPC traffic from peers.'],
                  ['data-dir',  'String',  './data',   'Root directory for all persistent state. DRMQ creates subdirectories here for raft/ (log + metadata) and store/ (topic segments + indexes). Point this at an NVMe-backed path in production.'],
                  ['peers',     'String',  'none',     'Comma-separated list of peer addresses in host:port format, e.g. localhost:9093,localhost:9094. Omit the node\'s own address. Empty means standalone (single-node) mode with no replication.'],
                ].map(([arg, type, def, desc]) => (
                  <tr key={arg} className="hover:bg-white/2 transition-colors">
                    <td className="px-5 py-3 font-mono text-cyan-400 align-top">{arg}</td>
                    <td className="px-5 py-3 text-slate-500 align-top font-mono text-xs">{type}</td>
                    <td className="px-5 py-3 text-zinc-500 align-top">{def}</td>
                    <td className="px-5 py-3 text-slate-400 align-top leading-relaxed">{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <SubHeader title="Internal constants" />
          <P>
            The following values are compiled into the broker. They are not runtime-configurable in
            the current version but are documented here for operational awareness.
          </P>
          <div className="rounded-lg border border-white/10 overflow-hidden my-4">
            <table className="w-full text-sm text-left">
              <thead className="bg-white/5 text-white border-b border-white/10">
                <tr>
                  <th className="px-5 py-3 font-medium w-64">Constant</th>
                  <th className="px-5 py-3 font-medium w-28">Value</th>
                  <th className="px-5 py-3 font-medium">Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {[
                  ['MAX_FRAME_SIZE',       '256 MB',  'Maximum Netty frame size. Governs the largest single RPC payload — relevant during snapshot transfer.'],
                  ['MAX_ENTRIES_PER_RPC',  '500',     'Maximum Raft log entries sent in a single AppendEntries call. Prevents OOM during follower catch-up after a long partition.'],
                  ['MAX_SEGMENT_SIZE',     '100 MB',  'Size at which an active .log segment is sealed and a new one is rolled. Older segments become candidates for compaction.'],
                  ['ELECTION_TIMEOUT',     '150–300 ms', 'Randomised election timeout range (per node). A follower that receives no heartbeat within this window initiates a Pre-Vote round. The randomisation (150ms floor, 300ms ceiling) prevents split-votes by ensuring nodes rarely time out simultaneously.'],
                  ['HEARTBEAT_INTERVAL',   '75 ms',      'How often the Leader sends AppendEntries heartbeats to reset follower timers. Must be well below the election timeout minimum (150ms) to prevent spurious elections under normal operation.'],
                  ['RECONNECT_DELAY_MS',   '500 ms',  'Client SDK: pause between broker failover attempts to allow leader election to stabilise.'],
                  ['MAX_RETRIES',          '5',       'Client SDK: maximum retries per operation across the full set of bootstrap servers before throwing IOException.'],
                  ['TELEMETRY_WS_PORT',    '9093',    'WebSocket port on each broker node that streams telemetry JSON frames to the dashboard.'],
                ].map(([name, val, desc]) => (
                  <tr key={name} className="hover:bg-white/2 transition-colors">
                    <td className="px-5 py-3 font-mono text-amber-400 align-top text-xs">{name}</td>
                    <td className="px-5 py-3 text-white align-top font-mono text-xs">{val}</td>
                    <td className="px-5 py-3 text-slate-400 align-top leading-relaxed">{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <SubHeader title="Example: production 3-node cluster" />
          <CodeBlock lang="bash">
{`# Node 1 — 10.0.1.10
java -server \\
  -Xms4g -Xmx4g \\
  -XX:+UseG1GC \\
  -XX:MaxGCPauseMillis=20 \\
  -jar drmq-broker.jar \\
  1 9092 /mnt/nvme/drmq/node1 10.0.1.11:9092,10.0.1.12:9092

# Node 2 — 10.0.1.11
java -server \\
  -Xms4g -Xmx4g \\
  -XX:+UseG1GC \\
  -XX:MaxGCPauseMillis=20 \\
  -jar drmq-broker.jar \\
  2 9092 /mnt/nvme/drmq/node2 10.0.1.10:9092,10.0.1.12:9092

# Node 3 — 10.0.1.12
java -server \\
  -Xms4g -Xmx4g \\
  -XX:+UseG1GC \\
  -XX:MaxGCPauseMillis=20 \\
  -jar drmq-broker.jar \\
  3 9092 /mnt/nvme/drmq/node3 10.0.1.10:9092,10.0.1.11:9092`}
          </CodeBlock>

          {/* ── Section 5: Raft Consensus ───────────────────────────────── */}
          <SectionHeader id="raft" title="5. Raft Consensus" />
          <P>
            DRMQ implements the Raft consensus algorithm as described in the original{' '}
            <em>In Search of an Understandable Consensus Algorithm</em> paper (Ongaro &amp; Ousterhout, 2014),
            extended with the Pre-Vote mechanism from the follow-up dissertation. The implementation
            lives entirely in <code className="text-cyan-400 text-sm">RaftNode.java</code>.
          </P>

          <SubHeader title="Node states" />
          <div className="grid grid-cols-3 gap-3 my-4">
            {[
              { state: 'FOLLOWER',  color: 'zinc',    desc: 'Default state on startup. Passively replicates entries from the leader. Runs an election timer; if no heartbeat is received before timeout, transitions to Pre-Candidate.' },
              { state: 'CANDIDATE', color: 'amber',   desc: 'Actively seeking votes. Sends RequestVote RPCs to all peers. Transitions to LEADER on quorum, or back to FOLLOWER if a higher term is observed.' },
              { state: 'LEADER',    color: 'cyan',    desc: 'Accepts all write requests. Broadcasts AppendEntries RPCs on every heartbeat interval and immediately after a new log entry is appended. There is exactly one leader per term.' },
            ].map(({ state, color, desc }) => (
              <div key={state} className={`border border-${color}-500/20 bg-${color}-500/5 rounded-lg p-4`}>
                <div className={`text-xs font-bold tracking-widest text-${color}-400 mb-2`}>{state}</div>
                <p className="text-xs text-slate-500 leading-relaxed">{desc}</p>
              </div>
            ))}
          </div>

          <SubHeader title="Leader election and the Pre-Vote extension" />
          <P>
            A standard Raft follower increments its term and requests votes the moment its election
            timer fires. This is safe but can cause unnecessary term inflation when a partitioned node
            reconnects — it may have a term far ahead of the cluster, forcing a brief but disruptive
            leader re-election even though it has stale log data.
          </P>
          <P>
            DRMQ uses the <strong className="text-white">Pre-Vote</strong> extension to prevent this.
            When a follower's timer expires it sends a{' '}
            <code className="text-cyan-400 text-sm">PreVoteRequest</code> without incrementing its
            term. A peer grants a pre-vote only if the requester's log is at least as up-to-date
            as the peer's own log. Only after receiving pre-votes from a majority does the node
            increment its term and send real{' '}
            <code className="text-cyan-400 text-sm">RequestVote</code> RPCs. A lagging node that
            has missed thousands of entries will be denied pre-votes and remain a follower, silently
            catching up without disrupting the cluster.
          </P>

          <SubHeader title="Log replication" />
          <P>
            Every <code className="text-cyan-400 text-sm">ProduceRequest</code> is converted into a
            Raft log entry and appended to the leader's local <code className="text-cyan-400 text-sm">RaftLog</code>.
            The leader then replicates it via <code className="text-cyan-400 text-sm">AppendEntries</code> RPCs.
            Key implementation details:
          </P>
          <div className="space-y-2 my-4">
            {[
              ['Batching during catch-up', 'When a follower is lagging, the leader batches up to MAX_ENTRIES_PER_RPC (500) entries per RPC to bound per-message memory overhead and prevent frame-size exceptions on the Netty transport.'],
              ['prevLogIndex / prevLogTerm check', 'Every AppendEntries carries the index and term of the entry immediately before the batch. The follower rejects the RPC if its local log does not match — this enforces the Log Matching Property.'],
              ['Monotonic commit advancement', 'The leader advances commitIndex only after a majority of matchIndex values meet or exceed the new entry\'s index. commitIndex never decreases.'],
              ['Apply loop', 'A dedicated thread watches commitIndex. Whenever commitIndex > lastApplied, entries are applied to the MessageStore in strict order and lastApplied is incremented. This is the point at which messages become readable by consumers.'],
            ].map(([title, desc]) => (
              <div key={title} className="flex gap-4 border border-white/5 rounded-lg px-5 py-3 bg-white/2">
                <span className="text-white text-sm font-medium w-64 shrink-0 leading-relaxed">{title}</span>
                <span className="text-slate-500 text-sm leading-relaxed">{desc}</span>
              </div>
            ))}
          </div>

          <SubHeader title="Log Compaction and Follower Catch-up" />
          <P>
            The Raft log grows continuously as messages are appended. To prevent unbounded disk usage,
            DRMQ compacts the <code className="text-cyan-400 text-sm">RaftLog</code> by truncating
            entries that have already been applied to the <code className="text-cyan-400 text-sm">MessageStore</code>.
            This is done by rewriting the <code className="text-cyan-400 text-sm">raft.log</code> file to remove
            older entries and updating the <code className="text-cyan-400 text-sm">startIndex</code>.
          </P>
          <InfoBox title="Zero-Downtime Snapshot Installation">
            <strong>State Transfer:</strong> When a follower falls behind and its required entries have been compacted from the leader's <code className="text-cyan-400 text-sm">RaftLog</code>, the leader initiates an <code className="text-cyan-400 text-sm">InstallSnapshotRequest</code> stream. The leader dynamically zips its persistent <code className="text-cyan-400 text-sm">MessageStore</code> and <code className="text-cyan-400 text-sm">OffsetManager</code> data into a single archive, transmitting it in 2MB chunks over the network. Upon receiving the final chunk, the follower safely and atomically hot-swaps its current storage directories with the snapshot contents, completely recovering its topic and offset states without needing a JVM restart.
          </InfoBox>

          <SubHeader title="Persistence across restarts" />
          <P>
            The following state is written to disk before any RPC response is sent, ensuring correctness
            after a crash:
          </P>
          <div className="rounded-lg border border-white/10 overflow-hidden my-4">
            <table className="w-full text-sm text-left">
              <thead className="bg-white/5 text-white border-b border-white/10">
                <tr>
                  <th className="px-5 py-3 font-medium">Field</th>
                  <th className="px-5 py-3 font-medium">Where stored</th>
                  <th className="px-5 py-3 font-medium">Why it must survive a crash</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {[
                  ['currentTerm',  'raft/state.properties', 'Prevents a restarted node from accepting RPCs from a leader in an older term.'],
                  ['votedFor',     'raft/state.properties', 'Ensures a node never grants two votes in the same term (Safety Property).'],
                  ['log entries',  'raft/raft.log',         'The source of truth for uncommitted writes. Entries committed but not yet snapshotted must survive to be re-applied.'],
                  ['lastApplied',  'raft/state.properties', 'Restored on restart and used to reconstruct commitIndex via Math.min(lastLogIndex, max(commitIndex, lastApplied)). Also used to set RaftLog.startIndex if the live log is empty after a snapshot.'],
                  ['commitIndex',  '(derived)',              'Not directly persisted. On startup it is reconstructed from lastApplied and the last index in the live Raft log, preventing re-application of already-stored messages.'],
                ].map(([field, loc, why]) => (
                  <tr key={field} className="hover:bg-white/2 transition-colors">
                    <td className="px-5 py-3 font-mono text-cyan-400 align-top text-xs">{field}</td>
                    <td className="px-5 py-3 text-zinc-500 align-top font-mono text-xs">{loc}</td>
                    <td className="px-5 py-3 text-slate-400 align-top leading-relaxed">{why}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* ── Section 6: Producer API ─────────────────────────────────── */}
          <SectionHeader id="producer" title="6. Producer API" />
          <P>
            The <code className="text-cyan-400 text-sm">DRMQProducer</code> is a thread-safe client for sending messages to the cluster.
            It uses synchronous sends with an underlying blocking TCP socket, meaning <code className="text-cyan-400 text-sm">send()</code>
            blocks until the message is either acknowledged by a Raft quorum or an unrecoverable error occurs.
          </P>

          <SubHeader title="Initialisation and Failover" />
          <P>
            Producers should be initialised with a list of bootstrap servers. The producer connects to a random
            server from the list. If it connects to a Follower, the Follower immediately responds with a
            <code className="text-cyan-400 text-sm">NOT_LEADER</code> error containing the current Leader's address.
            The producer transparently redirects to the Leader. If the Leader crashes, the producer will
            cycle through the bootstrap servers, waiting for a new Leader to be elected.
          </P>
          <CodeBlock lang="java">
{`import com.drmq.client.DRMQProducer;

// Initialize with a comma-separated list of bootstrap servers
DRMQProducer producer = new DRMQProducer("10.0.1.10:9092,10.0.1.11:9092,10.0.1.12:9092");

// Connect to the cluster
producer.connect();`}
          </CodeBlock>

          <SubHeader title="Sending Messages" />
          <P>
            Messages can be sent as raw byte arrays or as UTF-8 strings. You can optionally attach a key
            to the message (useful for downstream grouping/partitioning logic, though DRMQ enforces global
            ordering regardless of the key).
          </P>
          <CodeBlock lang="java">
{`// Send a simple string payload
DRMQProducer.SendResult result = producer.send("orders", "Order #1234");

if (result.isSuccess()) {
    System.out.println("Message committed at offset: " + result.getOffset());
} else {
    System.err.println("Failed to send: " + result.getErrorMessage());
}

// Send raw bytes with an optional key
byte[] payload = serialize(myObject);
DRMQProducer.SendResult result = producer.send("metrics", payload, "sensor-01");`}
          </CodeBlock>
          <WarnBox>
            The <code className="text-cyan-400 text-sm">send()</code> method automatically retries up to{' '}
            <code className="text-cyan-400 text-sm">MAX_RETRIES = 5</code> times across the bootstrap servers
            if connection errors or leader elections occur during the send. It only throws an{' '}
            <code className="text-cyan-400 text-sm">IOException</code> if all retries are exhausted.
          </WarnBox>

          {/* ── Section 7: Consumer API ─────────────────────────────────── */}
          <SectionHeader id="consumer" title="7. Consumer API" />
          <P>
            The <code className="text-cyan-400 text-sm">DRMQConsumer</code> is used to read messages from topics.
            Unlike Kafka, DRMQ supports two distinct consumption modes: <strong>Group Mode</strong> (default)
            and <strong>Single Mode</strong>.
          </P>

          <SubHeader title="Consumption Modes" />
          <div className="grid grid-cols-2 gap-4 my-4">
            {[
              { mode: 'Group Mode (Default)', desc: 'The broker tracks the consumer\'s offsets persistently via Raft. Multiple consumers in the same group coordinate via short-lived leases, ensuring a message is delivered to exactly one consumer in the group.' },
              { mode: 'Single Mode', desc: 'The consumer tracks its own offsets locally. It sends raw offset-based requests to the broker. Useful for replay tools, ad-hoc scripts, or systems that persist offsets in their own external database.' },
            ].map(({ mode, desc }) => (
              <div key={mode} className={`border border-white/10 bg-white/5 rounded-lg p-5`}>
                <div className={`text-sm font-bold text-blue-400 mb-2`}>{mode}</div>
                <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
              </div>
            ))}
          </div>

          <SubHeader title="Basic Usage (Group Mode)" />
          <P>
            In group mode, the consumer asks the broker where it left off upon subscription. Auto-commit
            is disabled by default, meaning you must manually commit offsets after processing, or explicitly
            enable auto-commit.
          </P>
          <CodeBlock lang="java">
{`import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQConsumer.ConsumedMessage;

// Initialize with bootstrap servers and consumer group ID
DRMQConsumer consumer = new DRMQConsumer("10.0.1.10:9092,10.0.1.11:9092,10.0.1.12:9092", "analytics-group");
consumer.connect();

// Let the broker manage the offset
consumer.subscribe("orders");

// Optional: Enable auto-commit after every poll
consumer.setAutoCommit(true);

while (true) {
    // Poll max 100 messages. Wait up to 2000ms if queue is empty (Long Polling)
    List<ConsumedMessage> messages = consumer.poll(100, 2000);

    for (ConsumedMessage msg : messages) {
        System.out.printf("Offset: %d, Key: %s, Data: %s%n", 
            msg.offset(), msg.key(), msg.payloadAsString());
    }
}`}
          </CodeBlock>

          <SubHeader title="Manual Offset Management" />
          <P>
            To achieve <strong>at-least-once</strong> processing semantics, you must disable auto-commit, process the
            messages fully, and only then commit the offset. You can also explicitly seek to a specific offset.
          </P>
          <CodeBlock lang="java">
{`consumer.setAutoCommit(false);

// Override the broker's offset and explicitly resume from offset 500
consumer.subscribe("orders", 500L);

List<ConsumedMessage> messages = consumer.poll(50, 1000);
for (ConsumedMessage msg : messages) {
    processInDatabase(msg);
}

// Manually commit the offset to the broker
if (!messages.isEmpty()) {
    long lastOffset = messages.get(messages.size() - 1).offset();
    consumer.commit("orders", lastOffset);
}`}
          </CodeBlock>

          <SubHeader title="Single Mode (No Group Coordination)" />
          <P>
            If you want a consumer to simply read from a topic independently without the broker tracking
            leases or preventing other consumers from reading the same messages, disable group mode.
          </P>
          <CodeBlock lang="java">
{`DRMQConsumer singleConsumer = new DRMQConsumer("10.0.1.10:9092,10.0.1.11:9092");

// Disable group coordination
singleConsumer.setGroupMode(false);
singleConsumer.connect();

// Provide an explicit starting offset, as the broker won't track it for you
singleConsumer.subscribe("system-logs", 0L);

while (true) {
    List<ConsumedMessage> logs = singleConsumer.poll(500);
    for (ConsumedMessage log : logs) {
        System.out.println(log.payloadAsString());
    }
}`}
          </CodeBlock>

          <SubHeader title="Long Polling vs Short Polling" />
          <P>
            The <code className="text-cyan-400 text-sm">poll(maxMessages, timeoutMs)</code> method
            determines the polling behaviour based on the <code className="text-cyan-400 text-sm">timeoutMs</code>:
          </P>
          <ul className="list-disc list-inside text-slate-400 space-y-2 my-4 text-sm">
            <li>
              <strong>timeoutMs = 0 (Short Poll):</strong> The broker checks the log and returns immediately,
              even if there are no new messages. Useful for non-blocking UI threads or background checks.
            </li>
            <li>
              <strong>timeoutMs &gt; 0 (Long Poll):</strong> The broker holds the TCP request open for up to
              <code className="text-cyan-400 text-sm">timeoutMs</code>. If a new message is appended to the
              Raft log by a Producer, the broker instantly wakes up the connection and pushes the message.
              This reduces CPU overhead drastically compared to busy-waiting short polls.
            </li>
          </ul>

          {/* ── Section 8: Storage Engine ───────────────────────────────── */}
          <SectionHeader id="storage" title="8. Storage Engine" />
          <P>
            Once a message is committed by the Raft consensus layer, it is handed off to the{' '}
            <code className="text-cyan-400 text-sm">MessageStore</code>. The storage layer uses a
            segmented, append-only log design optimized for high-throughput sequential writes and
            efficient sequential reads.
          </P>

          <SubHeader title="Log Segments and Sparse Indexing" />
          <P>
            Messages for a topic are stored in a dedicated directory (e.g., <code className="text-cyan-400 text-sm">./data/store/topics/orders/</code>).
            Instead of writing to a single infinitely growing file, the log is split into segments of
            up to <code className="text-cyan-400 text-sm">100 MB</code>.
          </P>
          <P>Each segment is stored as a single data file:</P>
          <ul className="list-disc list-inside text-slate-400 space-y-2 my-4 text-sm">
            <li>
              <strong>Data File (<code className="text-cyan-400">00000000000000000000.log</code>):</strong> Contains the raw serialized
              message payloads along with a binary header (length prefix).
            </li>
          </ul>
          <P>
            To achieve <code className="text-cyan-400 text-sm">O(1)</code> random access for consumers, DRMQ builds a 
            <strong>sparse index in memory</strong> (<code className="text-cyan-400 text-sm">ConcurrentSkipListMap</code>) 
            during broker startup. There are no on-disk <code className="text-cyan-400 text-sm">.idx</code> files. 
            When a consumer requests a specific offset, the broker binary-searches the in-memory index to find the 
            nearest byte boundary before performing a short linear scan on disk.
          </P>

          {/* ── Section 9: Consumer Groups ──────────────────────────────── */}
          <SectionHeader id="groups" title="9. Consumer Groups" />
          <P>
            DRMQ uses a server-coordinated consumer group model to provide exact load-balancing without the
            need for complex client-side partition rebalancing protocols (like ZooKeeper or Kafka's GroupCoordinator).
          </P>

          <SubHeader title="Lease-based Coordination" />
          <P>
            Because DRMQ has no partitions (every topic is a single linear log), multiple consumers in a group
            cannot simply lock different partitions. Instead, DRMQ uses a <strong>message-level lease system</strong>:
          </P>
          <div className="my-6 space-y-0">
            {[
              { step: '1', title: 'Poll Request', desc: 'A consumer in the group requests a batch of messages.' },
              { step: '2', title: 'Lease Grant', desc: 'The broker identifies the next uncommitted, unleased offset. It grants a 30-second lease to the consumer for a batch of messages.' },
              { step: '3', title: 'Processing', desc: 'While the lease is active, no other consumer in the group will be handed those specific offsets.' },
              { step: '4', title: 'Commit or Expire', desc: 'If the consumer commits the offsets within the timeout, the broker marks them permanently processed. If the lease expires or the client disconnects, the broker invalidates the lease and makes the messages available to the next polling consumer.' },
            ].map(({ step, title, desc }) => (
              <div key={step} className="flex gap-0">
                <div className="flex flex-col items-center mr-4 shrink-0">
                  <div className="w-8 h-8 rounded-full bg-blue-900 border border-white/10 flex items-center justify-center text-xs font-bold text-slate-300">{step}</div>
                  <div className="w-px flex-1 bg-white/5 my-1" />
                </div>
                <div className={`flex-1 rounded-lg border border-white/5 bg-white/5 p-4 mb-2`}>
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">{title}</div>
                  <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
                </div>
              </div>
            ))}
          </div>

          <SubHeader title="Offset Durability" />
          <P>
            Consumer group offsets must be as resilient as the messages themselves. When a consumer commits
            an offset, the broker generates an internal <code className="text-cyan-400 text-sm">CommitOffsetCommand</code>.
            This command is appended to the <strong>Raft log</strong> and replicated across the quorum just
            like a standard producer message. If the Leader crashes, the new Leader replays the Raft log
            to reconstruct the exact state of all consumer groups.
          </P>

          {/* ── Section 10: Telemetry ───────────────────────────────────── */}
          <SectionHeader id="telemetry" title="10. Telemetry & Dashboard" />
          <P>
            Every DRMQ broker runs an embedded WebSocket server (port <code className="text-cyan-400 text-sm">9093</code> by default)
            that streams real-time JSON telemetry frames.
          </P>
          <div className="grid grid-cols-2 gap-4 my-4">
            {[
              { title: 'Cluster Metrics', desc: 'Current term, leader identity, commit index, and last applied index.' },
              { title: 'Throughput', desc: 'Messages written/sec, messages read/sec, and active socket connections.' },
              { title: 'Storage Health', desc: 'Active segments, disk usage bytes, and compaction occurrences.' },
              { title: 'Raft Timers', desc: 'Election duration, heartbeat latency, and replication lag per follower.' },
            ].map(({ title, desc }) => (
              <div key={title} className="border border-white/5 bg-[#0d1117] rounded-lg p-4">
                <div className="text-sm font-bold text-emerald-400 mb-2">{title}</div>
                <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
              </div>
            ))}
          </div>
          <P>
            The React-based Dashboard connects directly to all nodes in the cluster simultaneously.
            It aggregates the WebSocket streams on the client side, allowing you to instantly visualize
            network partitions, leader failovers, and follower lag without relying on external metric scrapers like Prometheus.
          </P>

          {/* ── Section 11: Production Deployment ───────────────────────── */}
          <SectionHeader id="production" title="11. Production Deployment" />
          <P>
            While DRMQ runs easily on a laptop for development, running a distributed consensus-based 
            system in production requires specific hardware and OS-level considerations to guarantee 
            durability and performance.
          </P>

          <SubHeader title="Hardware Recommendations" />
          <div className="grid grid-cols-2 gap-4 my-4">
            {[
              { component: 'Storage (NVMe SSD)', desc: 'Crucial. DRMQ is an append-only system that requires fast fsyncs. Spinning HDDs or slow cloud EBS volumes will cause high Raft commit latency, slowing down producers.' },
              { component: 'Memory (RAM)', desc: 'DRMQ relies heavily on the Linux Page Cache. Allocate 4-8GB to the JVM, but leave the majority of the system RAM to the OS for caching .log files.' },
              { component: 'CPU', desc: 'At least 4 cores. Raft RPC handling, message decoding, and telemetry serialization run on separate thread pools to avoid blocking the consensus heartbeat.' },
              { component: 'Network', desc: '1Gbps+ isolated subnet. Because Raft replicates the full stream to all nodes, write throughput is bounded by the network bandwidth between the Leader and Followers.' },
            ].map(({ component, desc }) => (
              <div key={component} className="border border-white/5 bg-white/5 rounded-lg p-5">
                <div className="text-sm font-bold text-cyan-400 mb-2">{component}</div>
                <p className="text-sm text-slate-400 leading-relaxed">{desc}</p>
              </div>
            ))}
          </div>

          <SubHeader title="OS and JVM Tuning" />
          <ul className="list-disc list-inside text-slate-400 space-y-2 my-4 text-sm">
            <li>
              <strong>File Descriptors:</strong> Increase <code className="text-cyan-400">ulimit -n</code> to at least 100,000. 
              The broker holds open file descriptors for every topic segment and every active client connection.
            </li>
            <li>
              <strong>Swappiness:</strong> Set <code className="text-cyan-400">vm.swappiness = 1</code>. You do not want the 
              OS swapping the JVM heap to disk, as this will cause unpredictable GC pauses that trigger false Raft elections.
            </li>
            <li>
              <strong>Garbage Collection:</strong> Use <code className="text-cyan-400">-XX:+UseG1GC</code> with a low pause 
              target (e.g., <code className="text-cyan-400">-XX:MaxGCPauseMillis=20</code>) so GC pauses don't exceed the 
              75ms Raft heartbeat interval.
            </li>
          </ul>

          {/* ── Section 12: Fault Tolerance ───────────────────────────── */}
          <SectionHeader id="faults" title="12. Fault Tolerance" />
          <P>
            DRMQ is built to survive failures gracefully. The following scenarios describe how the cluster 
            behaves under duress.
          </P>
          <div className="space-y-4 my-4">
            {[
              ['Follower Crash', 'The cluster continues operating normally. The Leader logs warnings that the follower is unreachable. When the follower restarts, the Leader automatically sends it the missing entries.'],
              ['Leader Crash', 'Write operations temporarily block. Within 150-300ms, the remaining nodes notice the lack of heartbeats. One initiates an election, wins a quorum, and becomes the new Leader. Clients transparently reconnect.'],
              ['Minority Network Partition', 'If 1 node in a 3-node cluster loses network access, the other 2 nodes form a quorum and continue processing. The isolated node cannot elect itself (it cannot reach a quorum) and refuses writes. When the partition heals, the Pre-Vote mechanism prevents the isolated node from disrupting the active Leader.'],
              ['Majority Network Partition', 'If 2 out of 3 nodes crash, the remaining node steps down to FOLLOWER. All produce requests will fail (or block, depending on client configuration) because a quorum cannot be reached to commit writes. This guarantees consistency over availability (CP system).'],
            ].map(([scenario, result]) => (
              <div key={scenario} className="border-l-2 border-amber-500 bg-amber-500/5 pl-4 py-3 pr-4 rounded-r-lg">
                <div className="text-sm font-bold text-white mb-1">{scenario}</div>
                <div className="text-sm text-slate-400">{result}</div>
              </div>
            ))}
          </div>

          {/* ── Section 13: CLI Reference ─────────────────────────────── */}
          <SectionHeader id="cli" title="13. CLI Reference" />
          <P>
            The <code className="text-cyan-400 text-sm">drmq-client</code> module includes interactive REPL
            (Read-Eval-Print Loop) applications for testing clusters without writing Java code.
          </P>

          <SubHeader title="ProducerApp" />
          <P>Starts an interactive prompt to send messages to any topic.</P>
          <CodeBlock lang="bash">
{`# Usage: mvn exec:java -Dexec.mainClass="...ProducerApp" -Dexec.args="[bootstrap_servers]"
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp" \\
  -Dexec.args="10.0.1.10:9092,10.0.1.11:9092,10.0.1.12:9092"`}
          </CodeBlock>
          <P className="mt-2 text-sm">Inside the REPL:</P>
          <CodeBlock lang="text">
{`producer> send orders Book Order #101
✓ Message sent: topic=orders, offset=42

producer> send alerts System is ONLINE
✓ Message sent: topic=alerts, offset=43`}
          </CodeBlock>

          <SubHeader title="ConsumerApp" />
          <P>Starts an interactive prompt to subscribe, poll, and manage auto-commit.</P>
          <CodeBlock lang="bash">
{`# Usage: mvn exec:java -Dexec.mainClass="...ConsumerApp" -Dexec.args="[bootstrap_servers] [group_id]"
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" \\
  -Dexec.args="10.0.1.10:9092,10.0.1.11:9092,10.0.1.12:9092 analytics-group"`}
          </CodeBlock>
          <P className="mt-2 text-sm">Inside the REPL:</P>
          <CodeBlock lang="text">
{`consumer[analytics-group]> subscribe orders
✓ Subscribed to [orders] (resuming from broker offset 42)

consumer[analytics-group]> poll
[offset=42, key=null, time=14:30:22] Book Order #101

consumer[analytics-group]> commit orders 42
✓ Committed offset 42 for topic 'orders'`}
          </CodeBlock>

        </main>
      </div>
    </div>
  );
}
