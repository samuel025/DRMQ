import type { TelemetryProvider, TelemetryCallback, TelemetryState, BrokerNode, ClusterMetrics, Latencies } from '../../types/telemetry';

/**
 * Connects to ALL broker nodes simultaneously.
 *
 * Each broker knows its own metrics best (especially the leader, which has all
 * the produce/consume traffic). This provider fans out to every URL, collects
 * each broker's telemetry frame, and merges them into a single coherent state:
 *
 * - Node list  : one entry per unique broker ID, preferring data from that
 *                broker's own connection (most accurate self-knowledge).
 * - Metrics    : taken from whichever broker currently believes itself to be
 *                LEADER, since that is where all client traffic flows.
 * - Latencies  : from the leader connection.
 *
 * This means animations, rates and health are always correct regardless of
 * which node holds leadership.
 */
export class WebSocketTelemetryProvider implements TelemetryProvider {
  private urls: string[];
  private sockets: Map<string, WebSocket | null> = new Map();
  private reconnectTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private latestFrames: Map<string, TelemetryState> = new Map();
  private onDataCallback: TelemetryCallback | null = null;
  private onErrorCallback?: import('../../types/telemetry').TelemetryErrorCallback;
  private stopped = false;

  // Fixed screen positions for up to 3 nodes, by sorted index
  private static readonly POSITIONS = [
    { x: 500, y: 200 },  // top-centre  (usually leader in steady state)
    { x: 200, y: 500 },  // bottom-left
    { x: 800, y: 500 },  // bottom-right
  ];

  constructor(urls: string[]) {
    this.urls = urls;
  }

  connect(onData: TelemetryCallback, onError?: import('../../types/telemetry').TelemetryErrorCallback): void {
    this.stopped = false;
    this.onDataCallback = onData;
    this.onErrorCallback = onError;
    for (const url of this.urls) {
      this.openSocket(url);
    }
    setTimeout(() => {
      if (!this.stopped && this.onErrorCallback && Array.from(this.sockets.values()).every(s => !s || s.readyState !== WebSocket.OPEN)) {
        this.onErrorCallback('Connection timeout. Check if brokers are running.');
      }
    }, 5000);
  }

  disconnect(): void {
    this.stopped = true;
    for (const timer of this.reconnectTimers.values()) clearTimeout(timer);
    this.reconnectTimers.clear();
    for (const ws of this.sockets.values()) ws?.close();
    this.sockets.clear();
  }

  // ── private ────────────────────────────────────────────────────────────────

  private openSocket(url: string): void {
    if (this.stopped) return;
    console.log(`[DRMQ] Connecting to ${url}…`);
    try {
      const ws = new WebSocket(url);
      this.sockets.set(url, ws);

      ws.onopen = () => console.log(`[DRMQ] Connected: ${url}`);

      ws.onmessage = (event) => {
        if (!this.onDataCallback) return;
        try {
          const frame = JSON.parse(event.data) as TelemetryState;
          this.latestFrames.set(url, frame);
          this.onDataCallback(this.merge());
        } catch (e) {
          console.error('[DRMQ] Failed to parse telemetry', e);
        }
      };

      ws.onerror = () => console.warn(`[DRMQ] Socket error on ${url}`);

      ws.onclose = () => {
        console.log(`[DRMQ] Disconnected: ${url}`);
        this.sockets.set(url, null);
        this.latestFrames.delete(url);
        if (!this.stopped && this.onErrorCallback && Array.from(this.sockets.values()).every(s => !s || s.readyState !== WebSocket.OPEN)) {
          this.onErrorCallback('Cluster offline. Retrying connection...');
        }
        this.scheduleReconnect(url);
      };
    } catch (e) {
      console.error(`[DRMQ] Cannot create WebSocket for ${url}`, e);
      this.scheduleReconnect(url);
    }
  }

  private scheduleReconnect(url: string): void {
    if (this.stopped) return;
    const timer = setTimeout(() => this.openSocket(url), 3000);
    this.reconnectTimers.set(url, timer);
  }

  /**
   * Backfill any history arrays the broker doesn't send yet.
   * This prevents the dashboard from crashing on undefined.slice().
   */
  private static normalizeMetrics(m: ClusterMetrics): ClusterMetrics {
    return {
      ...m,
      throughputHistory: m.throughputHistory ?? [],
      consumeHistory:    m.consumeHistory    ?? [],
      errorHistory:      m.errorHistory      ?? [],
    };
  }

  /**
   * Merge all latest frames into one coherent TelemetryState.
   *
   * Key invariants:
   *  - Metrics (throughput, activeProducers, activeConsumers) come ONLY from
   *    the leader frame. Followers don't receive client traffic, so their
   *    counts are meaningless for the cluster-level view.
   *  - Node list is a union of all frames, with each broker's self-report
   *    preferred for its own entry (most accurate self-knowledge).
   *  - followerSync uses a fixed scale: 0 lag = 100%, ≥1000 lag = 0%.
   */
  private merge(): TelemetryState {
    const frames = Array.from(this.latestFrames.values());
    if (frames.length === 0) {
      return {
        nodes: [],
        metrics: {
          totalThroughputMB: 0, produceThroughputMB: 0, consumeThroughputMB: 0,
          produceRate: 0, consumeRate: 0, errorRate: 0, produceLatencyMs: 0, consumeLatencyMs: 0,
          activeProducers: 0, activeConsumers: 0, totalConnections: 0,
          health: 'CRITICAL', term: 0, commitIndex: 0, lastApplied: 0, followerSync: 0,
          globalOffset: 0, topicCount: 0, logSegments: 0, cachedMessages: 0,
          throughputHistory: [], consumeHistory: [], errorHistory: []
        },
        latencies: { alphaBeta: 0, betaGamma: 0, raftRpcMs: 0 },
        events: [],
      };
    }
    if (frames.length === 1) {
      return {
        ...frames[0],
        metrics: WebSocketTelemetryProvider.normalizeMetrics(frames[0].metrics),
        events: frames[0].events ?? [],
      };
    }

    // Find the most authoritative frame (leader with highest term)
    const maxTerm = Math.max(...frames.map(f => f.metrics.term ?? 0));
    const leaderFrame = frames.find(f => f.nodes[0]?.status === 'LEADER' && f.metrics.term === maxTerm)
                     ?? frames.find(f => f.nodes[0]?.status === 'LEADER')
                     ?? frames[0];

    // ── 1. Build unified node list ────────────────────────────────────────────
    const nodeMap = new Map<string, BrokerNode>();

    // First pass: use the authoritative (leader) frame to set baseline for all nodes.
    // This gives us the leader's replicationLag computation for each follower.
    for (const node of leaderFrame.nodes) {
      nodeMap.set(node.id, { ...node });
    }

    // Second pass: add any nodes that only appear in non-leader frames
    for (const frame of frames) {
      for (const node of frame.nodes) {
        if (!nodeMap.has(node.id)) nodeMap.set(node.id, { ...node });
      }
    }

    // Third pass: overwrite with each broker's own self-report for status
    // accuracy, BUT merge with the leader's data to preserve replicationLag.
    // Downgrade stale leaders to followers to prevent multi-leader ghosting.
    for (const frame of frames) {
      const local = frame.nodes[0];
      if (local) {
        const existingNode = nodeMap.get(local.id) || {};
        const isStaleLeader = local.status === 'LEADER' && (frame.metrics.term ?? 0) < maxTerm;
        // Spread existing first (preserves leader-computed replicationLag),
        // then overlay with local's self-report (more accurate status/throughput)
        const nodeToStore = isStaleLeader
          ? { ...existingNode, ...local, status: 'FOLLOWER' as const, color: '#a855f7' }
          : { ...existingNode, ...local };
        nodeMap.set(local.id, nodeToStore as BrokerNode);
      }
    }

    // Sort by id and pin to fixed SVG positions
    const sorted = Array.from(nodeMap.values()).sort((a, b) => a.id.localeCompare(b.id));
    const nodes: BrokerNode[] = sorted.map((node, i) => ({
      ...node,
      ...WebSocketTelemetryProvider.POSITIONS[i] ?? { x: 500, y: 500 },
    }));

    // ── 2. Metrics: exclusively from the leader frame ─────────────────────────
    // This prevents doubling of activeProducers/activeConsumers when multiple
    // surviving brokers each report their own Netty connection count.
    const metrics: ClusterMetrics = WebSocketTelemetryProvider.normalizeMetrics(leaderFrame.metrics);

    // Recompute followerSync with a FIXED scale (not proportional to commitIndex).
    // Scale: 0 lag = 100%, 1000+ lag = 0%.
    // Skip unreachable followers (replicationLag=-1 or commitIndex=0 when leader is far ahead).
    const LAG_FULL_SCALE = 1000;
    const leaderCommit = metrics.commitIndex;
    if (leaderCommit > 0 && nodes.length > 1) {
      const reachableFollowers = nodes.filter(n =>
        n.status !== 'LEADER' &&
        (n.replicationLag === undefined || n.replicationLag >= 0)
      );
      if (reachableFollowers.length > 0) {
        const maxLag = Math.max(...reachableFollowers.map(n => Math.max(0, leaderCommit - (n.commitIndex ?? 0))));
        metrics.followerSync = Math.max(0, Math.min(100,
          Math.round(100 - (maxLag / LAG_FULL_SCALE) * 100)
        ));
      } else {
        // All followers are unreachable — show 0% sync
        const hasUnreachable = nodes.some(n => n.status !== 'LEADER' && (n.replicationLag !== undefined && n.replicationLag < 0));
        metrics.followerSync = hasUnreachable ? 0 : 100;
      }
    }

    // ── 3. Latencies from leader ─────────────────────────────────────────────
    const latencies: Latencies = leaderFrame.latencies;

    // ── 4. Merge events from all frames, deduplicate by id ───────────────────
    const eventMap = new Map<string, any>();
    for (const frame of frames) {
      for (const evt of (frame.events ?? [])) {
        eventMap.set(evt.id, evt);
      }
    }
    const events = Array.from(eventMap.values())
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, 50);

    return { nodes, metrics, latencies, events };
  }
}
