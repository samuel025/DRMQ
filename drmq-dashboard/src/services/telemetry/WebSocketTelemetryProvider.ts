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
   * Merge all latest frames into one coherent TelemetryState.
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
          globalOffset: 0, topicCount: 0, logSegments: 0, cachedMessages: 0, throughputHistory: []
        },
        latencies: { alphaBeta: 0, betaGamma: 0, raftRpcMs: 0 }
      };
    }
    if (frames.length === 1) return frames[0];

    // Find the most authoritative frame (leader with highest term)
    const maxTerm = Math.max(...frames.map(f => f.metrics.term ?? 0));
    const leaderFrame = frames.find(f => f.nodes[0]?.status === 'LEADER' && f.metrics.term === maxTerm)
                     ?? frames.find(f => f.nodes[0]?.status === 'LEADER')
                     ?? frames[0];

    const nodeMap = new Map<string, BrokerNode>();

    // First pass: use the authoritative frame to set the baseline for all nodes
    for (const node of leaderFrame.nodes) {
      nodeMap.set(node.id, { ...node });
    }

    // Second pass: add any missing nodes from other frames (just in case)
    for (const frame of frames) {
      for (const node of frame.nodes) {
        if (!nodeMap.has(node.id)) nodeMap.set(node.id, { ...node });
      }
    }

    // Third pass: overwrite with each broker's own self-report, BUT
    // downgrade stale leaders to followers to prevent multi-leader ghosting.
    for (const frame of frames) {
      const local = frame.nodes[0];
      if (local) {
        const isStaleLeader = local.status === 'LEADER' && (frame.metrics.term ?? 0) < maxTerm;
        const nodeToStore = isStaleLeader 
          ? { ...local, status: 'FOLLOWER' as const, color: '#a855f7' } 
          : local;
        nodeMap.set(local.id, nodeToStore);
      }
    }

    // Sort by id and pin to fixed SVG positions
    const sorted = Array.from(nodeMap.values()).sort((a, b) => a.id.localeCompare(b.id));
    const nodes: BrokerNode[] = sorted.map((node, i) => ({
      ...node,
      ...WebSocketTelemetryProvider.POSITIONS[i] ?? { x: 500, y: 500 },
    }));

    const metrics: ClusterMetrics = { ...leaderFrame.metrics };

    // If we have more frames, compute the real follower sync from merged node
    // matchIndexes vs the leader commit index.
    const leaderCommit = metrics.commitIndex;
    if (leaderCommit > 0 && nodes.length > 1) {
      const followers = nodes.filter(n => n.status !== 'LEADER');
      if (followers.length > 0) {
        const maxLag = Math.max(...followers.map(n => leaderCommit - (n.commitIndex ?? 0)));
        metrics.followerSync = Math.max(0, Math.min(100,
          Math.round(100 - (maxLag / leaderCommit) * 100)
        ));
      }
    }

    // ── 3. Latencies from leader ─────────────────────────────────────────────
    const latencies: Latencies = leaderFrame.latencies;

    return { nodes, metrics, latencies };
  }
}
