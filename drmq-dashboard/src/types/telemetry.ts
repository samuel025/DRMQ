export interface BrokerNode {
  id: string;
  name: string;
  status: 'LEADER' | 'FOLLOWER' | 'CANDIDATE' | 'OFFLINE';
  throughputMBps: number;    // real MB/s this node handled (leader only)
  produceRate: number;       // msgs/s produced (leader only)
  consumeRate: number;       // msgs/s consumed (leader only)
  commitIndex: number;       // last known commit index (or matchIndex for followers)
  lastApplied: number;
  replicationLag?: number;   // entries behind leader commit (followers only)
  color: string;
  x: number;
  y: number;
}

export interface ClusterMetrics {
  // Throughput
  totalThroughputMB: number;
  produceThroughputMB: number;
  consumeThroughputMB: number;
  produceRate: number;        // msgs/s
  consumeRate: number;        // msgs/s
  errorRate: number;          // errors/s

  // Latency (real timer means in ms)
  produceLatencyMs: number;
  consumeLatencyMs: number;

  // Connections
  activeProducers: number;
  activeConsumers: number;
  totalConnections: number;

  // Cluster state
  health: 'OPTIMAL' | 'DEGRADED' | 'CRITICAL';
  term: number;
  commitIndex: number;
  lastApplied: number;
  followerSync: number;       // 0-100 real replication sync %

  // Storage
  globalOffset: number;
  topicCount: number;
  logSegments: number;
  cachedMessages: number;

  // Chart
  throughputHistory: number[]; // 0-100 scaled, 30 points
}

export interface Latencies {
  alphaBeta: number;    // ms
  betaGamma: number;   // ms
  raftRpcMs: number;   // real Raft RPC mean latency
}

export interface ClusterEvent {
  id: string;
  timestamp: number;
  type: 'REPLICATION' | 'ELECTION' | 'ERROR' | 'SNAPSHOT' | 'CONNECTION';
  message: string;
  nodeId?: string;
  severity: 'info' | 'warn' | 'error';
}

export interface TelemetryState {
  nodes: BrokerNode[];
  metrics: ClusterMetrics;
  latencies: Latencies;
  events: ClusterEvent[];
}

export type TelemetryCallback = (state: TelemetryState) => void;

export type TelemetryErrorCallback = (error: string) => void;

export interface TelemetryProvider {
  connect(onData: TelemetryCallback, onError?: TelemetryErrorCallback): void;
  disconnect(): void;
}
