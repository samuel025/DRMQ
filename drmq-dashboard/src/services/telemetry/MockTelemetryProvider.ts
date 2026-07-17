import type { TelemetryProvider, TelemetryCallback, TelemetryState, ClusterEvent } from '../../types/telemetry';

const EVENT_TEMPLATES: Array<{ type: ClusterEvent['type']; severity: ClusterEvent['severity']; messages: string[] }> = [
  { type: 'REPLICATION', severity: 'info', messages: [
    'AppendEntries replicated to {node}',
    '{node} caught up to commit index',
    'Batch replicated 128 entries to {node}',
  ]},
  { type: 'ELECTION', severity: 'warn', messages: [
    'Heartbeat timeout — {node} starting election',
    '{node} voted for Broker-1 in term {term}',
    'Leader re-elected in term {term}',
  ]},
  { type: 'SNAPSHOT', severity: 'info', messages: [
    'Snapshot triggered at index {offset}',
    '{node} installed snapshot successfully',
    'Log compacted — removed 3 segments',
  ]},
  { type: 'CONNECTION', severity: 'info', messages: [
    'Producer connected from 192.168.1.{ip}',
    'Consumer group "analytics" joined',
    'Client disconnected gracefully',
  ]},
  { type: 'ERROR', severity: 'error', messages: [
    'Produce timeout on partition 0',
    'Replication stalled to {node}',
  ]},
];

export class MockTelemetryProvider implements TelemetryProvider {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private state: TelemetryState;
  private tick = 0;
  private eventCounter = 0;

  constructor() {
    this.state = {
      nodes: [
        { id: 'broker1', name: 'Broker-1', status: 'LEADER',   throughputMBps: 2.4, produceRate: 4200, consumeRate: 3800, commitIndex: 104832, lastApplied: 104831, color: '#06b6d4', x: 500, y: 200 },
        { id: 'broker2', name: 'Broker-2', status: 'FOLLOWER', throughputMBps: 0,   produceRate: 0,    consumeRate: 0,    commitIndex: 104820, lastApplied: 104820, replicationLag: 12, color: '#a855f7', x: 200, y: 500 },
        { id: 'broker3', name: 'Broker-3', status: 'FOLLOWER', throughputMBps: 0,   produceRate: 0,    consumeRate: 0,    commitIndex: 104830, lastApplied: 104830, replicationLag: 2,  color: '#a855f7', x: 800, y: 500 },
      ],
      metrics: {
        totalThroughputMB:   2.4,
        produceThroughputMB: 1.5,
        consumeThroughputMB: 0.9,
        produceRate:         4200,
        consumeRate:         3800,
        errorRate:           0,
        produceLatencyMs:    1.2,
        consumeLatencyMs:    0.8,
        activeProducers:     6,
        activeConsumers:     9,
        totalConnections:    18,
        health:             'OPTIMAL',
        term:               12,
        commitIndex:        104832,
        lastApplied:        104831,
        followerSync:        97,
        globalOffset:       104832,
        topicCount:          3,
        logSegments:         7,
        cachedMessages:     3000,
        throughputHistory: Array.from({ length: 30 }, (_, i) =>
          Math.max(5, 50 + Math.sin(i / 4) * 30 + Math.random() * 10)
        ),
        consumeHistory: Array.from({ length: 30 }, (_, i) =>
          Math.min(100, Math.max(3, ((0.9 + Math.sin(i / 4 + 1) * 0.4 + Math.random() * 0.2) / 50) * 100))
        ),
        errorHistory: Array.from({ length: 30 }, () => 0),
      },
      latencies: { alphaBeta: 3.2, betaGamma: 2.8, raftRpcMs: 3.2 },
      events: [],
    };
  }

  connect(onData: TelemetryCallback, _onError?: (error: string) => void): void {
    // Seed a few initial events
    for (let i = 0; i < 5; i++) {
      this.state.events.push(this.generateEvent());
    }
    onData(this.state);
    this.intervalId = setInterval(() => {
      this.simulateTick();
      onData({ ...this.state, metrics: { ...this.state.metrics }, events: [...this.state.events] });
    }, 1000);
  }

  disconnect(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  private generateEvent(): ClusterEvent {
    const template = EVENT_TEMPLATES[Math.floor(Math.random() * EVENT_TEMPLATES.length)];
    const msg = template.messages[Math.floor(Math.random() * template.messages.length)];
    const nodes = ['Broker-2', 'Broker-3'];
    const node = nodes[Math.floor(Math.random() * nodes.length)];
    const formatted = msg
      .replace('{node}', node)
      .replace('{term}', String(this.state.metrics.term))
      .replace('{offset}', String(this.state.metrics.globalOffset))
      .replace('{ip}', String(Math.floor(Math.random() * 254) + 1));

    return {
      id: `evt-${++this.eventCounter}`,
      timestamp: Date.now() - Math.floor(Math.random() * 2000),
      type: template.type,
      severity: template.severity,
      message: formatted,
      nodeId: node.toLowerCase().replace('-', ''),
    };
  }

  private simulateTick() {
    this.tick++;
    const { metrics, nodes, latencies } = this.state;

    const dProduce = (Math.random() - 0.45) * 0.3;
    const dConsume = (Math.random() - 0.45) * 0.2;
    const produceMB = Math.max(0.1, metrics.produceThroughputMB + dProduce);
    const consumeMB = Math.max(0.05, metrics.consumeThroughputMB + dConsume);

    const produceRate = Math.max(100, metrics.produceRate + Math.floor((Math.random() - 0.45) * 400));
    const consumeRate = Math.max(80,  metrics.consumeRate + Math.floor((Math.random() - 0.45) * 300));
    const errorRate   = Math.random() < 0.05 ? Math.floor(Math.random() * 3) : 0;

    const chartVal = Math.min(100, Math.max(3, ((produceMB + consumeMB) / 50) * 100));
    const newHistory = [...metrics.throughputHistory.slice(1), chartVal];

    const newCommitIndex = metrics.commitIndex + produceRate;
    const followerSync = Math.max(80, Math.min(100, 100 - Math.random() * 8));

    this.state.metrics = {
      ...metrics,
      totalThroughputMB:   Math.round((produceMB + consumeMB) * 100) / 100,
      produceThroughputMB: Math.round(produceMB * 100) / 100,
      consumeThroughputMB: Math.round(consumeMB * 100) / 100,
      produceRate,
      consumeRate,
      errorRate,
      produceLatencyMs: Math.max(0.2, metrics.produceLatencyMs + (Math.random() - 0.5) * 0.3),
      consumeLatencyMs: Math.max(0.1, metrics.consumeLatencyMs + (Math.random() - 0.5) * 0.2),
      activeProducers: Math.max(1, metrics.activeProducers + Math.floor(Math.random() * 3 - 1)),
      activeConsumers: Math.max(1, metrics.activeConsumers + Math.floor(Math.random() * 3 - 1)),
      totalConnections: metrics.totalConnections,
      health: errorRate > 5 ? 'DEGRADED' : 'OPTIMAL',
      commitIndex:  newCommitIndex,
      lastApplied:  newCommitIndex - 1,
      followerSync: Math.round(followerSync),
      globalOffset: newCommitIndex,
      topicCount:   metrics.topicCount,
      logSegments:  metrics.logSegments + (newCommitIndex % 5000 === 0 ? 1 : 0),
      cachedMessages: Math.min(3000, metrics.cachedMessages + produceRate - consumeRate * 0.8),
      throughputHistory: newHistory,
      consumeHistory: [...metrics.consumeHistory.slice(1), Math.min(100, Math.max(3, ((consumeMB) / 50) * 100))],
      errorHistory: [...metrics.errorHistory.slice(1), errorRate],
    };

    this.state.nodes = nodes.map((node, idx) => ({
      ...node,
      commitIndex: idx === 0
        ? newCommitIndex
        : newCommitIndex - Math.floor(Math.random() * 20),
      replicationLag: idx === 0 ? undefined : Math.floor(Math.random() * 25),
    }));

    this.state.latencies = {
      raftRpcMs: Math.max(0.5, latencies.raftRpcMs + (Math.random() - 0.5) * 0.5),
      alphaBeta: Math.max(0.5, latencies.alphaBeta  + (Math.random() - 0.5) * 0.5),
      betaGamma: Math.max(0.5, latencies.betaGamma  + (Math.random() - 0.5) * 0.5),
    };

    // Generate 0-2 events per tick
    if (Math.random() < 0.6) {
      this.state.events = [this.generateEvent(), ...this.state.events].slice(0, 50);
    }
  }
}
