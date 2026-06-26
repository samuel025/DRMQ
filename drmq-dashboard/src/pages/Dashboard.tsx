import { Activity, Zap, Users, AlertTriangle, HardDrive, Clock, Radio, GitBranch, Wifi, Camera, XCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import ClusterTopology from '../components/ClusterTopology';
import { StatCard, SparkLine, Panel, LabeledProgress, formatNum } from '../components/DashboardWidgets';
import type { ClusterEvent } from '../types/telemetry';

/* ── Event Icon Map ──────────────────────────────────────────────────── */
const EVENT_ICON: Record<ClusterEvent['type'], typeof Radio> = {
  REPLICATION: GitBranch,
  ELECTION: Radio,
  CONNECTION: Wifi,
  SNAPSHOT: Camera,
  ERROR: XCircle,
};
const EVENT_COLOR: Record<ClusterEvent['severity'], string> = {
  info: '#3f3f46',
  warn: '#f59e0b',
  error: '#ef4444',
};

function timeAgo(ts: number): string {
  const s = Math.floor((Date.now() - ts) / 1000);
  if (s < 5) return 'just now';
  if (s < 60) return `${s}s ago`;
  return `${Math.floor(s / 60)}m ago`;
}

/* ═══════════════════════════════════════════════════════════════════════
   Dashboard Page
   ═══════════════════════════════════════════════════════════════════════ */
export default function Dashboard({ telemetryState, telemetryError }: { telemetryState: any; telemetryError?: string | null }) {

  /* ── Error state ───────────────────────────────────────────────── */
  if (telemetryError) {
    return (
      <div className="flex h-full items-center justify-center" style={{ background: '#000' }}>
        <div className="flex flex-col items-center gap-4">
          <div className="w-14 h-14 rounded-2xl flex items-center justify-center"
            style={{ background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)' }}>
            <AlertTriangle className="w-6 h-6 text-red-500" />
          </div>
          <p className="mono text-[10px] tracking-[0.3em] text-red-400 uppercase">{telemetryError}</p>
        </div>
      </div>
    );
  }

  /* ── Loading state ─────────────────────────────────────────────── */
  if (!telemetryState) {
    return (
      <div className="flex h-full items-center justify-center" style={{ background: '#000' }}>
        <div className="flex flex-col items-center gap-5">
          <motion.div animate={{ rotate: 360 }}
            transition={{ repeat: Infinity, duration: 1.2, ease: 'linear' }}
            className="w-10 h-10 rounded-full"
            style={{ border: '2px solid rgba(255,255,255,0.04)', borderTopColor: '#06b6d4' }} />
          <p className="mono text-[10px] tracking-[0.3em] text-cyan-700 uppercase">Connecting to Cluster…</p>
        </div>
      </div>
    );
  }

  const { nodes, metrics, latencies, events = [] } = telemetryState;
  const sortedNodes = [...nodes].sort((a: any, b: any) => a.id.localeCompare(b.id));
  const healthColor = metrics.health === 'OPTIMAL' ? '#10b981' : metrics.health === 'DEGRADED' ? '#f59e0b' : '#ef4444';
  const leaderName = nodes.find((n: any) => n.status === 'LEADER')?.name ?? 'No Leader';

  return (
    <div className="flex flex-col h-full p-3 gap-3 overflow-hidden" style={{ background: '#000' }}>

      {/* ── Header ────────────────────────────────────────────────── */}
      <header className="glass-panel h-14 shrink-0 flex items-center justify-between px-6">
        <div className="flex items-center gap-3">
          <div className="live-dot" style={{ backgroundColor: healthColor, boxShadow: `0 0 8px ${healthColor}` }} />
          <span className="text-[11px] font-bold tracking-[0.15em] text-white uppercase">Live Telemetry</span>
          <span className="text-zinc-700 text-xs select-none">/</span>
          <span className="mono text-[11px] text-zinc-500">{leaderName}</span>
        </div>
        <div className="flex items-center gap-2.5">
          <span className={`mono text-[9px] font-bold tracking-[0.12em] px-3 py-1 rounded-md border ${
            metrics.health === 'OPTIMAL'  ? 'text-emerald-400 border-emerald-500/20 bg-emerald-500/8' :
            metrics.health === 'DEGRADED' ? 'text-amber-400 border-amber-500/20 bg-amber-500/8' :
                                            'text-red-400 border-red-500/20 bg-red-500/8'
          }`}>{metrics.health}</span>
          <span className="mono text-[10px] text-zinc-600 bg-white/[0.03] border border-white/5 px-2.5 py-1 rounded-md">
            {metrics.logSegments} seg · {metrics.topicCount} topics
          </span>
        </div>
      </header>

      {/* ── Body ──────────────────────────────────────────────────── */}
      <div className="flex-1 flex gap-3 min-h-0">

        {/* Left column: topology + stats */}
        <div className="flex-[3] flex flex-col gap-3 min-h-0 min-w-0">

          {/* Cluster topology (draggable + clickable) */}
          <ClusterTopology nodes={nodes} metrics={metrics} latencies={latencies} />

          {/* Stats grid */}
          <div className="shrink-0 grid grid-cols-5 gap-2.5">
            <StatCard icon={Zap} label="Produce Rate"
              value={formatNum(metrics.produceRate)} unit="msg/s" color="#06b6d4"
              sub={`${metrics.produceThroughputMB.toFixed(2)} MB/s`} />
            <StatCard icon={Activity} label="Consume Rate"
              value={formatNum(metrics.consumeRate)} unit="msg/s" color="#a855f7"
              sub={`${metrics.consumeThroughputMB.toFixed(2)} MB/s`} />
            <StatCard icon={Users} label="Connections"
              value={metrics.totalConnections} color="#f59e0b"
              sub={`${metrics.activeProducers}P · ${metrics.activeConsumers}C`} />
            <StatCard icon={HardDrive} label="Global Offset"
              value={formatNum(metrics.globalOffset)} color="#10b981"
              sub={`${metrics.logSegments} segments`} />
            <StatCard icon={AlertTriangle} label="Error Rate"
              value={metrics.errorRate} unit="err/s"
              color={metrics.errorRate > 0 ? '#ef4444' : '#3f3f46'}
              sub={metrics.errorRate > 0 ? 'check logs' : 'clean'} />
          </div>
        </div>

        {/* ── Right panel ───────────────────────────────────────── */}
        <div className="w-[280px] shrink-0 flex flex-col gap-2.5 min-h-0 overflow-y-auto">

          {/* System Health */}
          <Panel title="System Health" className="shrink-0 relative overflow-hidden">
            <div className="absolute top-0 right-0 w-32 h-32 rounded-full blur-[50px] opacity-15 pointer-events-none"
              style={{ backgroundColor: healthColor }} />
            <div className="flex items-center justify-between mb-4 relative z-10">
              <div>
                <div className="text-xl font-extrabold tracking-widest" style={{ color: healthColor }}>
                  {metrics.health}
                </div>
                <div className="mono text-[10px] text-zinc-600 mt-0.5">Term {metrics.term}</div>
              </div>
              <div className="w-11 h-11 rounded-xl flex items-center justify-center"
                style={{ background: `${healthColor}12`, border: `1px solid ${healthColor}25` }}>
                <div className="live-dot" style={{ backgroundColor: healthColor }} />
              </div>
            </div>
            <div className="relative z-10">
              <div className="flex justify-between text-[10px] mb-1.5">
                <span className="text-zinc-500 font-medium">Follower Sync</span>
                <span className="mono font-bold" style={{ color: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}>
                  {metrics.followerSync}%
                </span>
              </div>
              <div className="progress-track">
                <motion.div className="progress-fill"
                  style={{ backgroundColor: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}
                  initial={{ width: 0 }} animate={{ width: `${metrics.followerSync}%` }}
                  transition={{ duration: 0.8 }} />
              </div>
            </div>
            <div className="mt-4 pt-3 border-t border-white/5 grid grid-cols-2 gap-3 text-[10px] mono relative z-10">
              <div>
                <div className="text-zinc-600 mb-0.5">Commit Index</div>
                <div className="text-zinc-200 font-bold text-sm">{formatNum(metrics.commitIndex)}</div>
              </div>
              <div>
                <div className="text-zinc-600 mb-0.5">Last Applied</div>
                <div className="text-zinc-200 font-bold text-sm">{formatNum(metrics.lastApplied)}</div>
              </div>
            </div>
          </Panel>

          {/* Latency */}
          <Panel title="Latency" badge={
            <div className="flex items-center gap-1.5 mono text-[9px] text-zinc-600">
              <Clock className="w-3 h-3" />p50
            </div>
          } className="shrink-0">
            <div className="space-y-3">
              <LabeledProgress label="Produce" value={metrics.produceLatencyMs} max={50} color="#06b6d4" suffix="ms" />
              <LabeledProgress label="Consume" value={metrics.consumeLatencyMs} max={50} color="#a855f7" suffix="ms" />
              <LabeledProgress label="Raft RPC" value={latencies.raftRpcMs} max={20} color="#f59e0b" suffix="ms" />
            </div>
          </Panel>

          {/* I/O Throughput */}
          <Panel title="I/O Throughput" badge={
            <div className="flex items-center gap-1.5 mono text-[9px] text-cyan-500 bg-cyan-500/8 px-2 py-0.5 rounded border border-cyan-500/15">
              <span className="live-dot" style={{ backgroundColor: '#06b6d4', width: 4, height: 4 }} />
              LIVE
            </div>
          } className="shrink-0">
            <div className="text-2xl font-semibold text-white tracking-tight mb-0.5">
              {metrics.totalThroughputMB.toFixed(2)} <span className="mono text-xs text-zinc-500">MB/s</span>
            </div>
            <div className="mono text-[10px] text-zinc-600 mb-3">
              ↑ {metrics.produceThroughputMB.toFixed(2)} · ↓ {metrics.consumeThroughputMB.toFixed(2)}
            </div>
            <SparkLine data={metrics.throughputHistory} color="#06b6d4" height={48} />
          </Panel>

          {/* Broker Roster */}
          <Panel title="Broker Roster" className="shrink-0">
            <div className="space-y-2">
              <AnimatePresence>
                {sortedNodes.map((node: any) => (
                  <motion.div key={node.id}
                    initial={{ opacity: 0, x: 12 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -12 }}
                    className="flex justify-between items-center bg-white/[0.02] px-3 py-2.5 rounded-lg border border-white/[0.04] hover:border-white/[0.08] transition-colors cursor-default">
                    <div className="flex items-center gap-2.5">
                      <div className="w-[5px] h-[5px] rounded-full shrink-0"
                        style={{ backgroundColor: node.color,
                          boxShadow: node.status === 'LEADER' ? `0 0 6px ${node.color}` : undefined }} />
                      <span className="text-xs font-semibold text-zinc-300">{node.name}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {node.replicationLag !== undefined && node.replicationLag < 0 && (
                        <span className="mono text-[8px] text-red-400">offline</span>
                      )}
                      {node.replicationLag !== undefined && node.replicationLag > 0 && (
                        <span className="mono text-[8px] text-amber-400">+{node.replicationLag}</span>
                      )}
                      <span className={`node-badge ${
                        node.status === 'LEADER' ? 'node-badge--leader' :
                        node.status === 'CANDIDATE' ? 'node-badge--candidate' : 'node-badge--follower'
                      }`}>{node.status}</span>
                    </div>
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
          </Panel>

          {/* ── Event Feed ─────────────────────────────────────────── */}
          <Panel title="Activity Feed" badge={
            <span className="mono text-[9px] text-zinc-600">{events.length} events</span>
          } className="flex-1 flex flex-col min-h-[400px]">
            <div className="flex-1 overflow-y-auto space-y-1 pr-1 -mr-1">
              <AnimatePresence initial={false}>
                {(events as ClusterEvent[]).slice(0, 20).map((evt) => {
                  const Icon = EVENT_ICON[evt.type] || Radio;
                  const borderColor = EVENT_COLOR[evt.severity];
                  return (
                    <motion.div key={evt.id}
                      initial={{ opacity: 0, height: 0, y: -4 }}
                      animate={{ opacity: 1, height: 'auto', y: 0 }}
                      exit={{ opacity: 0, height: 0 }}
                      transition={{ duration: 0.2 }}
                      className="flex items-start gap-2.5 py-2 border-b border-white/[0.03] last:border-0">
                      <div className="w-5 h-5 rounded-md flex items-center justify-center shrink-0 mt-0.5"
                        style={{ background: `${borderColor}12`, border: `1px solid ${borderColor}20` }}>
                        <Icon className="w-2.5 h-2.5" style={{ color: borderColor }} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-[10px] text-zinc-400 leading-relaxed truncate">{evt.message}</p>
                        <span className="mono text-[8px] text-zinc-700">{timeAgo(evt.timestamp)}</span>
                      </div>
                    </motion.div>
                  );
                })}
              </AnimatePresence>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
