import { Activity, Zap, Users, AlertTriangle, HardDrive, Radio, GitBranch, Wifi, Camera, XCircle, Pause, Play } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useState, useEffect, useRef } from 'react';
import ClusterTopology from '../components/ClusterTopology';
import { StatCard, Panel, formatNum } from '../components/DashboardWidgets';
import {
  LineChart, XAxis, YAxis, Legend, Tooltip, Line,
  AreaChart, Area,
  RadarChart, Radar,
  Sparkline,
  DitherButton,
  DitherAvatar,
  BlockLegend,
} from '../components/dither-kit';
import type { ChartConfig } from '../components/dither-kit';
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

/* ── Live Clock ─────────────────────────────────────────────────────── */
function LiveClock() {
  const [time, setTime] = useState(() => new Date().toUTCString().slice(17, 25));
  useEffect(() => {
    const id = setInterval(() => setTime(new Date().toUTCString().slice(17, 25)), 1000);
    return () => clearInterval(id);
  }, []);
  return <span className="mono text-[10px] text-zinc-600">{time} UTC</span>;
}

/* ── Dither Kit chart configs ───────────────────────────────────────── */
const throughputConfig: ChartConfig = {
  produce: { label: 'Produce MB/s', color: 'blue' },
  consume: { label: 'Consume MB/s', color: 'purple' },
};

const radarConfig: ChartConfig = {
  value: { label: 'Health', color: 'blue' },
};

const latencyConfig: ChartConfig = {
  produce: { label: 'Produce', color: 'blue' },
  consume: { label: 'Consume', color: 'purple' },
  rpc: { label: 'Raft RPC', color: 'orange' },
};

/* ═══════════════════════════════════════════════════════════════════════
   Dashboard Page
   ═══════════════════════════════════════════════════════════════════════ */
export default function Dashboard({
  telemetryState,
  telemetryError,
  paused,
  onTogglePause,
}: {
  telemetryState: any;
  telemetryError?: string | null;
  paused: boolean;
  onTogglePause: () => void;
}) {

  /* ── Hooks (must come before any early returns) ───────────────── */
  const [historyWindowSeconds, setHistoryWindowSeconds] = useState(30);
  const prevCommit = useRef<number>(0);
  const [latencyHistory, setLatencyHistory] = useState<any[]>([]);

  useEffect(() => {
    if (telemetryState?.metrics) {
      prevCommit.current = telemetryState.metrics.commitIndex;
    }
  }, [telemetryState?.metrics?.commitIndex]);

  useEffect(() => {
    if (!paused && telemetryState) {
      const { metrics, latencies } = telemetryState;
      setLatencyHistory(prev => {
        const next = [...prev, {
          t: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
          produce: metrics.produceLatencyMs,
          consume: metrics.consumeLatencyMs,
          rpc: latencies.raftRpcMs
        }].slice(-30);
        return next;
      });
    }
  }, [telemetryState?.metrics?.produceLatencyMs, telemetryState?.metrics?.consumeLatencyMs, telemetryState?.latencies?.raftRpcMs, paused, telemetryState]);

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

  /* ── Throughput chart data ─────────────────────────────────────── */
  const totalHistFull    = metrics.throughputHistory ?? [];
  const produceHistFull  = metrics.produceHistory ?? [];
  const consumeHistFull  = metrics.consumeHistory ?? [];
  const errorHistFull    = metrics.errorHistory ?? [];

  const totalHist   = totalHistFull.slice(-historyWindowSeconds);
  const produceHist = produceHistFull.slice(-historyWindowSeconds);
  const consumeHist = consumeHistFull.slice(-historyWindowSeconds);
  const errorHist   = errorHistFull.slice(-historyWindowSeconds);

  const throughputData = totalHist.map((v: number, i: number) => {
    const consumeVal = consumeHist[i] ?? 0;
    let produceVal = produceHist[i];
    if (produceVal === undefined) {
      produceVal = Math.max(0, v - consumeVal);
    }
    return {
      t: `-${totalHist.length - i}s`,
      produce: parseFloat(produceVal.toFixed(2)),
      consume: parseFloat(consumeVal.toFixed(2)),
    };
  });
  
  const formatMBps = (val: number) => val > 0 && val < 0.005 ? '< 0.01' : val.toFixed(2);

  /* ── Radar data for selected/leader node ──────────────────────── */
  const leaderNode = nodes.find((n: any) => n.status === 'LEADER');
  const logScale = (val: number, maxLog = 4) => val <= 0 ? 0 : Math.min(100, (Math.log10(val) / maxLog) * 100);
  
  const radarData = [
    { axis: 'Sync%',   value: metrics.followerSync },
    { axis: 'Produce', value: logScale(metrics.produceRate) },
    { axis: 'Consume', value: logScale(metrics.consumeRate) },
    { axis: 'Latency', value: Math.max(0, 100 - (metrics.produceLatencyMs / 50) * 100) },
    { axis: 'Health',  value: metrics.health === 'OPTIMAL' ? 100 : metrics.health === 'DEGRADED' ? 55 : 15 },
  ];

  /* ── Commit index delta ────────────────────────────────────────── */
  const delta = metrics.commitIndex - prevCommit.current;


  return (
    <div className="flex flex-col h-full p-3 gap-3 overflow-hidden" style={{ background: '#000' }}>

      {/* ── Header ────────────────────────────────────────────────── */}
      <header className="glass-panel h-14 shrink-0 flex items-center justify-between px-6">
        <div className="flex items-center gap-3">
          <div className="live-dot" style={{ backgroundColor: paused ? '#f59e0b' : healthColor, boxShadow: `0 0 8px ${paused ? '#f59e0b' : healthColor}` }} />
          <span className="text-[11px] font-bold tracking-[0.15em] text-white uppercase">
            {paused ? 'Paused' : 'Live Telemetry'}
          </span>
          <span className="text-zinc-700 text-xs select-none">/</span>
          <span className="mono text-[11px] text-zinc-500">{leaderName}</span>
        </div>
        <div className="flex items-center gap-2.5">
          <LiveClock />
          <span className={`mono text-[9px] font-bold tracking-[0.12em] px-3 py-1 rounded-md border ${
            paused                               ? 'text-amber-400 border-amber-500/20 bg-amber-500/8' :
            metrics.health === 'OPTIMAL'  ? 'text-emerald-400 border-emerald-500/20 bg-emerald-500/8' :
            metrics.health === 'DEGRADED' ? 'text-amber-400 border-amber-500/20 bg-amber-500/8' :
                                            'text-red-400 border-red-500/20 bg-red-500/8'
          }`}>{paused ? 'PAUSED' : metrics.health}</span>
          <span className="mono text-[10px] text-zinc-600 bg-white/[0.03] border border-white/5 px-2.5 py-1 rounded-md">
            {metrics.logSegments} seg · {metrics.topicCount} topics
          </span>
          {/* ── Pause / Resume ─────────────────────────────────── */}
          <DitherButton
            color={paused ? 'green' : 'blue'}
            bloom="aura"
            onClick={onTogglePause}
            className="flex items-center gap-1.5 text-[10px] font-semibold tracking-wide px-3 py-1.5 rounded-lg"
          >
            {paused
              ? <span className="flex items-center gap-1.5"><Play className="w-3 h-3" /> Resume</span>
              : <span className="flex items-center gap-1.5"><Pause className="w-3 h-3" /> Pause</span>}
          </DitherButton>
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
              sub={`${formatMBps(metrics.produceThroughputMB)} MB/s`}
              spark={<Sparkline data={totalHist.slice(-15)} color="blue" bloom="low" className="h-6 w-full" />} />
            <StatCard icon={Activity} label="Consume Rate"
              value={formatNum(metrics.consumeRate)} unit="msg/s" color="#a855f7"
              sub={`${formatMBps(metrics.consumeThroughputMB)} MB/s`}
              spark={<Sparkline data={consumeHist.slice(-15)} color="purple" bloom="low" className="h-6 w-full" />} />
            <StatCard icon={Users} label="Connections"
              value={metrics.totalConnections} color="#f59e0b"
              sub={`${metrics.activeProducers}P · ${metrics.activeConsumers}C`} />
            <StatCard icon={HardDrive} label="Global Offset"
              value={formatNum(metrics.globalOffset)} color="#10b981"
              sub={delta > 0 ? <span className="text-emerald-500">↑ +{formatNum(delta)}/tick</span> : `${metrics.logSegments} segments`} />
            <StatCard icon={AlertTriangle} label="Error Rate"
              value={metrics.errorRate} unit="err/s"
              color={metrics.errorRate > 0 ? '#ef4444' : '#3f3f46'}
              sub={metrics.errorRate > 0 ? 'check logs' : 'clean'}
              spark={<Sparkline data={errorHist.slice(-15)} color={metrics.errorRate > 0 ? 'red' : 'grey'} bloom={metrics.errorRate > 0 ? 'high' : 'off'} className="h-6 w-full" />} />
          </div>
        </div>

        {/* ── Right panel ───────────────────────────────────────── */}
        <div className="w-[360px] shrink-0 flex flex-col gap-2.5 min-h-0 overflow-y-auto pr-1">

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
              <div className="w-10 h-10 rounded-full flex items-center justify-center border border-white/5"
                style={{ background: `${healthColor}12` }}>
                <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: healthColor, boxShadow: `0 0 8px ${healthColor}` }} />
              </div>
            </div>
            <div className="relative z-10">
              <div className="flex justify-between text-[10px] mb-1.5">
                <span className="text-zinc-500 font-medium">Follower Sync</span>
                <span className="mono font-bold" style={{ color: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}>
                  {metrics.followerSync}%
                </span>
              </div>
              <div className="w-full h-[3px] bg-white/5 rounded-full overflow-hidden">
                <motion.div className="h-full rounded-full"
                  style={{ backgroundColor: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}
                  initial={{ width: 0 }} animate={{ width: `${metrics.followerSync}%` }}
                  transition={{ duration: 0.8 }} />
              </div>
            </div>
            <div className="mt-4 pt-3 border-t border-white/5 relative z-10">
              <BlockLegend
                config={{ 
                  commit: { label: 'Commit Index', color: 'blue' },
                  applied: { label: 'Last Applied', color: 'purple' }
                }}
                values={{ 
                  commit: metrics.commitIndex,
                  applied: metrics.lastApplied
                }}
                valueFormatter={(val) => formatNum(val).toString()}
              />
            </div>
          </Panel>

          {/* ── I/O Throughput — Dither Kit LineChart ──────────── */}
          <Panel title="I/O Throughput" badge={
            <div className="flex items-center gap-1.5 mono text-[9px] text-cyan-500 bg-cyan-500/8 px-2 py-0.5 rounded border border-cyan-500/15">
              <span className="live-dot" style={{ backgroundColor: '#06b6d4', width: 4, height: 4 }} />
              LIVE
            </div>
          } className="shrink-0">
            <div className="text-2xl font-semibold text-white tracking-tight mb-0.5">
              {formatMBps(metrics.totalThroughputMB)} <span className="mono text-xs text-zinc-500">MB/s</span>
            </div>
            <div className="mono text-[10px] text-zinc-600 mb-3">
              ↑ {formatMBps(metrics.produceThroughputMB)} · ↓ {formatMBps(metrics.consumeThroughputMB)}
            </div>
            <div className="h-[80px] w-full">
              <LineChart
                data={throughputData}
                config={throughputConfig}
                bloom="aura"
                margins={{ top: 24, right: 4, bottom: 16, left: 28 }}
                animationDuration={600}
              >
                <XAxis key="x" dataKey="t" />
                <YAxis key="y" />
                <Legend key="legend" isClickable />
                <Tooltip key="tooltip" labelKey="t" />
                <Line key="produce" dataKey="produce" />
                <Line key="consume" dataKey="consume" strokeVariant="dashed" />
              </LineChart>
            </div>
            <div className="mt-4 pt-4 border-t border-white/5 flex items-center justify-between gap-4">
              <span className="mono text-[10px] text-zinc-500 w-16">30s</span>
              <input 
                type="range" 
                min="30" 
                max="300" 
                step="30"
                value={historyWindowSeconds} 
                onChange={(e) => setHistoryWindowSeconds(Number(e.target.value))}
                className="flex-1 h-1.5 bg-white/10 rounded-full appearance-none [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-3 [&::-webkit-slider-thumb]:h-3 [&::-webkit-slider-thumb]:bg-cyan-400 [&::-webkit-slider-thumb]:rounded-full cursor-pointer"
              />
              <span className="mono text-[10px] text-zinc-500 w-16 text-right">5min</span>
            </div>
            <div className="text-center mt-1">
              <span className="mono text-[9px] text-zinc-600 tracking-wider">VIEWING LAST {historyWindowSeconds >= 60 ? `${historyWindowSeconds / 60} MIN` : `${historyWindowSeconds} SEC`}</span>
            </div>
          </Panel>

          {/* ── Node Health Radar ──────────────────────────────── */}
          <Panel title="Cluster Health Profile" badge={
            leaderNode && <span className="mono text-[9px] text-zinc-600">{leaderNode.name}</span>
          } className="shrink-0">
            <div className="h-[140px] w-full">
              <RadarChart
                data={radarData}
                config={radarConfig}
                nameKey="axis"
                bloom={metrics.health === 'CRITICAL' ? 'high' : metrics.health === 'DEGRADED' ? 'aura' : 'low'}
                animationDuration={800}
              >
                <Radar key="radar" dataKey="value" />
              </RadarChart>
            </div>
          </Panel>

          {/* Latency */}
          <Panel title="Latency Distribution" className="shrink-0">
            <div className="h-[140px] w-full mt-2">
              <AreaChart
                data={latencyHistory}
                config={latencyConfig}
                bloom="off"
                margins={{ top: 24, right: 8, bottom: 20, left: 28 }}
                animationDuration={400}
              >
                <XAxis key="x" dataKey="t" maxTicks={4} />
                <YAxis key="y" />
                <Legend key="legend" isClickable />
                <Tooltip key="tooltip" labelKey="t" />
                <Area key="produce" dataKey="produce" variant="hatched" />
                <Area key="consume" dataKey="consume" variant="dotted" />
                <Area key="rpc" dataKey="rpc" variant="gradient" />
              </AreaChart>
            </div>
          </Panel>

          {/* Broker Roster */}
          <Panel title="Broker Roster" className="shrink-0">
            <div className="space-y-2">
              <AnimatePresence>
                {sortedNodes.map((node: any) => (
                  <motion.div key={node.id}
                    initial={{ opacity: 0, x: 12 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -12 }}
                    className="flex justify-between items-center bg-white/[0.02] px-3 py-2.5 rounded-lg border border-white/[0.04] hover:border-white/[0.08] transition-colors cursor-pointer">
                    <div className="flex items-center gap-2.5">
                      <div className="relative">
                        <DitherAvatar name={node.id} className="w-6 h-6 rounded" />
                        {node.status === 'LEADER' && (
                          <div className="absolute -inset-1 border border-cyan-500/50 rounded-md pointer-events-none" />
                        )}
                      </div>
                      <div className="flex flex-col">
                        <span className="text-xs font-semibold text-zinc-300">{node.name}</span>
                        <span className="mono text-[8px] text-zinc-600">{node.id.substring(0, 8)}</span>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {node.replicationLag !== undefined && node.replicationLag < 0 && (
                        <span className="mono text-[8px] text-red-400">offline</span>
                      )}
                      {node.replicationLag !== undefined && node.replicationLag > 0 && (
                        <div className="flex items-center gap-1.5">
                          <div className="w-10 h-[2px] rounded-full bg-white/10 overflow-hidden">
                            <div className="h-full rounded-full transition-all duration-700"
                              style={{
                                width: `${Math.min(100, (node.replicationLag / 30) * 100)}%`,
                                backgroundColor: node.replicationLag > 15 ? '#ef4444' : '#f59e0b',
                              }} />
                          </div>
                          <span className="mono text-[8px] text-amber-400">+{node.replicationLag}</span>
                        </div>
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

          {/* ── Activity Feed ─────────────────────────────────────── */}
          <Panel title="Activity Feed" badge={
            <span className="mono text-[9px] text-zinc-600">{events.length} events</span>
          } className="flex-1 flex flex-col min-h-[200px]">
            <div className="flex-1 overflow-y-auto space-y-1 pr-1 -mr-1">
              {events.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-8 gap-3">
                  <Radio className="w-6 h-6 text-zinc-800" />
                  <p className="mono text-[9px] text-zinc-700 text-center">No events yet — waiting<br/>for cluster activity…</p>
                </div>
              ) : (
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
              )}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
