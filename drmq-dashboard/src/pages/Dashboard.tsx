import { Activity, Zap, Users, AlertTriangle, HardDrive } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
// ─── Sub-components ───────────────────────────────────────────────────────────

function HexagonNode({ x, y, color, label, subLabel, status }: any) {
  const isLeader    = status === 'LEADER';
  const isCandidate = status === 'CANDIDATE';
  const points      = '50,5 95,27 95,73 50,95 5,73 5,27';
  const innerPoints = '50,22 80,38 80,62 50,78 20,62 20,38';
  const glowId      = `glow-${color.replace('#', '')}`;

  return (
    <g transform={`translate(${x - 50}, ${y - 55})`}>
      <motion.g initial={{ scale: 0, opacity: 0 }} animate={{ scale: 1, opacity: 1 }} transition={{ type: 'spring', stiffness: 100, damping: 15 }}>
        <filter id={glowId}>
          <feGaussianBlur stdDeviation={isLeader ? '10' : '5'} result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>

        {isLeader && (
          <motion.circle cx="50" cy="50" r="62" fill="none" stroke={color} strokeWidth="1"
            initial={{ scale: 0.8, opacity: 0.7 }} animate={{ scale: 1.6, opacity: 0 }}
            transition={{ repeat: Infinity, duration: 2.2, ease: 'easeOut' }} />
        )}
        {isCandidate && (
          <motion.circle cx="50" cy="50" r="60" fill="none" stroke="#f59e0b" strokeWidth="1"
            animate={{ opacity: [0.8, 0.1, 0.8] }} transition={{ repeat: Infinity, duration: 1 }} />
        )}

        <polygon points={points} fill="#08101a" stroke={color} strokeWidth={isLeader ? '2.5' : '1.5'} filter={`url(#${glowId})`} />
        <motion.polygon points={innerPoints} fill={color}
          animate={{ opacity: isLeader ? [0.35, 0.75, 0.35] : isCandidate ? [0.5, 0.9, 0.5] : 0.2 }}
          transition={{ repeat: Infinity, duration: isLeader ? 2.5 : 1 }} />

        <text x="50" y="128" textAnchor="middle" fill="#ffffff" fontSize="11" fontFamily="monospace" fontWeight="bold" letterSpacing="1">{label}</text>
        <text x="50" y="144" textAnchor="middle" fill={color} fontSize="9" fontFamily="monospace">{subLabel}</text>

        {isLeader && (
          <motion.text x="50" y="-12" textAnchor="middle" fill="#06b6d4" fontSize="9" fontFamily="monospace" fontWeight="bold"
            animate={{ y: [-14, -10, -14] }} transition={{ repeat: Infinity, duration: 1.5 }}>
            ★ LEADER
          </motion.text>
        )}
        {isCandidate && (
          <text x="50" y="-12" textAnchor="middle" fill="#f59e0b" fontSize="9" fontFamily="monospace" fontWeight="bold">CANDIDATE</text>
        )}
      </motion.g>
    </g>
  );
}

function Particle({ path, color, delay, duration, reverse }: any) {
  return (
    <g>
      <path d={path} fill="none" stroke={color} strokeWidth="1" strokeDasharray="4 8" opacity="0.25" />
      <g>
        {reverse
          ? <animateMotion dur={`${duration}s`} repeatCount="indefinite" begin={`${delay}s`} keyPoints="1;0" keyTimes="0;1" calcMode="linear" path={path} />
          : <animateMotion dur={`${duration}s`} repeatCount="indefinite" begin={`${delay}s`} path={path} />
        }
        <circle r="2.5" fill="#fff" opacity="0.9" />
        <circle r="5" fill={color} opacity="0.4" />
      </g>
    </g>
  );
}

function StatCard({ icon: Icon, label, value, unit, color = '#06b6d4', sub }: any) {
  return (
    <div className="bg-[#0b1120]/80 border border-white/5 rounded-xl p-4 flex flex-col gap-1 shadow-lg hover:border-white/10 transition-colors">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[9px] font-bold tracking-[0.2em] text-zinc-500 uppercase">{label}</span>
        <Icon className="w-3.5 h-3.5" style={{ color }} />
      </div>
      <div className="text-2xl font-light text-white flex items-baseline gap-1.5">
        {value}
        {unit && <span className="text-xs font-mono text-zinc-500">{unit}</span>}
      </div>
      {sub && <div className="text-[10px] text-zinc-600 font-mono">{sub}</div>}
    </div>
  );
}

function SparkLine({ data, color }: { data: number[]; color: string }) {
  if (!data || data.length < 2) return null;
  const w = 100, h = 36;
  const max = Math.max(...data, 1);
  const step = w / (data.length - 1);
  let path = `M 0 ${h - (data[0] / max) * h}`;
  for (let i = 1; i < data.length; i++) {
    const x = i * step;
    const y = h - (data[i] / max) * h;
    const px = (i - 1) * step;
    const py = h - (data[i - 1] / max) * h;
    path += ` C ${px + step / 2} ${py}, ${x - step / 2} ${y}, ${x} ${y}`;
  }
  const fill = `${path} L ${w} ${h} L 0 ${h} Z`;
  return (
    <svg className="w-full h-10" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      <defs>
        <linearGradient id={`sg-${color.replace('#','')}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.4" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={fill} fill={`url(#sg-${color.replace('#','')})`} />
      <path d={path} fill="none" stroke={color} strokeWidth="1.2" />
    </svg>
  );
}

function formatNum(n: number, decimals = 0) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toFixed(decimals);
}

// ─── App ──────────────────────────────────────────────────────────────────────

export default function Dashboard({ telemetryState, telemetryError }: { telemetryState: any, telemetryError?: string | null }) {
  if (telemetryError) {
    return (
      <div className="flex h-full bg-[#06080f] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <AlertTriangle className="w-10 h-10 text-red-500" />
          <p className="text-[10px] font-mono tracking-[0.3em] text-red-400 uppercase">{telemetryError}</p>
        </div>
      </div>
    );
  }

  if (!telemetryState) {
    return (
      <div className="flex h-full bg-[#06080f] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <motion.div animate={{ rotate: 360 }} transition={{ repeat: Infinity, duration: 1.5, ease: 'linear' }}
            className="w-10 h-10 rounded-full border-2 border-white/5 border-t-cyan-500" />
          <p className="text-[10px] font-mono tracking-[0.3em] text-cyan-600 uppercase">Connecting to Cluster…</p>
        </div>
      </div>
    );
  }

  const { nodes, metrics, latencies } = telemetryState;
  const sortedNodes = [...nodes].sort((a, b) => a.id.localeCompare(b.id));
  const [n1, n2, n3] = sortedNodes;

  const healthColor = metrics.health === 'OPTIMAL' ? '#10b981' : metrics.health === 'DEGRADED' ? '#f59e0b' : '#ef4444';

  return (
    <div className="flex-1 flex flex-col min-w-0 p-3 gap-3 overflow-hidden h-full">

        {/* Header */}
        <header className="h-14 shrink-0 bg-[#080d18]/80 border border-white/5 rounded-xl flex items-center justify-between px-6">
          <div className="flex items-center gap-3">
            <span className="w-2 h-2 rounded-full animate-pulse" style={{ backgroundColor: healthColor, boxShadow: `0 0 8px ${healthColor}` }} />
            <span className="text-xs font-bold tracking-widest text-white uppercase">Live Telemetry</span>
            <span className="text-zinc-700 text-xs">/</span>
            <span className="text-xs font-mono text-zinc-500">
              {nodes.find((n: any) => n.status === 'LEADER')?.name ?? 'No Leader'}
            </span>
          </div>
          <div className="flex items-center gap-3">
            <span className={`text-[9px] font-bold tracking-widest px-3 py-1 rounded-full border ${
              metrics.health === 'OPTIMAL'  ? 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10' :
              metrics.health === 'DEGRADED' ? 'text-amber-400   border-amber-500/30   bg-amber-500/10'   :
                                              'text-red-400     border-red-500/30     bg-red-500/10'
            }`}>{metrics.health}</span>
            <span className="text-[10px] font-mono text-zinc-600 border border-white/5 px-2 py-1 rounded bg-black/30">
              {metrics.logSegments} segments · {metrics.topicCount} topics
            </span>
          </div>
        </header>

        {/* Body */}
        <div className="flex-1 flex gap-3 min-h-0">

          {/* ── Left: Canvas + Quick stats ──────────────────────────────── */}
          <div className="flex-[3] flex flex-col gap-3 min-h-0 min-w-0">

            {/* Cluster Topology Canvas */}
            <div className="flex-1 bg-[#080d18]/80 border border-white/5 rounded-xl overflow-hidden relative min-h-0">
              <div className="absolute inset-0 bg-[linear-gradient(to_right,#ffffff04_1px,transparent_1px),linear-gradient(to_bottom,#ffffff04_1px,transparent_1px)] bg-[size:36px_36px]" />
              <svg className="w-full h-full" viewBox="0 0 1000 700" preserveAspectRatio="xMidYMid meet">
                <defs>
                  <filter id="pglow"><feGaussianBlur stdDeviation="2" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
                  <linearGradient id="lg1" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#06b6d4" stopOpacity="0.7" />
                    <stop offset="100%" stopColor="#a855f7" stopOpacity="0.7" />
                  </linearGradient>
                </defs>

                {/* Edges */}
                {n1 && n2 && (<g>
                  <path d={`M ${n1.x} ${n1.y} L ${n2.x} ${n2.y}`} fill="none" stroke="url(#lg1)" strokeWidth="1.5" strokeDasharray="5 7" opacity="0.3" />
                  {metrics.produceRate > 0 && <Particle path={`M ${n1.x} ${n1.y} L ${n2.x} ${n2.y}`} color="#06b6d4" delay={0} duration={1.8} />}
                  {metrics.consumeRate > 0 && <Particle path={`M ${n1.x} ${n1.y} L ${n2.x} ${n2.y}`} color="#a855f7" delay={0.9} duration={1.8} reverse />}
                  {(latencies.raftRpcMs > 0) && (
                    <text x={(n1.x + n2.x) / 2 - 20} y={(n1.y + n2.y) / 2 - 12} fill="#4b5563" fontSize="11" fontFamily="monospace">
                      {latencies.raftRpcMs.toFixed(1)}ms
                    </text>
                  )}
                </g>)}
                {n2 && n3 && (<g>
                  <path d={`M ${n2.x} ${n2.y} L ${n3.x} ${n3.y}`} fill="none" stroke="url(#lg1)" strokeWidth="1.5" strokeDasharray="5 7" opacity="0.3" />
                  {metrics.produceRate > 0 && <Particle path={`M ${n2.x} ${n2.y} L ${n3.x} ${n3.y}`} color="#a855f7" delay={0} duration={2.2} />}
                  {metrics.consumeRate > 0 && <Particle path={`M ${n2.x} ${n2.y} L ${n3.x} ${n3.y}`} color="#06b6d4" delay={1.1} duration={2.2} reverse />}
                </g>)}
                {n1 && n3 && (<g>
                  <path d={`M ${n1.x} ${n1.y} L ${n3.x} ${n3.y}`} fill="none" stroke="#06b6d4" strokeWidth="1.5" strokeDasharray="5 7" opacity="0.2" />
                  {metrics.consumeRate > 0 && <Particle path={`M ${n1.x} ${n1.y} L ${n3.x} ${n3.y}`} color="#06b6d4" delay={0.4} duration={1.5} reverse />}
                  {metrics.produceRate > 0 && <Particle path={`M ${n1.x} ${n1.y} L ${n3.x} ${n3.y}`} color="#a855f7" delay={1.0} duration={1.5} />}
                </g>)}

                {/* Nodes */}
                {n1 && <HexagonNode x={n1.x} y={n1.y} color={n1.color} label={n1.name} status={n1.status}
                  subLabel={`${formatNum(n1.produceRate)}/s`} />}
                {n2 && <HexagonNode x={n2.x} y={n2.y} color={n2.color} label={n2.name} status={n2.status}
                  subLabel={n2.replicationLag !== undefined ? `lag: ${n2.replicationLag}` : 'synced'} />}
                {n3 && <HexagonNode x={n3.x} y={n3.y} color={n3.color} label={n3.name} status={n3.status}
                  subLabel={n3.replicationLag !== undefined ? `lag: ${n3.replicationLag}` : 'synced'} />}
              </svg>
            </div>

            {/* Stats row */}
            <div className="shrink-0 grid grid-cols-5 gap-3">
              <StatCard icon={Zap}           label="Produce Rate"     value={formatNum(metrics.produceRate)}    unit="msg/s"  color="#06b6d4"
                sub={`${metrics.produceThroughputMB.toFixed(2)} MB/s`} />
              <StatCard icon={Activity}      label="Consume Rate"     value={formatNum(metrics.consumeRate)}    unit="msg/s"  color="#a855f7"
                sub={`${metrics.consumeThroughputMB.toFixed(2)} MB/s`} />
              <StatCard icon={Users}         label="Connections"      value={metrics.totalConnections}          color="#f59e0b"
                sub={`${metrics.activeProducers}P · ${metrics.activeConsumers}C`} />
              <StatCard icon={HardDrive}     label="Global Offset"    value={formatNum(metrics.globalOffset)}   color="#10b981"
                sub={`${metrics.logSegments} segments`} />
              <StatCard icon={AlertTriangle} label="Error Rate"       value={metrics.errorRate}                 unit="err/s"
                color={metrics.errorRate > 0 ? '#ef4444' : '#4b5563'}
                sub={metrics.errorRate > 0 ? 'check logs' : 'clean'} />
            </div>
          </div>

          {/* ── Right Panel ──────────────────────────────────────────────── */}
          <div className="w-72 shrink-0 flex flex-col gap-3 min-h-0">

            {/* Health */}
            <div className="bg-[#080d18]/80 border border-white/5 rounded-xl p-5 shrink-0 relative overflow-hidden">
              <div className="absolute top-0 right-0 w-28 h-28 rounded-full blur-[40px] opacity-20 pointer-events-none"
                style={{ backgroundColor: healthColor }} />
              <h3 className="text-[9px] font-bold tracking-[0.25em] text-zinc-600 uppercase mb-3">System Health</h3>
              <div className="flex items-center justify-between mb-4">
                <div>
                  <div className="text-xl font-black tracking-widest" style={{ color: healthColor }}>{metrics.health}</div>
                  <div className="text-[10px] text-zinc-600 font-mono mt-0.5">Term {metrics.term}</div>
                </div>
                <div className="w-10 h-10 rounded-full flex items-center justify-center border" style={{ borderColor: `${healthColor}40`, backgroundColor: `${healthColor}15` }}>
                  <div className="w-3.5 h-3.5 rounded-full animate-pulse" style={{ backgroundColor: healthColor }} />
                </div>
              </div>

              {/* Follower Sync */}
              <div>
                <div className="flex justify-between text-[10px] mb-1.5">
                  <span className="text-zinc-500">Follower Sync</span>
                  <span className="font-mono font-bold" style={{ color: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}>
                    {metrics.followerSync}%
                  </span>
                </div>
                <div className="h-1.5 bg-black/50 rounded-full overflow-hidden border border-white/5">
                  <motion.div className="h-full rounded-full"
                    style={{ backgroundColor: metrics.followerSync >= 95 ? '#10b981' : '#f59e0b' }}
                    initial={{ width: 0 }} animate={{ width: `${metrics.followerSync}%` }} transition={{ duration: 0.8 }} />
                </div>
              </div>

              {/* Commit index */}
              <div className="mt-3 pt-3 border-t border-white/5 grid grid-cols-2 gap-2 text-[10px] font-mono">
                <div>
                  <div className="text-zinc-600">Commit Index</div>
                  <div className="text-zinc-300 font-bold">{formatNum(metrics.commitIndex)}</div>
                </div>
                <div>
                  <div className="text-zinc-600">Last Applied</div>
                  <div className="text-zinc-300 font-bold">{formatNum(metrics.lastApplied)}</div>
                </div>
              </div>
            </div>

            {/* Latency card */}
            <div className="bg-[#080d18]/80 border border-white/5 rounded-xl p-5 shrink-0">
              <h3 className="text-[9px] font-bold tracking-[0.25em] text-zinc-600 uppercase mb-3">Latency</h3>
              <div className="space-y-3">
                {[
                  { label: 'Produce p50',  value: metrics.produceLatencyMs, color: '#06b6d4', max: 50 },
                  { label: 'Consume p50',  value: metrics.consumeLatencyMs, color: '#a855f7', max: 50 },
                  { label: 'Raft RPC',     value: latencies.raftRpcMs,      color: '#f59e0b', max: 20 },
                ].map(({ label, value, color, max }) => (
                  <div key={label}>
                    <div className="flex justify-between text-[10px] mb-1">
                      <span className="text-zinc-500">{label}</span>
                      <span className="font-mono font-bold" style={{ color }}>
                        {value > 0 ? `${value.toFixed(2)}ms` : '--'}
                      </span>
                    </div>
                    <div className="h-1 bg-black/50 rounded-full overflow-hidden">
                      <motion.div className="h-full rounded-full"
                        style={{ backgroundColor: color }}
                        initial={{ width: 0 }} animate={{ width: `${Math.min(100, (value / max) * 100)}%` }} transition={{ duration: 0.6 }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Throughput chart */}
            <div className="bg-[#080d18]/80 border border-white/5 rounded-xl p-5 flex-1 flex flex-col min-h-0">
              <div className="flex justify-between items-center mb-3">
                <h3 className="text-[9px] font-bold tracking-[0.25em] text-zinc-600 uppercase">I/O Throughput</h3>
                <div className="flex items-center gap-1.5 text-[9px] font-mono text-cyan-500 bg-cyan-500/10 px-2 py-0.5 rounded border border-cyan-500/20">
                  <span className="w-1 h-1 rounded-full bg-cyan-500 animate-pulse" />LIVE
                </div>
              </div>
              <div className="text-2xl font-light text-white mb-1">
                {metrics.totalThroughputMB.toFixed(2)} <span className="text-xs text-zinc-500 font-mono">MB/s</span>
              </div>
              <div className="text-[10px] text-zinc-600 font-mono mb-3">
                ↑ {metrics.produceThroughputMB.toFixed(2)} produce · ↓ {metrics.consumeThroughputMB.toFixed(2)} consume
              </div>
              <div className="flex-1 min-h-0">
                <SparkLine data={metrics.throughputHistory} color="#06b6d4" />
              </div>
            </div>

            {/* Broker Roster */}
            <div className="bg-[#080d18]/80 border border-white/5 rounded-xl p-5 shrink-0">
              <h3 className="text-[9px] font-bold tracking-[0.25em] text-zinc-600 uppercase mb-3">Broker State</h3>
              <div className="space-y-2">
                <AnimatePresence>
                  {sortedNodes.map(node => (
                    <motion.div key={node.id}
                      initial={{ opacity: 0, x: 16 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -16 }}
                      className="flex justify-between items-center bg-black/30 px-3 py-2.5 rounded-lg border border-white/5">
                      <div className="flex items-center gap-2.5">
                        <div className="w-1.5 h-1.5 rounded-full" style={{
                          backgroundColor: node.color,
                          boxShadow: node.status === 'LEADER' ? `0 0 6px ${node.color}` : undefined
                        }} />
                        <span className="text-xs font-semibold text-zinc-300">{node.name}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        {node.replicationLag !== undefined && node.replicationLag > 0 && (
                          <span className="text-[8px] font-mono text-amber-400">+{node.replicationLag}</span>
                        )}
                        <span className={`text-[8px] font-mono font-bold px-2 py-0.5 rounded border ${
                          node.status === 'LEADER'    ? 'bg-cyan-500/10   text-cyan-400   border-cyan-500/30'   :
                          node.status === 'CANDIDATE' ? 'bg-amber-500/10  text-amber-400  border-amber-500/30'  :
                                                        'bg-zinc-800      text-zinc-500   border-zinc-700'
                        }`}>{node.status}</span>
                      </div>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </div>
            </div>

          </div>
        </div>
    </div>
  );
}
