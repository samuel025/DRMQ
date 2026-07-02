import { useState, useRef, useCallback } from 'react';
import { motion } from 'framer-motion';
import { formatNum } from './DashboardWidgets';

/* ── Types ───────────────────────────────────────────────────────────── */
interface NodePos { x: number; y: number }

/* ── Circular Node ───────────────────────────────────────────────────── */
function ClusterNode({ x, y, color, label, subLabel, status, isSelected, isHovered,
  onPointerDown, onPointerEnter, onPointerLeave, onClick }: any) {
  const isLeader = status === 'LEADER';
  const isCandidate = status === 'CANDIDATE';
  const r = 38;
  const uid = label.replace(/[^a-z0-9]/gi, '');

  return (
    <g transform={`translate(${x}, ${y})`} style={{ cursor: 'grab' }}
      onPointerDown={onPointerDown} onPointerEnter={onPointerEnter}
      onPointerLeave={onPointerLeave} onClick={onClick}>
      <defs>
        <filter id={`ng-${uid}`}>
          <feGaussianBlur stdDeviation={isLeader ? '8' : '4'} result="b" />
          <feMerge><feMergeNode in="b" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>
        <radialGradient id={`rg-${uid}`} cx="50%" cy="40%" r="60%">
          <stop offset="0%" stopColor={color} stopOpacity="0.25" />
          <stop offset="100%" stopColor={color} stopOpacity="0.02" />
        </radialGradient>
      </defs>

      {/* Selection ring */}
      {isSelected && (
        <motion.circle r={r + 14} fill="none" stroke={color} strokeWidth="1.5"
          strokeDasharray="6 4"
          initial={{ opacity: 0 }} animate={{ opacity: 0.6, rotate: [0, 360] }}
          transition={{ opacity: { duration: 0.2 }, rotate: { repeat: Infinity, duration: 10, ease: 'linear' } }} />
      )}

      {/* Hover highlight */}
      {isHovered && !isSelected && (
        <circle r={r + 10} fill="none" stroke={color} strokeWidth="0.8" opacity="0.3" />
      )}

      {/* Leader pulse rings */}
      {isLeader && (
        <>
          <motion.circle r={r + 20} fill="none" stroke={color} strokeWidth="0.5"
            initial={{ scale: 0.9, opacity: 0.5 }} animate={{ scale: 1.5, opacity: 0 }}
            transition={{ repeat: Infinity, duration: 2.5, ease: 'easeOut' }} />
          <motion.circle r={r + 12} fill="none" stroke={color} strokeWidth="0.5"
            initial={{ scale: 0.9, opacity: 0.4 }} animate={{ scale: 1.3, opacity: 0 }}
            transition={{ repeat: Infinity, duration: 2.5, ease: 'easeOut', delay: 0.4 }} />
        </>
      )}

      {/* Candidate ring */}
      {isCandidate && (
        <motion.circle r={r + 8} fill="none" stroke="#f59e0b" strokeWidth="1"
          strokeDasharray="4 6"
          animate={{ opacity: [0.7, 0.15, 0.7], rotate: [0, 360] }}
          transition={{ opacity: { repeat: Infinity, duration: 1.2 },
            rotate: { repeat: Infinity, duration: 8, ease: 'linear' } }} />
      )}

      {/* Main circle */}
      <circle r={r} fill={`url(#rg-${uid})`} stroke={color}
        strokeWidth={isLeader ? 2 : 1} opacity={isLeader ? 1 : 0.7}
        filter={`url(#ng-${uid})`} />

      {/* Inner core */}
      <motion.circle r={r * 0.55} fill={color}
        animate={{ opacity: isLeader ? [0.15, 0.35, 0.15] : isCandidate ? [0.2, 0.5, 0.2] : [0.06, 0.1, 0.06] }}
        transition={{ repeat: Infinity, duration: isLeader ? 3 : 1.5 }} />

      <circle r={r * 0.35} fill="none" stroke={color} strokeWidth="0.5" opacity="0.3" />
      <circle r={3} fill={color} opacity={isLeader ? 0.9 : 0.5} />

      {/* Labels */}
      <text y={r + 22} textAnchor="middle" fill="#ffffff" fontSize="11"
        fontFamily="'Inter', sans-serif" fontWeight="600" letterSpacing="0.5"
        style={{ pointerEvents: 'none', userSelect: 'none' }}>{label}</text>
      <text y={r + 36} textAnchor="middle" fill={color} fontSize="9.5"
        fontFamily="'JetBrains Mono', monospace" opacity="0.8"
        style={{ pointerEvents: 'none', userSelect: 'none' }}>{subLabel}</text>

      {/* Status badge */}
      {isLeader && (
        <motion.g animate={{ y: [-2, 2, -2] }} transition={{ repeat: Infinity, duration: 2, ease: 'easeInOut' }}>
          <rect x="-24" y={-r - 26} width="48" height="16" rx="4"
            fill="rgba(6,182,212,0.12)" stroke="rgba(6,182,212,0.3)" strokeWidth="0.5" />
          <text y={-r - 15} textAnchor="middle" fill="#22d3ee" fontSize="8"
            fontFamily="'JetBrains Mono', monospace" fontWeight="700" letterSpacing="1"
            style={{ pointerEvents: 'none' }}>LEADER</text>
        </motion.g>
      )}
      {isCandidate && (
        <g>
          <rect x="-30" y={-r - 24} width="60" height="16" rx="4"
            fill="rgba(245,158,11,0.12)" stroke="rgba(245,158,11,0.3)" strokeWidth="0.5" />
          <text y={-r - 13} textAnchor="middle" fill="#fbbf24" fontSize="8"
            fontFamily="'JetBrains Mono', monospace" fontWeight="700" letterSpacing="1"
            style={{ pointerEvents: 'none' }}>CANDIDATE</text>
        </g>
      )}
    </g>
  );
}

/* ── Data Particle ───────────────────────────────────────────────────── */
function DataParticle({ path, color, delay, duration, reverse }: any) {
  return (
    <g>
      {reverse
        ? <animateMotion dur={`${duration}s`} repeatCount="indefinite" begin={`${delay}s`}
            keyPoints="1;0" keyTimes="0;1" calcMode="linear" path={path} />
        : <animateMotion dur={`${duration}s`} repeatCount="indefinite" begin={`${delay}s`} path={path} />}
      <circle r="2" fill="#fff" opacity="0.85" />
      <circle r="4.5" fill={color} opacity="0.25" />
    </g>
  );
}

/* ── Connection ──────────────────────────────────────────────────────── */
function Connection({ x1, y1, x2, y2, produceRate, consumeRate, latencyMs }: any) {
  const mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
  const dx = x2 - x1, dy = y2 - y1;
  const len = Math.sqrt(dx * dx + dy * dy);
  const nx = -dy / len, ny = dx / len;
  const cx = mx + nx * 30, cy = my + ny * 30;
  const path = `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`;

  return (
    <g>
      <path d={path} fill="none" stroke="url(#connection-grad)" strokeWidth="1" opacity="0.2" />
      <path d={path} fill="none" stroke="url(#connection-grad)" strokeWidth="1"
        strokeDasharray="3 8" opacity="0.15" />

      {/* Latency label on edge */}
      {latencyMs > 0 && (
        <g>
          <rect x={cx - 18} y={cy - 8} width="36" height="14" rx="3"
            fill="rgba(0,0,0,0.6)" stroke="rgba(255,255,255,0.06)" strokeWidth="0.5" />
          <text x={cx} y={cy + 3} textAnchor="middle" fill="#71717a" fontSize="8"
            fontFamily="'JetBrains Mono', monospace">{latencyMs.toFixed(1)}ms</text>
        </g>
      )}

      {produceRate > 0 && (
        <>
          <DataParticle path={path} color="#06b6d4" delay={0} duration={2.5} />
          <DataParticle path={path} color="#06b6d4" delay={1.2} duration={2.5} />
        </>
      )}
      {consumeRate > 0 && (
        <DataParticle path={path} color="#a855f7" delay={0.6} duration={2.8} reverse />
      )}
    </g>
  );
}

/* ── Node Detail Popover ─────────────────────────────────────────────── */
function NodePopover({ node, position, onClose }: { node: any; position: NodePos; onClose: () => void }) {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.92, y: 8 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.92, y: 8 }}
      transition={{ duration: 0.15 }}
      className="absolute z-30 pointer-events-auto"
      style={{ left: position.x, top: position.y, transform: 'translate(-50%, 12px)' }}>
      <div className="glass-panel p-4 w-56" style={{ border: `1px solid ${node.color}25` }}>
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: node.color,
              boxShadow: node.status === 'LEADER' ? `0 0 8px ${node.color}` : undefined }} />
            <span className="text-xs font-bold text-white">{node.name}</span>
          </div>
          <span className={`node-badge ${
            node.status === 'LEADER' ? 'node-badge--leader' :
            node.status === 'CANDIDATE' ? 'node-badge--candidate' : 'node-badge--follower'
          }`}>{node.status}</span>
        </div>

        {/* Stats grid */}
        <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-[10px] mono">
          <div>
            <div className="text-zinc-600 mb-0.5">Commit Index</div>
            <div className="text-zinc-200 font-bold">{formatNum(node.commitIndex)}</div>
          </div>
          <div>
            <div className="text-zinc-600 mb-0.5">Last Applied</div>
            <div className="text-zinc-200 font-bold">{formatNum(node.lastApplied)}</div>
          </div>
          {node.status === 'LEADER' && (
            <>
              <div>
                <div className="text-zinc-600 mb-0.5">Produce Rate</div>
                <div className="text-cyan-400 font-bold">{formatNum(node.produceRate)}/s</div>
              </div>
              <div>
                <div className="text-zinc-600 mb-0.5">Consume Rate</div>
                <div className="text-purple-400 font-bold">{formatNum(node.consumeRate)}/s</div>
              </div>
              <div>
                <div className="text-zinc-600 mb-0.5">Throughput</div>
                <div className="text-zinc-200 font-bold">{node.throughputMBps.toFixed(2)} MB/s</div>
              </div>
            </>
          )}
          {node.replicationLag !== undefined && (
            <div>
              <div className="text-zinc-600 mb-0.5">Repl. Lag</div>
              <div className={`font-bold ${node.replicationLag > 10 ? 'text-amber-400' : 'text-emerald-400'}`}>
                {node.replicationLag} entries
              </div>
            </div>
          )}
        </div>

        {/* Close hint */}
        <div className="mt-3 pt-2 border-t border-white/5 text-center">
          <button onClick={onClose}
            className="text-[9px] text-zinc-600 hover:text-zinc-400 transition-colors cursor-pointer mono tracking-wide">
            Click away to close
          </button>
        </div>
      </div>
    </motion.div>
  );
}

/* ── Hover Tooltip ───────────────────────────────────────────────────── */
function NodeTooltip({ node, position }: { node: any; position: NodePos }) {
  return (
    <div className="absolute z-20 pointer-events-none"
      style={{ left: position.x, top: position.y, transform: 'translate(-50%, -100%) translateY(-12px)' }}>
      <div className="bg-black/90 border border-white/10 rounded-lg px-3 py-2 text-center backdrop-blur-sm"
        style={{ boxShadow: `0 0 12px ${node.color}15` }}>
        <div className="text-[10px] font-semibold text-white">{node.name}</div>
        <div className="mono text-[9px] text-zinc-500 mt-0.5">
          {node.status === 'LEADER' ? `${formatNum(node.produceRate)}/s` :
           node.replicationLag !== undefined ? `lag: ${node.replicationLag}` : 'synced'}
        </div>
        <div className="mono text-[8px] text-zinc-600 mt-0.5">click to inspect · drag to move</div>
      </div>
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════════
   Main Topology Component
   ══════════════════════════════════════════════════════════════════════ */
export default function ClusterTopology({ nodes, metrics, latencies }: any) {
  const sorted = [...nodes].sort((a: any, b: any) => a.id.localeCompare(b.id));
  const svgRef = useRef<SVGSVGElement>(null);

  // Triangle layout defaults
  const cxDef = 500, cyDef = 350, radius = 160;
  const defaultPositions: NodePos[] = [
    { x: cxDef, y: cyDef - radius },
    { x: cxDef - radius * 0.87, y: cyDef + radius * 0.5 },
    { x: cxDef + radius * 0.87, y: cyDef + radius * 0.5 },
  ];

  const [positions, setPositions] = useState<NodePos[]>(defaultPositions);
  const [dragging, setDragging] = useState<number | null>(null);
  const [selectedIdx, setSelectedIdx] = useState<number | null>(null);
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const dragOffset = useRef<{ dx: number; dy: number }>({ dx: 0, dy: 0 });

  // Convert screen coords to SVG viewBox coords
  const screenToSvg = useCallback((clientX: number, clientY: number): NodePos => {
    const svg = svgRef.current;
    if (!svg) return { x: 0, y: 0 };
    const pt = svg.createSVGPoint();
    pt.x = clientX;
    pt.y = clientY;
    const ctm = svg.getScreenCTM();
    if (!ctm) return { x: 0, y: 0 };
    const svgPt = pt.matrixTransform(ctm.inverse());
    return { x: svgPt.x, y: svgPt.y };
  }, []);

  // Convert SVG coords to screen position for HTML overlays
  const svgToScreen = useCallback((svgX: number, svgY: number): NodePos => {
    const svg = svgRef.current;
    if (!svg) return { x: 0, y: 0 };
    const pt = svg.createSVGPoint();
    pt.x = svgX;
    pt.y = svgY;
    const ctm = svg.getScreenCTM();
    if (!ctm) return { x: 0, y: 0 };
    const screenPt = pt.matrixTransform(ctm);
    const rect = svg.getBoundingClientRect();
    return { x: screenPt.x - rect.left, y: screenPt.y - rect.top };
  }, []);

  const handlePointerDown = useCallback((idx: number, e: React.PointerEvent) => {
    e.stopPropagation();
    const svgPt = screenToSvg(e.clientX, e.clientY);
    dragOffset.current = { dx: positions[idx].x - svgPt.x, dy: positions[idx].y - svgPt.y };
    setDragging(idx);
    (e.target as Element).setPointerCapture(e.pointerId);
  }, [positions, screenToSvg]);

  const handlePointerMove = useCallback((e: React.PointerEvent) => {
    if (dragging === null) return;
    const svgPt = screenToSvg(e.clientX, e.clientY);
    setPositions(prev => {
      const next = [...prev];
      next[dragging] = { x: svgPt.x + dragOffset.current.dx, y: svgPt.y + dragOffset.current.dy };
      return next;
    });
  }, [dragging, screenToSvg]);

  const handlePointerUp = useCallback(() => {
    setDragging(null);
  }, []);

  const handleNodeClick = useCallback((idx: number, e: React.MouseEvent) => {
    e.stopPropagation();
    setSelectedIdx(prev => prev === idx ? null : idx);
  }, []);

  const handleCanvasClick = useCallback(() => {
    setSelectedIdx(null);
  }, []);

  const handleResetLayout = useCallback(() => {
    setPositions(defaultPositions);
    setSelectedIdx(null);
  }, []);

  const [n1, n2, n3] = sorted;
  const latencyValues = [latencies.alphaBeta, latencies.betaGamma, latencies.raftRpcMs];

  return (
    <div className="topology-canvas flex-1 rounded-2xl overflow-hidden relative min-h-0"
      style={{ border: '1px solid rgba(255,255,255,0.04)' }} onClick={handleCanvasClick}>

      {/* Corner labels */}
      <div className="absolute top-4 left-5 z-10 flex items-center gap-2">
        <span className="section-label">Cluster Topology</span>
        <span className="mono text-[9px] text-zinc-600 bg-white/[0.03] px-2 py-0.5 rounded border border-white/5">
          {sorted.length} nodes
        </span>
      </div>

      {/* Controls */}
      <div className="absolute top-4 right-5 z-10 flex items-center gap-2">
        {latencies?.raftRpcMs > 0 && (
          <span className="mono text-[9px] text-zinc-500 bg-white/[0.03] px-2 py-1 rounded border border-white/5">
            Raft RPC: <span className="text-amber-400 font-semibold">{latencies.raftRpcMs.toFixed(1)}ms</span>
          </span>
        )}
        <button onClick={(e) => { e.stopPropagation(); handleResetLayout(); }}
          className="mono text-[9px] text-zinc-600 hover:text-zinc-400 bg-white/[0.03] hover:bg-white/[0.06] px-2 py-1 rounded border border-white/5 transition-colors cursor-pointer">
          Reset Layout
        </button>
      </div>

      {/* SVG Canvas */}
      <svg ref={svgRef} className="w-full h-full" viewBox="0 0 1000 700"
        preserveAspectRatio="xMidYMid meet"
        onPointerMove={handlePointerMove} onPointerUp={handlePointerUp}
        style={{ cursor: dragging !== null ? 'grabbing' : 'default' }}>
        <defs>
          <linearGradient id="connection-grad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#06b6d4" stopOpacity="0.6" />
            <stop offset="50%" stopColor="#8b5cf6" stopOpacity="0.4" />
            <stop offset="100%" stopColor="#a855f7" stopOpacity="0.6" />
          </linearGradient>
        </defs>

        {/* Orbital rings (centered on default position) */}
        <circle cx={cxDef} cy={cyDef} r={radius * 1.35} fill="none"
          stroke="rgba(255,255,255,0.02)" strokeWidth="1" />
        <circle cx={cxDef} cy={cyDef} r={radius * 0.6} fill="none"
          stroke="rgba(255,255,255,0.015)" strokeWidth="1" strokeDasharray="4 8" />

        {/* Dynamic connections */}
        {(() => {
          const isDead = (n: any) => n?.status === 'OFFLINE' || (n?.replicationLag !== undefined && (n.replicationLag < 0 || n.replicationLag > 10));
          const shouldBlock = (na: any, nb: any) => isDead(na) || isDead(nb);

          return (
            <>
              {n1 && n2 && <Connection x1={positions[0].x} y1={positions[0].y}
                x2={positions[1].x} y2={positions[1].y}
                produceRate={shouldBlock(n1, n2) ? 0 : metrics?.produceRate || 0}
                consumeRate={shouldBlock(n1, n2) ? 0 : metrics?.consumeRate || 0}
                latencyMs={latencyValues[0]} />}
              {n2 && n3 && <Connection x1={positions[1].x} y1={positions[1].y}
                x2={positions[2].x} y2={positions[2].y}
                produceRate={shouldBlock(n2, n3) ? 0 : metrics?.produceRate || 0}
                consumeRate={shouldBlock(n2, n3) ? 0 : metrics?.consumeRate || 0}
                latencyMs={latencyValues[1]} />}
              {n1 && n3 && <Connection x1={positions[0].x} y1={positions[0].y}
                x2={positions[2].x} y2={positions[2].y}
                produceRate={shouldBlock(n1, n3) ? 0 : metrics?.produceRate || 0}
                consumeRate={shouldBlock(n1, n3) ? 0 : metrics?.consumeRate || 0}
                latencyMs={latencyValues[2]} />}
            </>
          );
        })()}

        {/* Draggable nodes */}
        {sorted.map((node: any, idx: number) => (
          <ClusterNode key={node.id} x={positions[idx].x} y={positions[idx].y}
            color={node.color} label={node.name} status={node.status}
            isSelected={selectedIdx === idx} isHovered={hoveredIdx === idx && dragging === null}
            subLabel={node.status === 'LEADER' ? `${formatNum(node.produceRate)}/s` :
              node.replicationLag !== undefined && node.replicationLag < 0 ? 'offline' :
              node.replicationLag !== undefined && node.replicationLag > 0 ? `lag: ${formatNum(node.replicationLag)}` : 'synced'}
            onPointerDown={(e: React.PointerEvent) => handlePointerDown(idx, e)}
            onPointerEnter={() => setHoveredIdx(idx)}
            onPointerLeave={() => setHoveredIdx(null)}
            onClick={(e: React.MouseEvent) => handleNodeClick(idx, e)} />
        ))}
      </svg>

      {/* HTML Tooltip overlay */}
      {hoveredIdx !== null && selectedIdx !== hoveredIdx && dragging === null && sorted[hoveredIdx] && (
        <NodeTooltip node={sorted[hoveredIdx]}
          position={svgToScreen(positions[hoveredIdx].x, positions[hoveredIdx].y - 38)} />
      )}

      {/* HTML Detail Popover */}
      {selectedIdx !== null && sorted[selectedIdx] && (
        <NodePopover node={sorted[selectedIdx]}
          position={svgToScreen(positions[selectedIdx].x, positions[selectedIdx].y + 50)}
          onClose={() => setSelectedIdx(null)} />
      )}
    </div>
  );
}
