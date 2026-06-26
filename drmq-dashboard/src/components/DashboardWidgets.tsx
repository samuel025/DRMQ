import { motion } from 'framer-motion';
import type { ReactNode } from 'react';

/* ── Stat Card ───────────────────────────────────────────────────────── */
export function StatCard({ icon: Icon, label, value, unit, color = '#06b6d4', sub }: any) {
  return (
    <div className="stat-card p-4 flex flex-col gap-1.5" style={{ '--accent-color': `${color}40` } as any}>
      <div className="flex items-center justify-between">
        <span className="section-label" style={{ fontSize: '9px' }}>{label}</span>
        <div className="w-7 h-7 rounded-lg flex items-center justify-center"
          style={{ background: `${color}10`, border: `1px solid ${color}15` }}>
          <Icon className="w-3.5 h-3.5" style={{ color }} />
        </div>
      </div>
      <div className="flex items-baseline gap-1.5">
        <span className="text-2xl font-semibold text-white tracking-tight">{value}</span>
        {unit && <span className="mono text-[10px] text-zinc-500">{unit}</span>}
      </div>
      {sub && <div className="mono text-[10px] text-zinc-600">{sub}</div>}
    </div>
  );
}

/* ── Sparkline Chart ─────────────────────────────────────────────────── */
export function SparkLine({ data, color, height = 40 }: { data: number[]; color: string; height?: number }) {
  if (!data || data.length < 2) return null;
  const w = 200, h = height;
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
  const fillPath = `${path} L ${w} ${h} L 0 ${h} Z`;
  const gradId = `spark-${color.replace('#', '')}`;

  return (
    <svg className="w-full" style={{ height }} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={fillPath} fill={`url(#${gradId})`} />
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      {/* Current value dot */}
      <circle cx={w} cy={h - (data[data.length - 1] / max) * h}
        r="2.5" fill={color} opacity="0.8" />
    </svg>
  );
}

/* ── Panel wrapper ───────────────────────────────────────────────────── */
export function Panel({ title, badge, children, className = '' }: {
  title: string; badge?: ReactNode; children: ReactNode; className?: string;
}) {
  return (
    <div className={`glass-panel p-5 ${className}`}>
      <div className="flex justify-between items-center mb-4">
        <h3 className="section-label">{title}</h3>
        {badge}
      </div>
      {children}
    </div>
  );
}

/* ── Progress bar with label ─────────────────────────────────────────── */
export function LabeledProgress({ label, value, max, color, suffix = '' }: {
  label: string; value: number; max: number; color: string; suffix?: string;
}) {
  const pct = Math.min(100, (value / max) * 100);
  return (
    <div>
      <div className="flex justify-between text-[10px] mb-1.5">
        <span className="text-zinc-500 font-medium">{label}</span>
        <span className="mono font-semibold" style={{ color }}>
          {value > 0 ? `${value.toFixed(2)}${suffix}` : '--'}
        </span>
      </div>
      <div className="progress-track">
        <motion.div className="progress-fill"
          style={{ backgroundColor: color }}
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
          transition={{ duration: 0.8, ease: [0.4, 0, 0.2, 1] }} />
      </div>
    </div>
  );
}

/* ── Number formatter ────────────────────────────────────────────────── */
export function formatNum(n: number, decimals = 0) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toFixed(decimals);
}
