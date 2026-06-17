import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Book, Server, Database, Network } from 'lucide-react';
import Dashboard from './pages/Dashboard';
import Documentation from './pages/Documentation';
import { useClusterTelemetry } from './useClusterTelemetry';

function Sidebar() {
  const location = useLocation();
  const telemetryState = useClusterTelemetry();
  
  const metrics = telemetryState?.metrics;
  const healthColor = metrics?.health === 'OPTIMAL' ? '#10b981' : metrics?.health === 'DEGRADED' ? '#f59e0b' : '#ef4444';

  return (
    <aside className="w-16 lg:w-60 shrink-0 bg-[#080d18]/90 border-r border-white/5 flex flex-col z-10 h-full">
      {/* Logo */}
      <div className="px-4 py-5 flex items-center gap-3 border-b border-white/5">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-purple-600 flex items-center justify-center shrink-0 shadow-[0_0_16px_rgba(6,182,212,0.4)]">
          <Server className="w-4 h-4 text-white" />
        </div>
        <div className="hidden lg:block">
          <h1 className="text-sm font-black tracking-widest text-white">DRMQ</h1>
          <p className="text-[9px] text-cyan-500 uppercase tracking-[0.2em] font-semibold">Message Broker</p>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 p-3 space-y-1">
        <Link to="/"
          className={`flex items-center justify-center lg:justify-start gap-3 px-3 py-2.5 rounded-lg text-sm transition-all ${
            location.pathname === '/' ? 'bg-cyan-500/10 text-white border border-cyan-500/20 shadow-[inset_2px_0_0_#06b6d4]'
                   : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/5'}`}>
          <LayoutDashboard className={`w-4 h-4 shrink-0 ${location.pathname === '/' ? 'text-cyan-400' : ''}`} />
          <span className="hidden lg:block font-medium tracking-wide">Telemetry</span>
        </Link>
        <Link to="/docs"
          className={`flex items-center justify-center lg:justify-start gap-3 px-3 py-2.5 rounded-lg text-sm transition-all ${
            location.pathname === '/docs' ? 'bg-cyan-500/10 text-white border border-cyan-500/20 shadow-[inset_2px_0_0_#06b6d4]'
                   : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/5'}`}>
          <Book className={`w-4 h-4 shrink-0 ${location.pathname === '/docs' ? 'text-cyan-400' : ''}`} />
          <span className="hidden lg:block font-medium tracking-wide">Documentation</span>
        </Link>
      </nav>

      {/* Cluster status badge */}
      {metrics && (
        <div className="p-4 border-t border-white/5">
          <div className="hidden lg:flex items-center gap-2 px-3 py-2 rounded-lg bg-black/30 border border-white/5">
            <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ backgroundColor: healthColor, boxShadow: `0 0 6px ${healthColor}` }} />
            <span className="text-[10px] font-mono text-zinc-400">Term <span className="text-white font-bold">{metrics.term}</span></span>
          </div>
        </div>
      )}
    </aside>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="h-screen bg-[#06080f] flex text-zinc-300 font-sans overflow-hidden">
        <Sidebar />
        <main className="flex-1 min-w-0 overflow-y-auto">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/docs" element={<Documentation />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
