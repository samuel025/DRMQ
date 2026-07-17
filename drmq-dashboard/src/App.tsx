import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Book, Server, ChevronRight } from 'lucide-react';
import { useState, useCallback } from 'react';
import Dashboard from './pages/Dashboard';
import { useClusterTelemetry } from './useClusterTelemetry';

function Sidebar({ telemetryState }: { telemetryState: any }) {
  const location = useLocation();
  const metrics = telemetryState?.metrics;
  const healthColor = metrics?.health === 'OPTIMAL' ? '#10b981' : metrics?.health === 'DEGRADED' ? '#f59e0b' : '#ef4444';

  const navItems = [
    { to: '/', icon: LayoutDashboard, label: 'Telemetry' },
    { to: '/docs', icon: Book, label: 'Documentation' },
  ];

  return (
    <aside className="w-16 lg:w-[220px] shrink-0 flex flex-col z-10 h-full"
      style={{
        background: 'linear-gradient(180deg, rgba(7,10,18,0.95), rgba(0,0,0,0.98))',
        borderRight: '1px solid rgba(255,255,255,0.04)',
      }}>

      {/* Logo */}
      <div className="px-4 py-5 flex items-center gap-3" style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
        <div className="w-8 h-8 rounded-xl flex items-center justify-center shrink-0"
          style={{
            background: 'linear-gradient(135deg, #52525b, #3f3f46)',
            boxShadow: '0 0 20px rgba(113,113,122,0.25)',
          }}>
          <Server className="w-4 h-4 text-white" />
        </div>
        <div className="hidden lg:block">
          <h1 className="text-sm font-black tracking-[0.15em] text-white">DRMQ</h1>
          <p className="text-[9px] uppercase tracking-[0.2em] font-semibold"
            style={{ color: '#a1a1aa' }}>Message Broker</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-3 space-y-1">
        {navItems.map(({ to, icon: Icon, label }) => {
          const active = location.pathname === to;
          return (
            <Link key={to} to={to} aria-label={label}
              aria-current={active ? "page" : undefined}
              className="flex items-center justify-center lg:justify-start gap-3 px-3 py-2.5 rounded-xl text-sm transition-all duration-200 cursor-pointer group"
              style={active ? {
                background: 'rgba(255,255,255,0.06)',
                border: '1px solid rgba(255,255,255,0.1)',
                color: '#fff',
              } : {
                background: 'transparent',
                border: '1px solid transparent',
                color: '#71717a',
              }}
              onMouseEnter={e => {
                if (!active) {
                  e.currentTarget.style.color = '#a1a1aa';
                  e.currentTarget.style.background = 'rgba(255,255,255,0.03)';
                }
              }}
              onMouseLeave={e => {
                if (!active) {
                  e.currentTarget.style.color = '#71717a';
                  e.currentTarget.style.background = 'transparent';
                }
              }}>
              <Icon className={`w-4 h-4 shrink-0 ${active ? 'text-zinc-100' : ''}`} />
              <span className="hidden lg:block font-medium tracking-wide">{label}</span>
              {active && <ChevronRight className="hidden lg:block w-3 h-3 ml-auto text-zinc-500/50" />}
            </Link>
          );
        })}
      </nav>

      {/* Cluster status */}
      {metrics && (
        <div className="p-3" style={{ borderTop: '1px solid rgba(255,255,255,0.04)' }}>
          <div className="hidden lg:flex items-center gap-2.5 px-3 py-2.5 rounded-xl"
            style={{ background: 'rgba(255,255,255,0.02)', border: '1px solid rgba(255,255,255,0.04)' }}>
            <span className="w-1.5 h-1.5 rounded-full animate-pulse"
              style={{ backgroundColor: healthColor, boxShadow: `0 0 6px ${healthColor}` }} />
            <span className="mono text-[10px] text-zinc-500">
              Term <span className="text-white font-bold">{metrics.term}</span>
            </span>
          </div>
        </div>
      )}
    </aside>
  );
}

export default function App() {
  const [paused, setPaused] = useState(false);
  const { data: telemetryState, error: telemetryError, provider } = useClusterTelemetry();

  const handleTogglePause = useCallback(() => {
    setPaused(prev => {
      const next = !prev;
      if (next) provider.disconnect();
      else provider.reconnect();
      return next;
    });
  }, [provider]);

  return (
    <BrowserRouter>
      <div className="h-screen flex overflow-hidden" style={{ background: '#000', color: '#d4d4d8', fontFamily: "'Inter', sans-serif" }}>
        <Sidebar telemetryState={telemetryState} />
        <main className="flex-1 min-w-0 overflow-y-auto">
          <Routes>
            <Route path="/" element={
              <Dashboard
                telemetryState={telemetryState}
                telemetryError={telemetryError}
                paused={paused}
                onTogglePause={handleTogglePause}
              />
            } />
            <Route path="/docs" element={
              <iframe
                src="https://drmq.vercel.app"
                className="w-full h-full border-0"
                title="DRMQ Documentation"
                style={{ minHeight: '100vh' }}
              />
            } />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
