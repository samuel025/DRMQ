import React, { useState, useEffect } from 'react';
import { Panel } from '../components/DashboardWidgets';
import { RefreshCw, Database } from 'lucide-react';
import { DitherButton } from '../components/dither-kit';

interface TopicData {
  name: string;
  messageCount: number;
}

export default function Topics() {
  const [topics, setTopics] = useState<TopicData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchTopics = async () => {
    setLoading(true);
    setError(null);
    try {
      // For now, default to the local admin port of Broker-1 (9392)
      const res = await fetch('http://localhost:9392/api/topics');
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setTopics(data);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch topics');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTopics();
    const interval = setInterval(fetchTopics, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="p-6 h-full flex flex-col gap-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            <Database className="w-6 h-6 text-cyan-400" />
            Topic Explorer
          </h1>
          <p className="text-sm text-zinc-400 mt-1">Live visibility into DRMQ active topics</p>
        </div>
        <DitherButton color="blue" bloom="aura" onClick={fetchTopics} className="flex items-center gap-2 px-4 py-2">
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </DitherButton>
      </header>

      <Panel title="Active Topics" className="flex-1 overflow-hidden flex flex-col">
        {error && (
          <div className="bg-red-500/10 border border-red-500/20 text-red-400 p-4 rounded-xl mb-4 text-sm">
            {error}
          </div>
        )}

        <div className="flex-1 overflow-y-auto min-h-0 relative">
          <table className="w-full text-left border-collapse">
            <thead className="sticky top-0 bg-[#070a12] z-10 border-b border-white/5 shadow-[0_4px_20px_#000]">
              <tr>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Topic Name</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Message Count</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500 text-right">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {topics.length === 0 && !loading && !error && (
                <tr>
                  <td colSpan={3} className="py-8 text-center text-zinc-500 text-sm">
                    No active topics found.
                  </td>
                </tr>
              )}
              {topics.map(topic => (
                <tr key={topic.name} className="hover:bg-white/[0.02] transition-colors">
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-3">
                      <div className="w-2 h-2 rounded-full bg-cyan-500" />
                      <span className="font-mono text-sm text-zinc-200">{topic.name}</span>
                    </div>
                  </td>
                  <td className="py-3 px-4">
                    <span className="font-mono text-sm text-zinc-300">{topic.messageCount.toLocaleString()}</span>
                  </td>
                  <td className="py-3 px-4 text-right">
                    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                      Active
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
