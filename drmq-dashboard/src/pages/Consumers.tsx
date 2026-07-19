import React, { useState, useEffect } from 'react';
import { Panel } from '../components/DashboardWidgets';
import { RefreshCw, Users, AlertCircle, CheckCircle2 } from 'lucide-react';
import { DitherButton } from '../components/dither-kit';

interface ConsumerTopic {
  topic: string;
  committedOffset: number;
  lag: number;
  activeMembers: number;
}

interface ConsumerGroup {
  groupId: string;
  topics: ConsumerTopic[];
}

export default function Consumers() {
  const [groups, setGroups] = useState<ConsumerGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchConsumers = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('http://localhost:9392/api/consumers');
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setGroups(data);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch consumers');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConsumers();
    const interval = setInterval(fetchConsumers, 5000);
    return () => clearInterval(interval);
  }, []);

  const getLagColor = (lag: number) => {
    if (lag === 0) return 'text-emerald-400';
    if (lag < 100) return 'text-amber-400';
    return 'text-red-400';
  };

  return (
    <div className="p-6 h-full flex flex-col gap-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            <Users className="w-6 h-6 text-purple-400" />
            Consumer Groups & Lag
          </h1>
          <p className="text-sm text-zinc-400 mt-1">Monitor message processing lag across all consumer groups</p>
        </div>
        <DitherButton color="purple" bloom="aura" onClick={fetchConsumers} className="flex items-center gap-2 px-4 py-2">
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </DitherButton>
      </header>

      <Panel title="Active Groups" className="flex-1 overflow-hidden flex flex-col">
        {error && (
          <div className="bg-red-500/10 border border-red-500/20 text-red-400 p-4 rounded-xl mb-4 text-sm">
            {error}
          </div>
        )}

        <div className="flex-1 overflow-y-auto min-h-0 relative">
          <table className="w-full text-left border-collapse">
            <thead className="sticky top-0 bg-[#070a12] z-10 border-b border-white/5 shadow-[0_4px_20px_#000]">
              <tr>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Group ID</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Topic</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Offset</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500">Consumer Lag</th>
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500 text-right">Members</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {groups.length === 0 && !loading && !error && (
                <tr>
                  <td colSpan={5} className="py-8 text-center text-zinc-500 text-sm">
                    No consumer groups registered.
                  </td>
                </tr>
              )}
              {groups.flatMap((group) => 
                group.topics.map((topic, i) => (
                  <tr key={`${group.groupId}-${topic.topic}`} className="hover:bg-white/[0.02] transition-colors">
                    {i === 0 ? (
                      <td className="py-3 px-4" rowSpan={group.topics.length}>
                        <span className="font-mono text-sm text-zinc-200 bg-white/5 px-2 py-1 rounded">
                          {group.groupId}
                        </span>
                      </td>
                    ) : null}
                    <td className="py-3 px-4">
                      <span className="font-mono text-sm text-zinc-400">{topic.topic}</span>
                    </td>
                    <td className="py-3 px-4">
                      <span className="font-mono text-sm text-zinc-300">{topic.committedOffset.toLocaleString()}</span>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-2">
                        {topic.lag === 0 ? (
                          <CheckCircle2 className="w-4 h-4 text-emerald-500/70" />
                        ) : (
                          <AlertCircle className={`w-4 h-4 ${topic.lag > 100 ? 'text-red-500/70' : 'text-amber-500/70'}`} />
                        )}
                        <span className={`font-mono text-sm font-bold ${getLagColor(topic.lag)}`}>
                          {topic.lag.toLocaleString()}
                        </span>
                      </div>
                    </td>
                    <td className="py-3 px-4 text-right">
                      <span className="inline-flex items-center justify-center min-w-[2rem] px-2 py-1 rounded-full text-xs font-bold bg-white/5 text-zinc-300 border border-white/10">
                        {topic.activeMembers}
                      </span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
