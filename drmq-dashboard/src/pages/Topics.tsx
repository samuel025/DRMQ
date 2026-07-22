import React, { useState, useEffect } from 'react';
import { Panel } from '../components/DashboardWidgets';
import { RefreshCw, Database } from 'lucide-react';
import { DitherButton } from '../components/dither-kit';
import { motion, AnimatePresence } from 'framer-motion';

interface TopicData {
  name: string;
  messageCount: number;
}

export default function Topics() {
  const [topics, setTopics] = useState<TopicData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [inspectorTopic, setInspectorTopic] = useState<string | null>(null);
  const [inspectOffset, setInspectOffset] = useState<number | string>(0);
  const [inspectTimestamp, setInspectTimestamp] = useState<string>('');
  const [inspectLimit, setInspectLimit] = useState<number | string>(10);
  const [messages, setMessages] = useState<any[]>([]);
  const [inspectLoading, setInspectLoading] = useState(false);
  const [inspectError, setInspectError] = useState<string | null>(null);

  const fetchMessages = async () => {
    if (!inspectorTopic) return;
    setInspectLoading(true);
    setInspectError(null);
    try {
      const offsetParam = inspectOffset === '' ? 0 : inspectOffset;
      const limitParam = inspectLimit === '' ? 1 : inspectLimit;
      
      let url = `http://localhost:9392/api/messages?topic=${inspectorTopic}&limit=${limitParam}`;
      if (inspectTimestamp) {
        const ts = new Date(inspectTimestamp).getTime();
        if (!isNaN(ts)) {
          url += `&timestamp=${ts}`;
        } else {
          url += `&offset=${offsetParam}`;
        }
      } else {
        url += `&offset=${offsetParam}`;
      }

      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setMessages(data);

      // Snap the input offset to the actual returned offset to show compaction jumps
      if (data && data.length > 0) {
        setInspectOffset(data[0].offset);
      }
    } catch (err: any) {
      setInspectError(err.message || 'Failed to fetch messages');
    } finally {
      setInspectLoading(false);
    }
  };

  // Automatically fetch messages when inspector opens
  useEffect(() => {
    if (inspectorTopic) {
      setInspectOffset(0); // Reset to 0 when opening new topic
      fetchMessages();
    } else {
      setMessages([]);
    }
  }, [inspectorTopic]);

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
        <button 
          onClick={fetchTopics} 
          className="flex items-center gap-2 px-4 py-2 bg-white/5 hover:bg-white/10 text-white border border-white/10 rounded-lg text-sm font-medium transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
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
                <th className="py-3 px-4 text-xs font-semibold uppercase tracking-wider text-zinc-500 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {topics.length === 0 && !loading && !error && (
                <tr>
                  <td colSpan={4} className="py-8 text-center text-zinc-500 text-sm">
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
                  <td className="py-3 px-4 text-right">
                    <button 
                      onClick={() => setInspectorTopic(topic.name)}
                      className="text-xs font-mono text-zinc-300 hover:text-white transition-colors bg-white/5 hover:bg-white/10 px-3 py-1.5 rounded border border-white/10 uppercase tracking-wider"
                    >
                      Inspect
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      
      {/* Live Message Inspector Drawer */}
      <AnimatePresence>
        {inspectorTopic && (
          <motion.div 
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: 'spring', damping: 25, stiffness: 200 }}
            className="fixed top-0 right-0 bottom-0 w-[500px] bg-[#070a12]/95 backdrop-blur-2xl border-l border-white/10 shadow-[0_0_80px_rgba(0,0,0,0.8)] flex flex-col z-50"
          >
            <header className="px-6 py-5 border-b border-white/10 flex items-center justify-between bg-black/20">
              <div>
                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                  <Database className="w-5 h-5 text-cyan-400" />
                  Inspect Topic
                </h2>
                <div className="mono text-[11px] text-cyan-400/70 mt-1">{inspectorTopic}</div>
              </div>
              <button 
                onClick={() => setInspectorTopic(null)} 
                className="w-8 h-8 flex items-center justify-center rounded-full bg-white/5 hover:bg-white/10 text-zinc-400 hover:text-white transition-colors"
              >
                ✕
              </button>
            </header>
            
            <div className="px-6 py-4 border-b border-white/10 bg-black/40 flex flex-col gap-3">
              <div className="flex items-end gap-3">
                <div className="flex flex-col gap-1.5 flex-1">
                  <label className="mono text-[10px] text-zinc-500 tracking-wider">SEEK BY OFFSET</label>
                  <input 
                    type="number" 
                    value={inspectOffset}
                    disabled={!!inspectTimestamp}
                    onChange={e => setInspectOffset(e.target.value === '' ? '' : parseInt(e.target.value))}
                    className="bg-black/40 border border-white/10 rounded px-3 py-2 text-sm text-white font-mono focus:outline-none focus:border-cyan-500/50 transition-colors disabled:opacity-30"
                    placeholder="e.g. 100"
                  />
                </div>
                <div className="flex flex-col gap-1.5 flex-1">
                  <label className="mono text-[10px] text-zinc-500 tracking-wider">OR SEEK BY TIME</label>
                  <input 
                    type="datetime-local" 
                    value={inspectTimestamp}
                    onChange={e => setInspectTimestamp(e.target.value)}
                    className="bg-black/40 border border-white/10 rounded px-3 py-2 text-sm text-white font-mono focus:outline-none focus:border-cyan-500/50 transition-colors [&::-webkit-calendar-picker-indicator]:invert"
                  />
                </div>
              </div>
              <div className="flex items-end gap-3">
                <div className="flex flex-col gap-1.5 flex-1">
                  <label className="mono text-[10px] text-zinc-500 tracking-wider">LIMIT</label>
                  <input 
                    type="number" 
                    value={inspectLimit}
                    onChange={e => setInspectLimit(e.target.value === '' ? '' : parseInt(e.target.value))}
                    className="bg-black/40 border border-white/10 rounded px-3 py-2 text-sm text-white font-mono focus:outline-none focus:border-cyan-500/50 transition-colors"
                  />
                </div>
                <div className="flex flex-col gap-1.5 flex-1">
                  <button 
                    onClick={fetchMessages} 
                    className="px-4 py-2 flex items-center justify-center gap-2 text-sm w-full bg-cyan-500/10 hover:bg-cyan-500/20 text-cyan-400 border border-cyan-500/20 transition-colors rounded"
                  >
                    <RefreshCw className={`w-3.5 h-3.5 ${inspectLoading ? 'animate-spin' : ''}`} />
                    Fetch Messages
                  </button>
                </div>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto p-6 flex flex-col gap-5 bg-[#030408]">
              {inspectError && (
                <div className="bg-red-500/10 border border-red-500/20 text-red-400 p-4 rounded-lg text-sm flex items-start gap-3">
                  <span className="mt-0.5">⚠️</span>
                  <div className="flex-1">{inspectError}</div>
                </div>
              )}
              
              {messages.length === 0 && !inspectLoading && !inspectError && (
                <div className="flex flex-col items-center justify-center py-12 gap-3 opacity-50">
                  <Database className="w-8 h-8 text-zinc-500" />
                  <div className="mono text-[11px] text-zinc-400 tracking-wider">NO MESSAGES FOUND</div>
                </div>
              )}
              
              <div className="space-y-4">
                {messages.map((msg, idx) => {
                  // Attempt to prettify JSON payloads
                  let formattedPayload = msg.payload;
                  try {
                    const parsed = JSON.parse(msg.payload);
                    formattedPayload = JSON.stringify(parsed, null, 2);
                  } catch (e) { /* ignore */ }

                  return (
                    <div key={`${msg.offset}-${idx}`} className="bg-[#0a0d14] border border-white/5 rounded-lg overflow-hidden group hover:border-white/10 transition-colors">
                      <header className="bg-white/[0.02] px-4 py-2.5 flex items-center justify-between border-b border-white/5">
                        <div className="flex items-center gap-2">
                          <span className="mono text-[9px] px-1.5 py-0.5 rounded bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
                            OFFSET {msg.offset}
                          </span>
                        </div>
                        <span className="mono text-[10px] text-zinc-500">{new Date(msg.storedAt).toISOString()}</span>
                      </header>
                      
                      {msg.key && (
                        <div className="px-4 py-2 border-b border-white/5 bg-black/20 flex items-center gap-2">
                          <span className="mono text-[9px] text-zinc-600">KEY</span>
                          <span className="font-mono text-xs text-amber-200/90">{msg.key}</span>
                        </div>
                      )}
                      
                      <div className="p-4 overflow-x-auto">
                        <pre className="text-[11px] leading-relaxed text-zinc-300 font-mono">
                          {formattedPayload}
                        </pre>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
