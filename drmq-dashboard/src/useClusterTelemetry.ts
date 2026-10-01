import { useState, useEffect, useRef, useCallback } from 'react';
import type { TelemetryState, TelemetryProvider } from './types/telemetry';
import { MockTelemetryProvider } from './services/telemetry/MockTelemetryProvider';
import { WebSocketTelemetryProvider } from './services/telemetry/WebSocketTelemetryProvider';

export function useClusterTelemetry() {
  const [data, setData] = useState<TelemetryState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const providerRef = useRef<TelemetryProvider | null>(null);
  const onDataRef = useRef<((d: TelemetryState) => void) | null>(null);
  const onErrorRef = useRef<((e: string) => void) | null>(null);

  useEffect(() => {
    const onData = (newData: TelemetryState) => { setData(newData); setError(null); };
    const onError = (errMsg: string) => { setError(errMsg); };
    onDataRef.current = onData;
    onErrorRef.current = onError;

    const searchParams = new URLSearchParams(window.location.search);
    const queryWs = searchParams.get('ws');
    const storedWs = typeof localStorage !== 'undefined' ? localStorage.getItem('drmq_ws_urls') : null;

    // Save custom ws URL to localStorage if provided via ?ws=
    if (queryWs && typeof localStorage !== 'undefined') {
      localStorage.setItem('drmq_ws_urls', queryWs);
    }

    // Default to true for WebSocket unless explicitly mock
    const useMock = import.meta.env.VITE_USE_MOCK === 'true' || import.meta.env.VITE_USE_WEBSOCKET === 'false';
    if (!useMock) {
      const host = window.location.hostname || 'localhost';
      const defaultUrls = `ws://${host}:9292,ws://${host}:9293,ws://${host}:9294`;
      const wsUrlsString = queryWs || storedWs || import.meta.env.VITE_WEBSOCKET_URLS || defaultUrls;
      const wsUrls = wsUrlsString.split(',').map((u: string) => u.trim()).filter(Boolean);
      providerRef.current = new WebSocketTelemetryProvider(wsUrls);
    } else {
      providerRef.current = new MockTelemetryProvider();
    }

    providerRef.current.connect(onData, onError);

    return () => {
      providerRef.current?.disconnect();
    };
  }, []);

  const disconnect = useCallback(() => {
    providerRef.current?.disconnect();
  }, []);

  const reconnect = useCallback(() => {
    if (!providerRef.current || !onDataRef.current) return;
    providerRef.current.connect(onDataRef.current, onErrorRef.current ?? undefined);
  }, []);

  return { data, error, provider: { disconnect, reconnect } };
}
