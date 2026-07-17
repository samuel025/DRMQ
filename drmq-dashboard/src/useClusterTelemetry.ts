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

    const useWebSocket = import.meta.env.VITE_USE_WEBSOCKET === 'true';
    if (useWebSocket) {
      const defaultUrls = 'ws://localhost:9292,ws://localhost:9293,ws://localhost:9294';
      const wsUrlsString = import.meta.env.VITE_WEBSOCKET_URLS || defaultUrls;
      const wsUrls = wsUrlsString.split(',').map((u: string) => u.trim());
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
