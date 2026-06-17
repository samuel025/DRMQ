import { useState, useEffect, useRef } from 'react';
import type { TelemetryState, TelemetryProvider } from './types/telemetry';
import { MockTelemetryProvider } from './services/telemetry/MockTelemetryProvider';
import { WebSocketTelemetryProvider } from './services/telemetry/WebSocketTelemetryProvider';

export function useClusterTelemetry() {
  const [data, setData] = useState<TelemetryState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const providerRef = useRef<TelemetryProvider | null>(null);

  useEffect(() => {
    // Phase 2: Switch between Mock and WebSocket based on environment
    // For now, we default to MockProvider until the backend is ready.
    const useWebSocket = import.meta.env.VITE_USE_WEBSOCKET === 'true';
    
    if (useWebSocket) {
      const defaultUrls = 'ws://localhost:9292,ws://localhost:9293,ws://localhost:9294';
      const wsUrlsString = import.meta.env.VITE_WEBSOCKET_URLS || defaultUrls;
      const wsUrls = wsUrlsString.split(',').map((u: string) => u.trim());
      
      providerRef.current = new WebSocketTelemetryProvider(wsUrls);
    } else {
      providerRef.current = new MockTelemetryProvider();
    }

    providerRef.current.connect((newData) => {
      setData(newData);
      setError(null);
    }, (errMsg) => {
      setError(errMsg);
    });

    return () => {
      if (providerRef.current) {
        providerRef.current.disconnect();
      }
    };
  }, []);

  return { data, error };
}
