"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export const LIVE_HEARTBEAT_TIMEOUT_MS = 10_000;
const LIVE_HEARTBEAT_CHECK_MS = 1_000;

/**
 * Tracks data liveness independently from the EventSource transport state.
 * EventSource emits `error` during normal automatic reconnects, so a recent
 * valid payload must keep the UI live until the heartbeat watchdog expires.
 */
export function useLiveHeartbeat() {
  const [connected, setConnected] = useState(false);
  const lastHeartbeatAt = useRef<number | null>(null);

  const markHeartbeat = useCallback((healthy = true) => {
    if (!healthy) {
      setConnected(false);
      return;
    }
    lastHeartbeatAt.current = Date.now();
    setConnected(true);
  }, []);

  const markStreamError = useCallback(() => {
    const heartbeatAt = lastHeartbeatAt.current;
    if (heartbeatAt == null || Date.now() - heartbeatAt > LIVE_HEARTBEAT_TIMEOUT_MS) setConnected(false);
  }, []);

  useEffect(() => {
    const watchdog = window.setInterval(() => {
      const heartbeatAt = lastHeartbeatAt.current;
      if (heartbeatAt == null || Date.now() - heartbeatAt > LIVE_HEARTBEAT_TIMEOUT_MS) setConnected(false);
    }, LIVE_HEARTBEAT_CHECK_MS);
    return () => window.clearInterval(watchdog);
  }, []);

  return { connected, markHeartbeat, markStreamError };
}
