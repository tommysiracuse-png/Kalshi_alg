import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Monitoring, ScreenerRun, ScreenerRunSummary } from "@/lib/types";
import { formatDuration, LiveMonitoring, LiveScreener, ScreenerHistory } from "./live-monitoring";

afterEach(() => {
  cleanup();
  window.localStorage.clear();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const summary = (overrides: Partial<ScreenerRunSummary> = {}): ScreenerRunSummary => ({
  totalRuns: 2, succeeded: 1, failed: 0, interrupted: 0, running: 1,
  scannedMarkets: 30_000, apiRequests: 18, averageDurationMs: 2_000,
  added: 3, changed: 2, removed: 1, ...overrides,
});

const run = (overrides: Partial<ScreenerRun>): ScreenerRun => ({
  id: "screen-new", fleetRunId: "fleet-12345678", sessionId: "session-1", sessionName: "Newer", venue: "kalshi",
  status: "running", reason: "scheduled", startedAt: 3_000, endedAt: null, durationMs: null,
  generationId: 2, configuredLimit: 20_000, effectiveLimit: 20_000, scannedMarkets: 20_000,
  apiRequests: 12, apiErrors: 0, added: 2, changed: 1, removed: 0,
  inventoryCarried: 0, inventoryUnknown: 0, warnings: [], error: null, ...overrides,
});

const monitoring = (runs: ScreenerRun[], runSummary = summary(), nextCursor: string | null = null): Monitoring => ({
  generatedAt: 5_000,
  source: { available: true, updatedAt: 5_000, stale: false },
  warnings: [], manager: {}, clients: [],
  screener: { history: runs, historySummary: runSummary, historyNextCursor: nextCursor },
});

describe("monitoring formatting", () => {
  it("formats running durations", () => {
    expect(formatDuration(9_000)).toBe("9s");
    expect(formatDuration(125_000)).toBe("2m 5s");
    expect(formatDuration(3_720_000)).toBe("1h 2m");
  });

  it("handles missing durations", () => {
    expect(formatDuration(undefined)).toBe("Unavailable");
  });
});

describe("monitoring and screener pages", () => {
  it("renders screener activity only in the dedicated screener view", () => {
    class MockEventSource {
      static OPEN = 1;
      readyState = MockEventSource.OPEN;
      onerror: (() => void) | null = null;
      addEventListener() {}
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const initial = monitoring([]);

    render(<LiveMonitoring initial={initial} />);
    expect(screen.getByRole("heading", { name: "Monitoring" })).toBeInTheDocument();
    expect(screen.getAllByText("unavailable").length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByRole("heading", { name: "Refresh activity" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Historical runs" })).not.toBeInTheDocument();

    cleanup();
    render(<LiveScreener initial={initial} />);
    expect(screen.getByRole("heading", { name: "Screener" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Refresh activity" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Historical runs" })).toBeInTheDocument();
  });

  it("renders aggregate venue, shard, and market telemetry with configurable columns", async () => {
    class MockEventSource {
      static OPEN = 1; readyState = MockEventSource.OPEN; onerror: (() => void) | null = null;
      addEventListener() {} close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const initial: Monitoring = {
      ...monitoring([]), schemaVersion: 5,
      manager: {
        lifecycle: "running", activeVenues: ["kalshi", "polymarket"], configuredBots: 3, botsRunning: 2,
        pnl: { fills: 4, feesCents: 10, realizedCents: 125, unrealizedCents: 25, totalCents: 150, complete: true },
        apiActivity: { rest: { total: 30, requestsLast60s: 5, errors: 1 } },
      },
      venues: [{ venue: "kalshi", active: true, botsRunning: 2, configuredBots: 3, apiActivity: { rest: { total: 30, successes: 29, errors: 1, requestsLast60s: 5, averageLatencyMs: 12 }, stream: { connections: 1, message: 50, messagesLast60s: 8 } } }],
      workers: [{ venue: "kalshi", workerId: "worker-00", pid: 123, running: true, phase: "healthy", startedAtMs: 1_000, assignedMarkets: 1, marketIds: ["TEST-1"], botsRunning: 1, watchdog: { mode: "reduction_only", counts: { reduction_only: 1 } }, heartbeatAtMs: 4_900, stale: false, memoryRssBytes: 1024, queueDepth: 0, eventLagMs: 10 }],
      clients: [{ venue: "kalshi", workerId: "worker-00", marketId: "TEST-1", title: "Test market", pid: 123, lifecycle: "running", socketHealthy: true, runtime: { startedAtMs: 1_000 }, market: { priceUnits: 5_000, priceSource: "ticker", lastQuoteAtMs: 4_700 }, portfolio: { currentPositionUnits: 100 }, pnl: { fills: 1, feesCents: 1, realizedCents: 2, unrealizedCents: 3, totalCents: 5 }, fills: { count: 1, lastFillAtMs: 4_500 }, orderActivity: { byAction: { create: { attempts: 2 } }, lastCreateAtMs: 4_600 }, watchdog: { running: true, mode: "normal" } }],
    };
    render(<LiveMonitoring initial={initial} />);
    expect(screen.getByText("kalshi · polymarket")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Venue transport activity" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Shard health" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Market activity" })).toBeInTheDocument();
    expect(screen.getByText("TEST-1")).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("Markets: Last fill"));
    expect(screen.queryByRole("columnheader", { name: "Last fill" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Move REST total right in API" }));
    const restHeader = screen.getByRole("columnheader", { name: "REST total" });
    const handle = restHeader.querySelector(".metrics-column-resize-handle");
    fireEvent.pointerDown(handle as Element, { clientX: 100 });
    fireEvent.pointerMove(window, { clientX: 150 }); fireEvent.pointerUp(window, { clientX: 150 });
    await waitFor(() => expect(window.localStorage.getItem("kalshi.monitoring.columns.v1")).toContain("lastFill"));
  });

  it("keeps the connection live through an automatic SSE reconnect while heartbeats are fresh", () => {
    vi.useFakeTimers();
    try {
      const now = vi.spyOn(Date, "now").mockReturnValue(1_000);
      let monitoringListener: ((event: MessageEvent) => void) | undefined;
      let triggerError: (() => void) | undefined;
      class MockEventSource {
        static OPEN = 1;
        readyState = MockEventSource.OPEN;
        onerror: (() => void) | null = null;
        constructor() { triggerError = () => this.onerror?.(); }
        addEventListener(name: string, listener: EventListener) { if (name === "monitoring") monitoringListener = listener as (event: MessageEvent) => void; }
        close() {}
      }
      vi.stubGlobal("EventSource", MockEventSource);
      render(<LiveMonitoring initial={monitoring([])} />);

      act(() => monitoringListener?.({ data: JSON.stringify(monitoring([])) } as MessageEvent));
      expect(screen.getByText("Live", { exact: true })).toBeInTheDocument();

      act(() => triggerError?.());
      expect(screen.getByText("Live", { exact: true })).toBeInTheDocument();

      now.mockReturnValue(12_001);
      act(() => vi.advanceTimersByTime(1_000));
      expect(screen.getByText("Reconnecting", { exact: true })).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it("renders system and venue capacity telemetry and updates it live", async () => {
    let monitoringListener: ((event: MessageEvent) => void) | undefined;
    class MockEventSource {
      static OPEN = 1; readyState = MockEventSource.OPEN; onerror: (() => void) | null = null;
      addEventListener(name: string, listener: EventListener) { if (name === "monitoring") monitoringListener = listener as (event: MessageEvent) => void; }
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const initial: Monitoring = {
      ...monitoring([]),
      allocation: { admittedMarkets: 22, slotsRemaining: 453 },
      systemCapacity: {
        configuredMaxBots: 500, hardMaxBots: 500, effectiveMaxBots: 475, resourceCapacity: 500, healthCapacity: 475,
        reason: "worker_starvation", cpuPercent: 87.5, memoryPercent: 82.1, workerCpuPercent: 175,
        workerMemoryRssBytes: 2 * 1024 ** 3, maxEventLoopLagMs: 650, maxQueueWaitMs: 1200,
        starvedWorkers: 2, totalWorkers: 20, unhealthySamples: 2, healthySinceMs: null,
        lastReducedAtMs: 10_000, lastRecoveredAtMs: null, activeWorkerBudget: 475,
        workersRetained: 19, workersScaledDown: 1,
      },
      venueCapacity: {
        kalshi: {
          venueCapacityLimit: 255, venueQuoteSideCapacity: 510, admittedMarkets: 22, admittedQuoteSides: 44, writeRefillRate: 300, venueCapacityLimited: true, venueCapacityReason: "capacity",
          rateLimit: {
            source: "kalshi_broker", updatedAtMs: 5_000, windowSeconds: 60, partialWindow: true,
            read: { available: 470, capacity: 500, tokensLast60s: 180, spendPerSecond: 3, refillPerSecond: 10, headroomPerSecond: 7 },
            write: { available: 80, capacity: 100, tokensLast60s: 120, spendPerSecond: 2, refillPerSecond: 5, headroomPerSecond: 3 },
          },
        },
        polymarket: { venue_capacity_limit: 475, normal_quote_side_capacity: 950, admitted_markets: 12, admitted_quote_sides: 24, write_refill_rate: 600, venue_capacity_limited: false },
      },
    };
    render(<LiveMonitoring initial={initial} />);
    expect(screen.getByRole("heading", { name: "Fleet capacity" })).toBeInTheDocument();
    expect(screen.getAllByRole("tab").map(tab => tab.textContent)).toEqual(["Capacity", "System Limits", "Venue Limits"]);
    expect(screen.getByRole("tab", { name: "Capacity" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getAllByText("worker-starvation limited").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("475").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/2\.0 GB/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "System Limits" }));
    expect(screen.getByRole("heading", { name: "Scaling diagnostics" })).toBeInTheDocument();
    expect(screen.getByText("650 ms")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Venue Limits" }));
    expect(screen.getByText("Venue/API limited")).toBeInTheDocument();
    expect(screen.getByText("polymarket")).toBeInTheDocument();
    expect(screen.getByText("600")).toBeInTheDocument();
    expect(screen.getByText("470 / 500")).toBeInTheDocument();
    expect(screen.getByText("80 / 100")).toBeInTheDocument();
    expect(screen.getByText("partial 60s window")).toBeInTheDocument();

    const updated = { ...initial, systemCapacity: { ...initial.systemCapacity, effectiveMaxBots: 450, reason: "memory", memoryPercent: 90, workersScaledDown: 2 } };
    monitoringListener?.({ data: JSON.stringify(updated) } as MessageEvent);
    await waitFor(() => expect(screen.getByText("450")).toBeInTheDocument());
    expect(screen.getAllByText("memory limited").length).toBeGreaterThanOrEqual(1);
  });
});

describe("ScreenerHistory", () => {
  it("renders, sorts, reorders, hides, and resizes the requested columns", async () => {
    const older = run({ id: "screen-old", fleetRunId: "fleet-87654321", sessionName: "Older", status: "succeeded", startedAt: 1_000, endedAt: 2_000, durationMs: 1_000, scannedMarkets: 10_000, apiRequests: 6 });
    render(<ScreenerHistory initial={monitoring([older, run({})])} clockMs={5_000} onLoadMore={vi.fn()} />);

    const requested = ["Venue", "Session run in", "Status of screener run", "Time Started", "Time Ended", "Markets to Screen", "API Requests to Venue", "Duration", "Latest Changes"];
    expect(screen.getAllByRole("columnheader").map(header => header.textContent?.replace(/[⋮↕↑↓]/g, ""))).toEqual(requested);
    const rowText = () => [...document.querySelectorAll(".screener-history-table tbody tr")].map(row => row.textContent ?? "");
    expect(rowText()[0]).toContain("Newer");
    fireEvent.click(screen.getByRole("button", { name: "Time Started" }));
    expect(rowText()[0]).toContain("Older");

    fireEvent.click(screen.getByLabelText("Screener runs: Time Ended"));
    expect(screen.queryByRole("columnheader", { name: "Time Ended" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Move Session run in right in Screener runs" }));
    expect(screen.getAllByRole("columnheader")[2]).toHaveTextContent("Session run in");

    const sessionHeader = screen.getByRole("columnheader", { name: "Session run in" });
    const resizeHandle = sessionHeader.querySelector(".metrics-column-resize-handle");
    expect(resizeHandle).not.toBeNull();
    fireEvent.pointerDown(resizeHandle as Element, { clientX: 100 });
    fireEvent.pointerMove(window, { clientX: 160 });
    fireEvent.pointerUp(window, { clientX: 160 });
    expect(sessionHeader).toHaveStyle({ width: "300px" });
    await waitFor(() => {
      const stored = JSON.parse(window.localStorage.getItem("kalshi.screener.widths.v1") ?? "null") as { widths: Record<string, number> };
      expect(stored.widths.session).toBe(300);
    });
  });

  it("updates live rows and summary, shows legacy values safely, and appends pagination", async () => {
    const current = run({});
    const loadMore = vi.fn().mockResolvedValue({
      items: [run({ id: "legacy", fleetRunId: "fleet-legacy00", sessionName: "Legacy", status: "failed", startedAt: 500, endedAt: 900, durationMs: 400, scannedMarkets: null, apiRequests: null, error: "venue unavailable" })],
      nextCursor: null,
    });
    const { rerender } = render(<ScreenerHistory initial={monitoring([current], summary({ totalRuns: 1, succeeded: 0, running: 1 }), "cursor-1")} clockMs={5_000} onLoadMore={loadMore} />);
    expect(screen.getAllByText("2s").length).toBeGreaterThan(0);

    const completed = run({ status: "succeeded", endedAt: 6_000, durationMs: 3_000 });
    rerender(<ScreenerHistory initial={monitoring([completed], summary({ totalRuns: 1, succeeded: 1, running: 0 }), "cursor-1")} clockMs={6_000} onLoadMore={loadMore} />);
    await waitFor(() => expect(screen.getAllByText("succeeded").length).toBeGreaterThan(0));
    expect(screen.getAllByText("3s").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Load older runs" }));
    expect(await screen.findByText("Legacy")).toBeInTheDocument();
    expect(screen.getByText("venue unavailable")).toHaveAttribute("title", "venue unavailable");
    expect(screen.getAllByText("Unavailable").length).toBeGreaterThanOrEqual(2);
    expect(loadMore).toHaveBeenCalledWith("cursor-1");
  });
});
