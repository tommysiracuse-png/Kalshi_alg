import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  id: "screen-new", fleetRunId: "fleet-12345678", sessionId: "session-1", sessionName: "Newer",
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
    expect(screen.queryByRole("heading", { name: "Refresh activity" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Historical runs" })).not.toBeInTheDocument();

    cleanup();
    render(<LiveScreener initial={initial} />);
    expect(screen.getByRole("heading", { name: "Screener" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Refresh activity" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Historical runs" })).toBeInTheDocument();
  });
});

describe("ScreenerHistory", () => {
  it("renders, sorts, reorders, hides, and resizes the requested columns", async () => {
    const older = run({ id: "screen-old", fleetRunId: "fleet-87654321", sessionName: "Older", status: "succeeded", startedAt: 1_000, endedAt: 2_000, durationMs: 1_000, scannedMarkets: 10_000, apiRequests: 6 });
    render(<ScreenerHistory initial={monitoring([older, run({})])} clockMs={5_000} onLoadMore={vi.fn()} />);

    const requested = ["Session run in", "Status of screener run", "Time Started", "Time Ended", "Markets to Screen", "API Requests to Venue", "Duration", "Latest Changes"];
    expect(screen.getAllByRole("columnheader").map(header => header.textContent?.replace(/[⋮↕↑↓]/g, ""))).toEqual(requested);
    const rowText = () => [...document.querySelectorAll(".screener-history-table tbody tr")].map(row => row.textContent ?? "");
    expect(rowText()[0]).toContain("Newer");
    fireEvent.click(screen.getByRole("button", { name: "Time Started" }));
    expect(rowText()[0]).toContain("Older");

    fireEvent.click(screen.getByLabelText("Screener runs: Time Ended"));
    expect(screen.queryByRole("columnheader", { name: "Time Ended" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Move Session run in right in Screener runs" }));
    expect(screen.getAllByRole("columnheader")[1]).toHaveTextContent("Session run in");

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
