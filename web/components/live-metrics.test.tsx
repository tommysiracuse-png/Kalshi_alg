import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MetricsResponse, RunMarketActivityResponse, RunMarketsResponse, SavedSession } from "@/lib/types";
import { LiveMetrics } from "./live-metrics";

afterEach(() => {
  cleanup();
  window.localStorage.clear();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("LiveMetrics", () => {
  it("renders nested fills before orders, persists caps, and shows truncation", async () => {
    class MockEventSource {
      static OPEN = 1;
      static listeners = new Map<string, (event: MessageEvent) => void>();
      readyState = 1;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      addEventListener(name: string, listener: EventListener) { MockEventSource.listeners.set(name, listener as (event: MessageEvent) => void); }
      static emit(name: string, payload: unknown) { MockEventSource.listeners.get(name)?.({ data: JSON.stringify(payload) } as MessageEvent); }
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    window.localStorage.setItem("kalshi.metrics.fillLimit", "25");
    window.localStorage.setItem("kalshi.metrics.orderLimit", "50");
    const source = { available: true, updatedAt: 2_000_000, stale: false };
    const markets: RunMarketsResponse = {
      generatedAt: 2_000_000, runId: "run-12345678", source, warnings: [],
      items: [{
        ticker: "TEST-1", description: "Test market", marketUrl: "https://kalshi.com/markets/test/test-market/test-event", side: "BOTH",
        yesContractsUnits: 100, noContractsUnits: 100, yesAverageCostPriceUnits: 4_000, noAverageCostPriceUnits: 5_000,
        totalCostUnits: 9_100, realizedPnlUnits: 900, realizedReturnBps: 989, fillCount: 2, orderCount: 2,
        firstFillAtMs: 1_999_000, lastFillAtMs: 2_000_000, coverage: { fillsComplete: true, ordersComplete: true }, warnings: [],
      }],
    };
    const activity: RunMarketActivityResponse = {
      generatedAt: 2_000_000, runId: "run-12345678", market: markets.items[0], source, warnings: [],
      fills: { totalCount: 125, truncated: true, items: [{ fillId: "fill-1", orderId: "order-1", filledAtMs: 2_000_000, side: "yes", contractsUnits: 100, matchedContractsUnits: 50, openContractsUnits: 50, timeToFillMs: 1_000, totalPaidUnits: 4_100, liquidationValueUnits: 5_000, unrealizedValueUnits: 5_100, realizedPnlUnits: 600, unrealizedPnlUnits: 300, fillPnlUnits: 900 }] },
      orders: { totalCount: 70, truncated: true, items: [{ revisionKey: "revision-1", orderId: "order-1", placedAtMs: 1_999_000, side: "yes", contractsUnits: 100, timeOnBookMs: 1_000, bookBidPriceUnits: 4_000, bookAskPriceUnits: 4_200, bookMidPriceUnits: 4_100, orderPriceUnits: 4_000, endedState: "Filled" }] },
    };
    const request = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
      const url = String(input);
      const body = url.includes("/activity") ? activity : markets;
      return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
    });
    const sessions: SavedSession[] = [{ id: "session-1", name: "Default", description: "", configuration: {} as SavedSession["configuration"], version: 1, createdAt: 1, updatedAt: 1, selected: true, runCount: 1 }];
    const initial: MetricsResponse = {
      generatedAt: 2_000_000,
      summary: { timesRun: 1, runtimeMs: 60_000, orders: 2, ordersPerMinute: 2, fills: 2, fillsPerMinute: 2, apiCalls: 4, apiErrors: 0, realizedCents: 1, unrealizedCents: 0, totalCents: 1, pnlComplete: true, outcomes: { running: 1 }, apiByComponent: { bots: 4 } },
      runs: [{ id: "run-12345678", sessionId: "session-1", sessionName: "Default", configurationVersion: 1, configuration: {} as SavedSession["configuration"], status: "running", createdAt: 1_900_000, startedAt: 1_900_000, heartbeatAt: 2_000_000, artifactPath: "/tmp/run", metrics: { runtimeMs: 100_000, orders: 2, fills: 2, totalCents: 1, apiCalls: 4, apiErrors: 0 } }],
    };

    render(<LiveMetrics initial={initial} sessions={sessions} query="" />);
    await waitFor(() => expect(screen.getByLabelText("Fills")).toHaveValue("25"));
    expect(screen.getByLabelText("Orders")).toHaveValue("50");
    fireEvent.click(screen.getByRole("button", { name: /Default.*1 run/i }));
    fireEvent.click(screen.getByRole("button", { name: /Expand run run-12345678/i }));
    expect(await screen.findByText("TEST-1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Expand TEST-1" }));
    expect(await screen.findByText("Newest 1 of 125")).toBeInTheDocument();
    expect(screen.getByText("Newest 1 of 70")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Realized P&L" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Unrealized P&L" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Liquidation value" })).not.toBeInTheDocument();
    expect(screen.getByText("0.50 matched · 0.50 open")).toBeInTheDocument();
    const activityView = screen.getByText("Newest 1 of 125").closest(".market-activity");
    expect(activityView).not.toBeNull();
    const headings = [...(activityView as HTMLElement).querySelectorAll(".nested-heading strong")];
    expect(headings.map(item => item.textContent)).toEqual(["Fills", "Orders"]);
    expect(request).toHaveBeenCalledWith(expect.stringContaining("fill_limit=25&order_limit=50"), { cache: "no-store" });

    fireEvent.change(screen.getByLabelText("Fills"), { target: { value: "250" } });
    expect(window.localStorage.getItem("kalshi.metrics.fillLimit")).toBe("250");
    await waitFor(() => expect(request).toHaveBeenCalledWith(expect.stringContaining("fill_limit=250&order_limit=50"), { cache: "no-store" }));

    const heartbeat = { generatedAt: 2_001_000, source, activeRun: { id: "run-12345678", sessionId: "session-1", status: "running", heartbeatAt: 2_001_000, activityRevision: "revision-1", summary: { runtimeMs: 101_000, orders: 3, fills: 2, apiCalls: 5, apiErrors: 0, totalCents: 1, pnlComplete: true }, source } };
    const beforeRevision = request.mock.calls.length;
    act(() => MockEventSource.emit("metrics_heartbeat", heartbeat));
    await waitFor(() => expect(request.mock.calls.length).toBe(beforeRevision + 2));
    const afterRevision = request.mock.calls.length;
    act(() => MockEventSource.emit("metrics_heartbeat", { ...heartbeat, generatedAt: 2_002_000, activeRun: { ...heartbeat.activeRun, heartbeatAt: 2_002_000, summary: { ...heartbeat.activeRun.summary, runtimeMs: 102_000 } } }));
    await act(async () => { await Promise.resolve(); });
    expect(request.mock.calls.length).toBe(afterRevision);
  });
});
