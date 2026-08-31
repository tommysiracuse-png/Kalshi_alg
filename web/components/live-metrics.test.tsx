import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MetricsResponse, RunMarketActivityResponse, RunMarketsResponse, SavedSession } from "@/lib/types";
import { LiveMetrics } from "./live-metrics";

const aggregate = (netMarkoutUnits: number, fills = 1, averageNetMarkoutPriceUnits = 100) => ({
  horizonMs: 30_000, grossMarkoutUnits: netMarkoutUnits + 10, feeUnits: 10, netMarkoutUnits,
  averageNetMarkoutPriceUnits, coveredFillCount: fills, coveredContractsUnits: fills * 100,
  totalFillCount: fills, pendingFillCount: 0, unavailableFillCount: 0, complete: true,
});

const fillMarkout = (netMarkoutUnits: number, signedMarkoutPriceUnits: number) => ({
  horizonMs: 30_000, capturedAtMs: 2_030_000, futureMidYesUnits: 5_100,
  signedMarkoutPriceUnits, grossMarkoutUnits: netMarkoutUnits + 10, feeUnits: 10, netMarkoutUnits,
});

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
        markoutsByHorizon: { "30000": aggregate(890, 2, 445) },
        firstFillAtMs: 1_999_000, lastFillAtMs: 2_000_000, coverage: { fillsComplete: true, ordersComplete: true }, warnings: [],
      }],
    };
    const activity: RunMarketActivityResponse = {
      generatedAt: 2_000_000, runId: "run-12345678", market: markets.items[0], source, warnings: [],
      fills: { totalCount: 125, truncated: true, items: [{ fillId: "fill-1", orderId: "order-1", filledAtMs: 2_000_000, side: "yes", contractsUnits: 100, matchedContractsUnits: 50, openContractsUnits: 50, timeToFillMs: 1_000, totalPaidUnits: 4_100, liquidationValueUnits: 5_000, unrealizedValueUnits: 5_100, realizedPnlUnits: 600, unrealizedPnlUnits: 300, fillPnlUnits: 900, markoutsByHorizon: { "30000": fillMarkout(290, 300) } }] },
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
      summary: { timesRun: 1, runtimeMs: 60_000, orders: 2, ordersPerMinute: 2, fills: 2, fillsPerMinute: 2, apiCalls: 4, apiErrors: 0, realizedCents: 1, unrealizedCents: 0, totalCents: 1, pnlComplete: true, outcomes: { running: 1 }, apiByComponent: { bots: 4 }, markoutsByHorizon: { "30000": aggregate(890, 2, 445) } },
      runs: [{ id: "run-12345678", sessionId: "session-1", sessionName: "Default", configurationVersion: 1, configuration: {} as SavedSession["configuration"], status: "running", createdAt: 1_900_000, startedAt: 1_900_000, heartbeatAt: 2_000_000, artifactPath: "/tmp/run", metrics: { runtimeMs: 100_000, orders: 2, fills: 2, totalCents: 1, apiCalls: 4, apiErrors: 0, markoutsByHorizon: { "30000": aggregate(890, 2, 445) } } }],
    };

    render(<LiveMetrics initial={initial} sessions={sessions} query="" />);
    expect(screen.getByLabelText("Markout horizon")).toHaveValue("30000");
    expect(screen.getByText("30s Net Markout")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Markout horizon"), { target: { value: "5000" } });
    expect(window.location.search).toContain("markout_horizon_ms=5000");
    fireEvent.change(screen.getByLabelText("Markout horizon"), { target: { value: "30000" } });
    await waitFor(() => expect(screen.getByLabelText("Fills")).toHaveValue("25"));
    expect(screen.getByLabelText("Orders")).toHaveValue("50");
    expect(screen.getByLabelText("Total Cost minimum")).toBeDisabled();
    expect(screen.getByLabelText("Markets: Name / Description / Link / Ticker")).toBeChecked();
    expect(screen.getByLabelText("Markets: Name / Description / Link / Ticker")).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: /Default.*1 run/i }));
    fireEvent.click(screen.getByRole("button", { name: /Expand run run-12345678/i }));
    expect(await screen.findByText("TEST-1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Expand TEST-1" }));
    expect(await screen.findByText("Newest 1 of 125")).toBeInTheDocument();
    expect(screen.getByText("Newest 1 of 70")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Signed markout" })).toBeInTheDocument();
    expect(screen.getAllByRole("columnheader", { name: "30s net markout" })).toHaveLength(3);
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

    const beforeColumns = request.mock.calls.length;
    fireEvent.click(screen.getByLabelText("Fills: Fill ID"));
    const dragData = new Map<string, string>();
    const dataTransfer = {
      effectAllowed: "none", dropEffect: "none",
      setData: (type: string, value: string) => dragData.set(type, value),
      getData: (type: string) => dragData.get(type) ?? "",
    } as unknown as DataTransfer;
    fireEvent.dragStart(screen.getByRole("columnheader", { name: "Total paid" }), { dataTransfer });
    expect(dragData.size).toBe(2);
    const fillTable = (activityView as HTMLElement).querySelector(".activity-table") as HTMLElement;
    const contractsHeader = [...fillTable.querySelectorAll("th")].find(header => header.textContent?.includes("Contracts")) as HTMLElement;
    fireEvent.dragOver(contractsHeader, { dataTransfer });
    fireEvent.drop(contractsHeader, { dataTransfer });
    expect(screen.queryByRole("columnheader", { name: "Fill ID" })).not.toBeInTheDocument();
    const fillHeaders = [...((activityView as HTMLElement).querySelector(".activity-table") as HTMLElement).querySelectorAll("th")].map(item => item.textContent);
    expect(fillHeaders).toEqual(["⋮⋮Fill time↓", "⋮⋮Side↕", "⋮⋮Contracts↕", "⋮⋮Total paid↕", "⋮⋮Time to fill↕", "⋮⋮Signed markout↕", "⋮⋮Future midpoint↕", "⋮⋮Fee↕", "⋮⋮30s net markout↕"]);
    await waitFor(() => {
      const stored = JSON.parse(window.localStorage.getItem("kalshi.metrics.columns.v1") ?? "null") as { tables: { fills: Array<{ id: string; enabled: boolean }> } };
      expect(stored.tables.fills.find(column => column.id === "fillId")?.enabled).toBe(false);
      expect(stored.tables.fills.map(column => column.id).indexOf("totalPaid")).toBeLessThan(stored.tables.fills.map(column => column.id).indexOf("timeToFill"));
    });
    expect(request.mock.calls.length).toBe(beforeColumns);

    cleanup();
    render(<LiveMetrics initial={initial} sessions={sessions} query="" />);
    fireEvent.click(screen.getByRole("button", { name: /Default.*1 run/i }));
    fireEvent.click(screen.getByRole("button", { name: /Expand run run-12345678/i }));
    expect(await screen.findByText("TEST-1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Expand TEST-1" }));
    expect(await screen.findByText("Newest 1 of 125")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Fill ID" })).not.toBeInTheDocument();
    for (const label of ["Order Time", "Order ID", "Side", "Contracts", "Time on Book", "Bid / Ask / Mid", "Order Price", "Latest State"]) {
      fireEvent.click(screen.getByLabelText(`Orders: ${label}`));
    }
    expect(screen.getByText("No columns enabled for Orders. Use Columns at the top of the page to enable one.")).toBeInTheDocument();
  });

  it("sorts each activity table and filters loaded rows without fetching", async () => {
    class MockEventSource {
      static OPEN = 1;
      readyState = 1;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      addEventListener() {}
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const source = { available: true, updatedAt: 4_000_000, stale: false };
    const market = (ticker: string, lastFillAtMs: number, totalCostUnits: number, contractsUnits: number) => ({
      ticker, description: `${ticker} market`, marketUrl: null, side: "YES" as const,
      yesContractsUnits: contractsUnits, noContractsUnits: 0, yesAverageCostPriceUnits: 4_000, noAverageCostPriceUnits: null,
      totalCostUnits, realizedPnlUnits: ticker === "NEW" ? 1_000 : 500, realizedReturnBps: 500,
      markoutsByHorizon: { "30000": aggregate(ticker === "NEW" ? 1_000 : 500, 2, 300) },
      fillCount: 2, orderCount: 2, firstFillAtMs: lastFillAtMs - 500, lastFillAtMs,
      coverage: { fillsComplete: true, ordersComplete: true }, warnings: [],
    });
    const markets: RunMarketsResponse = {
      generatedAt: 4_000_000, runId: "run-12345678", source, warnings: [],
      items: [market("OLD", 2_000_000, 10_000, 100), market("NEW", 4_000_000, 30_000, 300)],
    };
    const activity: RunMarketActivityResponse = {
      generatedAt: 4_000_000, runId: "run-12345678", market: markets.items[1], source, warnings: [],
      fills: { totalCount: 2, truncated: false, items: [
        { fillId: "fill-old", orderId: "order-old", filledAtMs: 2_000_000, side: "yes", contractsUnits: 50, matchedContractsUnits: 50, openContractsUnits: 0, timeToFillMs: 500, totalPaidUnits: 5_000, realizedPnlUnits: 100, unrealizedPnlUnits: 0, fillPnlUnits: 100, markoutsByHorizon: { "30000": fillMarkout(90, 100) } },
        { fillId: "fill-new", orderId: "order-new", filledAtMs: 4_000_000, side: "yes", contractsUnits: 200, matchedContractsUnits: 100, openContractsUnits: 100, timeToFillMs: 1_000, totalPaidUnits: 20_000, realizedPnlUnits: 600, unrealizedPnlUnits: 300, fillPnlUnits: 900, markoutsByHorizon: { "30000": fillMarkout(590, 300) } },
      ] },
      orders: { totalCount: 2, truncated: false, items: [
        { revisionKey: "revision-old", orderId: "order-old", placedAtMs: 1_500_000, side: "yes", contractsUnits: 100, timeOnBookMs: 500, bookBidPriceUnits: 3_900, bookAskPriceUnits: 4_100, bookMidPriceUnits: 4_000, orderPriceUnits: 4_000, endedState: "Filled" },
        { revisionKey: "revision-new", orderId: "order-new", placedAtMs: 3_500_000, side: "yes", contractsUnits: 200, timeOnBookMs: 1_000, bookBidPriceUnits: 4_900, bookAskPriceUnits: 5_100, bookMidPriceUnits: 5_000, orderPriceUnits: 5_000, endedState: "Resting" },
      ] },
    };
    const request = vi.spyOn(globalThis, "fetch").mockImplementation(async input => new Response(
      JSON.stringify(String(input).includes("/activity") ? activity : markets),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    const sessions: SavedSession[] = [{ id: "session-1", name: "Default", description: "", configuration: {} as SavedSession["configuration"], version: 1, createdAt: 1, updatedAt: 1, selected: true, runCount: 1 }];
    const initial: MetricsResponse = {
      generatedAt: 4_000_000,
      summary: { timesRun: 1, runtimeMs: 60_000, orders: 4, ordersPerMinute: 4, fills: 4, fillsPerMinute: 4, apiCalls: 4, apiErrors: 0, realizedCents: 1, unrealizedCents: 0, totalCents: 1, pnlComplete: true, outcomes: { stopped: 1 }, apiByComponent: { bots: 4 }, markoutsByHorizon: { "30000": aggregate(1_500, 4, 300) } },
      runs: [{ id: "run-12345678", sessionId: "session-1", sessionName: "Default", configurationVersion: 1, configuration: {} as SavedSession["configuration"], status: "stopped", createdAt: 1_000_000, startedAt: 1_000_000, endedAt: 4_000_000, artifactPath: "/tmp/run", metrics: { runtimeMs: 3_000_000, orders: 4, fills: 4, totalCents: 1, apiCalls: 4, apiErrors: 0, markoutsByHorizon: { "30000": aggregate(1_500, 4, 300) } } }],
    };

    render(<LiveMetrics initial={initial} sessions={sessions} query="session_id=session-1" filters={{ sessionId: "session-1", status: "", from: "", to: "" }} />);
    expect(screen.getByLabelText("Total Cost minimum")).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: /Default.*1 run/i }));
    fireEvent.click(screen.getByRole("button", { name: /Expand run run-12345678/i }));
    expect(await screen.findByText("NEW")).toBeInTheDocument();
    const marketRows = () => [...document.querySelectorAll(".market-metrics-table tbody>tr.expandable-position")].map(row => row.textContent ?? "");
    expect(marketRows()[0]).toContain("NEW");
    expect(screen.getByRole("columnheader", { name: "Last fill" })).toHaveAttribute("aria-sort", "descending");
    fireEvent.click(screen.getByRole("button", { name: "Last fill" }));
    expect(marketRows()[0]).toContain("OLD");

    fireEvent.click(screen.getByRole("button", { name: "Expand NEW" }));
    expect(await screen.findByText("fill-new")).toBeInTheDocument();
    const activityTables = () => [...document.querySelectorAll(".market-activity .activity-table table")];
    const rowTexts = (table: Element) => [...table.querySelectorAll("tbody>tr")].map(row => row.textContent ?? "");
    expect(rowTexts(activityTables()[0])[0]).toContain("fill-new");
    expect(rowTexts(activityTables()[1])[0]).toContain("order-new");
    fireEvent.click(screen.getByRole("button", { name: "Fill time" }));
    fireEvent.click(screen.getByRole("button", { name: "Order time" }));
    expect(rowTexts(activityTables()[0])[0]).toContain("fill-old");
    expect(rowTexts(activityTables()[1])[0]).toContain("order-old");

    const beforeFilters = request.mock.calls.length;
    fireEvent.change(screen.getByLabelText("Total Cost minimum"), { target: { value: "0.75" } });
    fireEvent.change(screen.getByLabelText("Average Markout minimum"), { target: { value: "2" } });
    expect(screen.queryByText("fill-old")).not.toBeInTheDocument();
    expect(screen.getByText("fill-new")).toBeInTheDocument();
    expect(screen.queryByText("order-old")).not.toBeInTheDocument();
    expect(screen.getByText("order-new")).toBeInTheDocument();
    expect(screen.getByText("OLD")).toBeInTheDocument();
    expect(request.mock.calls.length).toBe(beforeFilters);

    fireEvent.change(screen.getByLabelText("Total Cost minimum"), { target: { value: "1.50" } });
    expect(screen.getByText("No orders match the current filters.")).toBeInTheDocument();
    expect(request.mock.calls.length).toBe(beforeFilters);
    fireEvent.click(screen.getByRole("button", { name: "Clear filters" }));
    expect(screen.getByText("fill-old")).toBeInTheDocument();
    expect(screen.getByText("order-old")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Expand OLD" }));
    await waitFor(() => expect(screen.getAllByText("fill-new")).toHaveLength(2));
    const beforeDrag = request.mock.calls.length;
    const dragData = new Map<string, string>();
    const dataTransfer = {
      effectAllowed: "none", dropEffect: "none",
      setData: (type: string, value: string) => dragData.set(type, value),
      getData: (type: string) => dragData.get(type) ?? "",
    } as unknown as DataTransfer;
    fireEvent.dragStart(screen.getAllByRole("columnheader", { name: "Fill time" })[0], { dataTransfer });
    expect(dragData.size).toBe(2);
    fireEvent.dragOver(screen.getAllByRole("columnheader", { name: "Fill ID" })[0], { dataTransfer });
    fireEvent.drop(screen.getAllByRole("columnheader", { name: "Fill ID" })[0], { dataTransfer });
    const fillTables = [...document.querySelectorAll(".market-activity")].map(view => view.querySelector(".activity-table") as HTMLElement);
    expect(fillTables.map(table => table.querySelector("th")?.textContent)).toEqual(["⋮⋮Fill ID↕", "⋮⋮Fill ID↕"]);
    await waitFor(() => {
      const stored = JSON.parse(window.localStorage.getItem("kalshi.metrics.columns.v1") ?? "null") as { tables: { fills: Array<{ id: string }> } };
      expect(stored.tables.fills[0].id).toBe("fillId");
    });
    expect(request.mock.calls.length).toBe(beforeDrag);
  });

  it("sanitizes saved column preferences and enables newly known columns", async () => {
    class MockEventSource {
      static OPEN = 1;
      readyState = 1;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      addEventListener() {}
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    window.localStorage.setItem("kalshi.metrics.columns.v1", JSON.stringify({
      version: 2,
      tables: {
        markets: [{ id: "side", enabled: false }, { id: "identity", enabled: false }, { id: "removedColumn", enabled: false }],
        fills: [{ id: "fillId", enabled: false }],
        orders: "invalid",
      },
    }));
    const initial: MetricsResponse = {
      generatedAt: 1,
      summary: { timesRun: 0, runtimeMs: 0, orders: 0, ordersPerMinute: 0, fills: 0, fillsPerMinute: 0, apiCalls: 0, apiErrors: 0, realizedCents: 0, unrealizedCents: 0, totalCents: 0, pnlComplete: true, outcomes: {}, apiByComponent: {} },
      runs: [],
    };

    render(<LiveMetrics initial={initial} sessions={[]} query="" />);
    await waitFor(() => expect(screen.getByLabelText("Markets: Side")).not.toBeChecked());
    expect(screen.getByLabelText("Markets: Name / Description / Link / Ticker")).toBeChecked();
    expect(screen.getByLabelText("Markets: Avg Cost")).toBeChecked();
    expect(screen.getByLabelText("Fills: Fill ID")).not.toBeChecked();
    expect(screen.getByLabelText("Fills: Fill Time")).toBeChecked();
    expect(screen.getByLabelText("Orders: Order Time")).toBeChecked();
    await waitFor(() => {
      const stored = window.localStorage.getItem("kalshi.metrics.columns.v1") ?? "";
      expect(stored).not.toContain("removedColumn");
      expect(JSON.parse(stored).tables.markets[0]).toEqual({ id: "identity", enabled: true });
    });
  });
});
