import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { SessionEditor } from "./session-editor";
import type { SavedSession, ScreenerConfiguration, ScreenerFilters } from "@/lib/types";

// vitest runs without `globals`, so testing-library's automatic afterEach
// cleanup is not registered; without this each render stacks up in the DOM.
afterEach(cleanup);

const session: SavedSession = {
  id: "session-1", name: "Default", description: "Test profile", version: 1,
  createdAt: 1, updatedAt: 1, archivedAt: null, selected: true, runCount: 2,
  configuration: {
    schemaVersion: 1,
    execution: { useDemo: false, dryRun: false, subaccount: 0 },
    launcher: { fixedTicker: "", maxBots: 40, yesBudgetCents: 100 },
    watchdog: { intervalSeconds: 60, flattenRetries: 2 },
    bot: { post_only_quotes: true, markout_horizons_seconds: [1, 5, 30] },
  },
};

// Schema v4 screener section with the kalshi_screener_config.py defaults plus one keyword.
const screener: ScreenerConfiguration = {
  status: "open", mveFilter: "exclude", maxMarketsToScan: 20000, topN: 200,
  minSpreadCents: 4, maxSpreadCents: 35, minYesBidCents: 5, minNoBidCents: 5,
  minVol24h: 500, minOpenInterest: 100, minTimeToCloseHours: 3, maxTimeToCloseHours: 50,
  excludedTickerKeywords: ["LOWT"], targetEdgeCents: 2, quoteSize: 50,
  markoutFilterEnabled: true, markoutFilterNetThresholdCents: -1, markoutFilterTickerMinFills: 3,
  markoutFilterSeriesMinFills: 20, markoutFilterHorizonSeconds: 5, markoutFilterLookbackDays: 14,
  markoutFilterTotalNetThresholdCents: -300,
};
const v4Session: SavedSession = { ...session, configuration: { ...session.configuration, schemaVersion: 4, screener } };

describe("SessionEditor", () => {
  it("renders typed configuration sections and active-run guidance", () => {
    render(<SessionEditor initial={[session]} activeRun={{ id: "run-1", sessionId: session.id, sessionName: session.name, status: "running" }} />);
    expect(screen.getByRole("heading", { name: "Launcher" })).toBeInTheDocument();
    expect(screen.getByLabelText("Max Bots")).toHaveAttribute("type", "number");
    expect(screen.getByLabelText("Post only quotes")).toHaveAttribute("type", "checkbox");
    expect(screen.getByText(/apply only to the next start/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Select" })).toBeDisabled();
    // Pre-v4 sessions carry no screener section and render no Screener panel.
    expect(screen.queryByRole("heading", { name: "Screener" })).not.toBeInTheDocument();
  });

  it("renders the screener section with selects, numbers, toggle and an editable keyword chip list", () => {
    render(<SessionEditor initial={[v4Session]} />);
    expect(screen.getByRole("heading", { name: "Screener" })).toBeInTheDocument();
    expect(screen.getByText(/applies at the next fleet start/i)).toBeInTheDocument();
    expect(screen.getByLabelText("Status")).toHaveValue("open");
    expect(screen.getByLabelText("MVE filter")).toHaveValue("exclude");
    expect(screen.getByLabelText("Min spread (cents)")).toHaveValue(4);
    expect(screen.getByLabelText("Markout filter enabled")).toBeChecked();
    // The retired fee buffer field is gone from the editor.
    expect(screen.queryByLabelText(/fee buffer/i)).not.toBeInTheDocument();

    const chips = screen.getByRole("group", { name: "Excluded ticker keywords" });
    expect(within(chips).getByText("LOWT")).toBeInTheDocument();

    // Adding a keyword upper-cases and de-duplicates; removing drops the chip.
    const input = screen.getByLabelText("Add excluded ticker keyword");
    fireEvent.change(input, { target: { value: "rain" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(within(chips).getByText("RAIN")).toBeInTheDocument();
    fireEvent.change(input, { target: { value: "lowt" } });
    fireEvent.click(screen.getByRole("button", { name: "Add keyword" }));
    expect(within(chips).getAllByText("LOWT")).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: "Remove LOWT" }));
    expect(within(chips).queryByText("LOWT")).not.toBeInTheDocument();
    expect(screen.getByText("Unsaved changes")).toBeInTheDocument();

    // Selects and numbers update the draft as typed values.
    fireEvent.change(screen.getByLabelText("Status"), { target: { value: "closed" } });
    expect(screen.getByLabelText("Status")).toHaveValue("closed");
    fireEvent.change(screen.getByLabelText("Min spread (cents)"), { target: { value: "6" } });
    expect(screen.getByLabelText("Min spread (cents)")).toHaveValue(6);
  });

  it("shows independent Kalshi and Polymarket market scan limits", () => {
    const venueLimited: SavedSession = {
      ...v4Session,
      configuration: {
        ...v4Session.configuration,
        schemaVersion: 5,
        screener: {
          general: screener as ScreenerFilters,
          venues: { kalshi: { maxMarketsToScan: 12000 }, polymarket: { maxMarketsToScan: 2500 } },
        } as ScreenerConfiguration,
      },
    };
    render(<SessionEditor initial={[venueLimited]} />);
    expect(screen.getByLabelText("Kalshi max markets to scan")).toHaveValue(12000);
    expect(screen.getByLabelText("Polymarket max markets to scan")).toHaveValue(2500);
    fireEvent.change(screen.getByLabelText("Polymarket max markets to scan"), { target: { value: "1500" } });
    expect(screen.getByLabelText("Polymarket max markets to scan")).toHaveValue(1500);
    expect(screen.getByLabelText("Kalshi max markets to scan")).toHaveValue(12000);
  });

  it("restricts the markout horizon to the recorded horizons through a select", () => {
    render(<SessionEditor initial={[v4Session]} />);
    const select = screen.getByLabelText("Markout horizon (seconds)");
    expect(select.tagName).toBe("SELECT");
    expect(select).toHaveValue("5");
    expect(within(select).getAllByRole("option").map(option => option.getAttribute("value"))).toEqual(["1", "5", "30", "120"]);
    fireEvent.change(select, { target: { value: "30" } });
    expect(select).toHaveValue("30");
    expect(screen.getByText("Unsaved changes")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("flags a stored horizon outside the recorded set and blocks saving until one is chosen", () => {
    const stale: SavedSession = { ...v4Session, configuration: { ...v4Session.configuration, screener: { ...screener, markoutFilterHorizonSeconds: 10 } } };
    render(<SessionEditor initial={[stale]} />);
    const select = screen.getByLabelText("Markout horizon (seconds)");
    expect(select).toHaveValue("10");
    expect(select).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByRole("alert")).toHaveTextContent(/recorded horizons/);
    expect(screen.getByRole("button", { name: "Save session" })).toBeDisabled();
    fireEvent.change(select, { target: { value: "120" } });
    expect(select).toHaveValue("120");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save session" })).toBeEnabled();
  });

  it("holds an emptied numeric input invalid instead of posting 0", () => {
    render(<SessionEditor initial={[v4Session]} />);
    const spread = screen.getByLabelText("Min spread (cents)");
    fireEvent.change(spread, { target: { value: "" } });
    // The draft keeps its previous value (nothing to save), the field shows
    // an inline message and the save button is locked.
    expect(spread).toHaveValue(null);
    expect(spread).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByRole("alert")).toHaveTextContent(/enter a number/i);
    expect(screen.getByText("1 invalid field")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save session" })).toBeDisabled();
    expect(screen.queryByText("Unsaved changes")).not.toBeInTheDocument();

    // A second emptied box (a generic section this time) is counted too.
    const maxBots = screen.getByLabelText("Max Bots");
    fireEvent.change(maxBots, { target: { value: "" } });
    expect(screen.getAllByRole("alert")).toHaveLength(2);
    expect(screen.getByText("2 invalid fields")).toBeInTheDocument();

    // Typing numbers again clears the state and commits the typed values.
    fireEvent.change(spread, { target: { value: "6" } });
    fireEvent.change(maxBots, { target: { value: "45" } });
    expect(spread).toHaveValue(6);
    expect(spread).not.toHaveAttribute("aria-invalid");
    expect(maxBots).toHaveValue(45);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByText("Unsaved changes")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save session" })).toBeEnabled();
  });
});
