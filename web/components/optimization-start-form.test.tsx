import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useState } from "react";
import { OptimizationStartForm } from "./optimization-start-form";
import { DEFAULT_OPTIMIZER_FORM, buildCommandPreview, buildStartPayload, type OptimizerFormState } from "../lib/optimizer-command";
import type { OptimizerOptions, OptimizerParams } from "../lib/types";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const options: OptimizerOptions = {
  classes: ["toxic", "thinWide", "thickCalm", "default"],
  sessions: [
    { id: "s1", name: "Default", hasScreener: true, selected: true },
    { id: "s2", name: "base one", hasScreener: false, selected: false },
  ],
  tiers: [
    { tier: 1, label: "Tier 1", available: true },
    { tier: 2, label: "Tier 2", available: false },
  ],
  recorder: { available: false, hours: 0, days: 0, markets: 0 },
  workers: { default: 8, max: 12 },
  presets: { toxicity: ["default_toxicity_cents", "strong_edge_threshold_cents"], priceFloors: ["minimum_best_bid_cents_required_to_quote"] },
  previousSessionToken: "$previous",
  baseParamsWinnerAvailable: true,
  commandPrefix: ["C:/venv/python.exe", "-m", "optimizer.main", "--history", "C:/ws/history_data/history.sqlite3"],
  outputDir: "C:/ws/runtime/optimizer",
};

const params: OptimizerParams = {
  tier: 1,
  searchable: [
    { name: "default_toxicity_cents", kind: "float", low: 0.5, high: 8, default: 2, group: "Toxicity & edge" },
    { name: "strong_edge_threshold_cents", kind: "int", low: 1, high: 12, default: 3, group: "Toxicity & edge" },
    { name: "minimum_best_bid_cents_required_to_quote", kind: "int", low: 3, high: 60, default: 15, group: "Price floors" },
    { name: "fair_value_mid_weight", kind: "float", low: 0, high: 1, default: 0.5, group: "Fair value" },
  ],
  pinned: [{ name: "minimum_top_level_depth_contracts", reason: "awaiting Tier-2 data" }],
  presets: { toxicity: ["default_toxicity_cents", "strong_edge_threshold_cents"], priceFloors: ["minimum_best_bid_cents_required_to_quote"] },
  groups: ["Toxicity & edge", "Price floors", "Fair value"],
};

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
}

function mockFetch(overrides: Partial<OptimizerOptions> = {}) {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.includes("/optimizer/options")) return jsonResponse({ ...options, ...overrides });
    if (url.includes("/optimizer/params")) return jsonResponse({ ...params, tier: Number(new URL(url, "http://test").searchParams.get("tier") ?? 1) });
    return new Response("{}", { status: 404 });
  });
}

function Harness({ onState, initial }: { onState?: (state: OptimizerFormState) => void; initial?: Partial<OptimizerFormState> }) {
  const [value, setValue] = useState<OptimizerFormState>({ ...DEFAULT_OPTIMIZER_FORM, ...initial });
  return <OptimizationStartForm value={value} onChange={next => { setValue(next); onState?.(next); }} disabled={false} />;
}

function preview(): string {
  return screen.getByLabelText("Command line preview").textContent ?? "";
}

describe("OptimizationStartForm", () => {
  it("preset buttons select the group and the preview shows --only-params", async () => {
    mockFetch();
    const states: OptimizerFormState[] = [];
    render(<Harness onState={state => states.push(state)} />);
    const toxicity = await screen.findByRole("button", { name: "Toxicity group" });
    await waitFor(() => expect(toxicity).toBeEnabled());
    fireEvent.click(toxicity);
    expect(screen.getByRole("button", { name: "Remove default_toxicity_cents" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove strong_edge_threshold_cents" })).toBeInTheDocument();
    expect(preview()).toContain("--only-params default_toxicity_cents,strong_edge_threshold_cents");
    expect(states.at(-1)?.onlyParams).toEqual(["default_toxicity_cents", "strong_edge_threshold_cents"]);

    fireEvent.click(screen.getByRole("button", { name: "Price floors" }));
    expect(preview()).toContain("--only-params minimum_best_bid_cents_required_to_quote");
    expect(preview()).not.toContain("default_toxicity_cents");

    // Individual checkboxes add to the selection; Clear empties it.
    fireEvent.click(screen.getByRole("checkbox", { name: /fair_value_mid_weight/ }));
    expect(preview()).toContain("--only-params minimum_best_bid_cents_required_to_quote,fair_value_mid_weight");
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(preview()).not.toContain("--only-params");
    expect(screen.getByLabelText("Top params")).toBeEnabled();
  });

  it("preview mirrors tier, class, sessions and the screener toggle in argv order", async () => {
    mockFetch();
    const states: OptimizerFormState[] = [];
    render(<Harness onState={state => states.push(state)} />);
    await screen.findByRole("button", { name: "Toxicity group" });
    await waitFor(() => expect(preview()).toContain("C:/venv/python.exe -m optimizer.main --history C:/ws/history_data/history.sqlite3 --tier 1 --workers 8 --seed <launch-time> --top-params 25 --output-dir C:/ws/runtime/optimizer"));
    // Seeded from the last winner while no base session is chosen.
    expect(preview()).toContain("--base-params-json C:/ws/runtime/optimizer/base_params_winner.json");

    fireEvent.change(screen.getByLabelText("Market class"), { target: { value: "toxic" } });
    fireEvent.change(screen.getByLabelText("Base session"), { target: { value: "base one" } });
    fireEvent.change(screen.getByLabelText("Seed"), { target: { value: "11" } });
    fireEvent.change(screen.getByLabelText("Data period"), { target: { value: "7" } });
    expect(preview()).toContain("--seed 11");
    expect(preview()).toContain("--class toxic");
    expect(preview()).toContain('--base-session "base one"');
    expect(preview()).toContain("--base-params-json C:/ws/runtime/optimizer/base_params_<launch-id>.json");
    expect(preview()).not.toContain("base_params_winner.json");
    expect(preview()).toContain("--write-back --last-days 7");
    expect(preview()).not.toContain("--no-screener-filter");

    fireEvent.click(screen.getByLabelText(/^Screener filter/));
    expect(preview()).toContain("--no-screener-filter");
    expect(screen.getByLabelText("Screener session")).toBeDisabled();

    fireEvent.click(screen.getByLabelText("Queue after current run"));
    const payload = buildStartPayload(states.at(-1)!);
    expect(payload).toMatchObject({ tier: 1, marketClass: "toxic", baseSession: "base one", screenerFilter: false, seed: 11, lastDays: 7, queueAfterCurrent: true, writeBack: true });
    expect(payload).not.toHaveProperty("screenerSession");
    expect(payload).not.toHaveProperty("onlyParams");
  });

  it("disables Tier 2 without recorded coverage and enables it with coverage", async () => {
    mockFetch();
    const { unmount } = render(<Harness initial={{ tier: 2 }} />);
    const tier2 = await screen.findByLabelText(/^Tier 2/);
    await waitFor(() => expect(tier2).toBeDisabled());
    expect(screen.getByText(/No recorded order-book coverage/)).toBeInTheDocument();
    // A Tier-2 selection made before the options arrived falls back to Tier 1.
    await waitFor(() => expect(screen.getByLabelText(/^Tier 1/)).toBeChecked());
    expect(preview()).toContain("--tier 1");
    expect(preview()).not.toContain("--record-root");
    unmount();
    vi.restoreAllMocks();

    mockFetch({
      tiers: [{ tier: 1, label: "Tier 1", available: true }, { tier: 2, label: "Tier 2", available: true }],
      recorder: { available: true, hours: 14, days: 2, markets: 150, fromMs: Date.UTC(2026, 8, 1, 5), toMs: Date.UTC(2026, 8, 2, 19) },
    });
    render(<Harness />);
    const enabled = await screen.findByLabelText(/^Tier 2/);
    await waitFor(() => expect(enabled).toBeEnabled());
    expect(screen.getByText(/150 markets · 14 recorded hours over 2 days/)).toBeInTheDocument();
    fireEvent.click(enabled);
    expect(preview()).toContain("--tier 2 --record-root record_data");
    expect(screen.getByLabelText("Record root")).toHaveValue("record_data");
  });
});

describe("optimizer-command helpers", () => {
  it("omits blank inputs and maps the data period", () => {
    const payload = buildStartPayload({ ...DEFAULT_OPTIMIZER_FORM, candidates: "", seed: " ", screenerEvalHours: "6", dataPeriod: "custom", fromDate: "2026-08-25", toDate: "" });
    expect(payload).toEqual({
      tier: 1, topParams: 25, workers: 8, splits: 3, fillShare: 0.5, screenerFilter: true, screenerEvalHours: 6,
      writeBack: true, useBaseParams: true, markets: 80, budgetMinutes: 120, fromDate: "2026-08-25", queueAfterCurrent: false,
    });
  });

  it("falls back to a generic prefix and quotes arguments with spaces", () => {
    const text = buildCommandPreview({ ...DEFAULT_OPTIMIZER_FORM, baseSession: "my session", onlyParams: ["a", "b"], screenerFilter: false, fillShare: "0.25" });
    expect(text.startsWith("python -m optimizer.main --history history_data/history.sqlite3 --tier 1 --workers 8 --seed <launch-time> --top-params 25 --output-dir runtime/optimizer")).toBe(true);
    expect(text).toContain('--fill-share 0.25 --only-params a,b --base-session "my session" --no-screener-filter --base-params-json runtime/optimizer/base_params_<launch-id>.json --write-back');
  });
});
