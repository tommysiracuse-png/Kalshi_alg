import type { OptimizerOptions, OptimizerStartPayload } from "./types";

// Form state for the Optimization tab's start form. Numeric inputs are kept as
// strings (what the <input> holds); buildStartPayload converts them.
export type OptimizerFormState = {
  tier: 1 | 2; recordRoot: string; marketClass: string; onlyParams: string[];
  topParams: string; workers: string; splits: string; fillShare: string; seed: string;
  baseSession: string; screenerSession: string; screenerFilter: boolean; screenerEvalHours: string;
  writeBack: boolean; useBaseParams: boolean;
  candidates: string; markets: string; budgetMinutes: string;
  // "all" | "7" | "14" | "30" | "custom"
  dataPeriod: string; fromDate: string; toDate: string;
  queueAfterCurrent: boolean;
};

export const PREVIOUS_SESSION = "$previous";

export const DEFAULT_OPTIMIZER_FORM: OptimizerFormState = {
  tier: 1, recordRoot: "record_data", marketClass: "", onlyParams: [],
  topParams: "25", workers: "8", splits: "3", fillShare: "0.5", seed: "",
  baseSession: "", screenerSession: "", screenerFilter: true, screenerEvalHours: "",
  writeBack: true, useBaseParams: true,
  candidates: "300", markets: "80", budgetMinutes: "120",
  dataPeriod: "all", fromDate: "", toDate: "", queueAfterCurrent: false,
};

function numberOrUndefined(value: string): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

// Payload for POST /api/v1/controls/optimizer/start. Blank inputs are omitted
// so the API applies its own defaults; the screener session is omitted when
// blank because optimizer.main then falls back to the base session's section.
export function buildStartPayload(form: OptimizerFormState): OptimizerStartPayload {
  const timeRange: Pick<OptimizerStartPayload, "lastDays" | "fromDate" | "toDate"> = form.dataPeriod === "custom"
    ? { fromDate: form.fromDate || undefined, toDate: form.toDate || undefined }
    : form.dataPeriod !== "all" && Number.isFinite(Number(form.dataPeriod))
      ? { lastDays: Number(form.dataPeriod) }
      : {};
  const payload: OptimizerStartPayload = {
    tier: form.tier,
    recordRoot: form.tier === 2 ? (form.recordRoot.trim() || "record_data") : undefined,
    marketClass: form.marketClass || undefined,
    onlyParams: form.onlyParams.length ? [...form.onlyParams] : undefined,
    topParams: numberOrUndefined(form.topParams),
    workers: numberOrUndefined(form.workers),
    splits: numberOrUndefined(form.splits),
    fillShare: numberOrUndefined(form.fillShare),
    seed: numberOrUndefined(form.seed),
    baseSession: form.baseSession || undefined,
    screenerSession: form.screenerSession || undefined,
    screenerFilter: form.screenerFilter,
    screenerEvalHours: numberOrUndefined(form.screenerEvalHours),
    writeBack: form.writeBack,
    useBaseParams: form.useBaseParams,
    candidates: numberOrUndefined(form.candidates),
    markets: numberOrUndefined(form.markets),
    budgetMinutes: numberOrUndefined(form.budgetMinutes),
    ...timeRange,
    queueAfterCurrent: form.queueAfterCurrent,
  };
  // Drop undefined keys so JSON.stringify output and tests stay tidy.
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== undefined)) as OptimizerStartPayload;
}

type PreviewContext = Pick<OptimizerOptions, "commandPrefix" | "outputDir" | "baseParamsWinnerAvailable">;

const FALLBACK_PREFIX = ["python", "-m", "optimizer.main", "--history", "history_data/history.sqlite3"];

// Mirrors OptimizerService._build_command's flag order so the preview reads
// like the argv the API will actually spawn. Values only known at launch
// (seed, the exported base-params file) show as placeholders.
export function buildCommandArgv(form: OptimizerFormState, context?: PreviewContext | null): string[] {
  const payload = buildStartPayload(form);
  const prefix = context?.commandPrefix?.length ? context.commandPrefix : FALLBACK_PREFIX;
  const outputDir = context?.outputDir || "runtime/optimizer";
  const argv = [...prefix, "--tier", String(payload.tier ?? 1)];
  if (payload.tier === 2) argv.push("--record-root", payload.recordRoot ?? "record_data");
  argv.push(
    "--workers", String(payload.workers ?? 8),
    "--seed", payload.seed !== undefined ? String(payload.seed) : "<launch-time>",
    "--top-params", String(payload.topParams ?? 25),
    "--output-dir", outputDir,
  );
  if (payload.candidates !== undefined) argv.push("--candidates", String(payload.candidates));
  if (payload.markets !== undefined) argv.push("--markets", String(payload.markets));
  if (payload.budgetMinutes !== undefined) argv.push("--budget-minutes", String(payload.budgetMinutes));
  if (payload.splits !== undefined) argv.push("--splits", String(payload.splits));
  if (payload.fillShare !== undefined) argv.push("--fill-share", String(payload.fillShare));
  if (payload.marketClass) argv.push("--class", payload.marketClass);
  if (payload.onlyParams?.length) argv.push("--only-params", payload.onlyParams.join(","));
  if (payload.baseSession) argv.push("--base-session", payload.baseSession);
  if (payload.screenerSession) argv.push("--screener-session", payload.screenerSession);
  if (payload.screenerFilter === false) argv.push("--no-screener-filter");
  if (payload.screenerEvalHours !== undefined) argv.push("--screener-eval-hours", String(payload.screenerEvalHours));
  if (payload.baseSession) argv.push("--base-params-json", `${outputDir}/base_params_<launch-id>.json`);
  else if (payload.useBaseParams && context?.baseParamsWinnerAvailable) argv.push("--base-params-json", `${outputDir}/base_params_winner.json`);
  if (payload.writeBack) argv.push("--write-back");
  if (payload.lastDays !== undefined) argv.push("--last-days", String(payload.lastDays));
  if (payload.fromDate) argv.push("--from-date", payload.fromDate);
  if (payload.toDate) argv.push("--to-date", payload.toDate);
  return argv;
}

function quoteArg(arg: string): string {
  return /[\s"]/.test(arg) ? `"${arg.replace(/"/g, "\\\"")}"` : arg;
}

export function buildCommandPreview(form: OptimizerFormState, context?: PreviewContext | null): string {
  return buildCommandArgv(form, context).map(quoteArg).join(" ");
}
