export function StatusBadge({ value }: { value?: string }) {
  const normalized = (value ?? "unknown").toLowerCase();
  const tone = ["running", "normal", "ok", "active", "succeeded"].includes(normalized) ? "good" : ["stopped", "failed", "flatten_only", "error"].includes(normalized) ? "bad" : "warn";
  return <span className={`status ${tone}`}><span aria-hidden />{value ?? "Unknown"}</span>;
}

export function Money({ cents, signed = false }: { cents?: number | null; signed?: boolean }) {
  if (cents === null || cents === undefined) return <span className="muted">Unavailable</span>;
  return <span className={cents < 0 ? "negative" : cents > 0 ? "positive" : ""}>{signed && cents > 0 ? "+" : ""}${(cents / 100).toFixed(2)}</span>;
}

export function Time({ value }: { value?: number | null }) {
  if (!value) return <span className="muted">Unavailable</span>;
  return <time dateTime={new Date(value).toISOString()}>{new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(value)}</time>;
}
