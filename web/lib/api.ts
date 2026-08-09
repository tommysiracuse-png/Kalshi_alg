import "server-only";

const base = process.env.KALSHI_API_URL ?? "http://127.0.0.1:8001";

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`${base}${path}`, {
    headers: { "x-internal-token": process.env.KALSHI_UI_INTERNAL_TOKEN ?? "" },
    cache: "no-store"
  });
  if (!response.ok) throw new Error(`Operations API returned ${response.status}`);
  return response.json() as Promise<T>;
}

export function apiBase(): string { return base; }
