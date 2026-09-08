import { SessionEditor } from "@/components/session-editor";
import { apiGet } from "@/lib/api";
import type { SavedSession } from "@/lib/types";

export default async function SessionsPage() {
  const data = await apiGet<{ items: SavedSession[]; activeRun?: { id: string; sessionId: string; sessionName: string; status: string } | null }>("/api/v1/sessions?include_archived=true");
  return <><header className="page-header"><div><span className="eyebrow">LAUNCH PROFILES</span><h1>Sessions</h1><p>Save launcher and strategy configurations. Every start records an immutable run snapshot. Per-venue screener market limits are in each session&apos;s Screener panel.</p></div></header><SessionEditor initial={data.items} activeRun={data.activeRun} /></>;
}
