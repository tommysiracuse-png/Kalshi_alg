import { apiGet } from "@/lib/api";
import { StatusBadge, Time } from "@/components/status";

type Audit = { id: number; request_id: string; timestamp_ms: number; action: string; target?: string; operator: string; result: string; error?: string };
export default async function ActivityPage() {
  const data = await apiGet<{ items: Audit[] }>("/api/v1/audit");
  return <><header className="page-header"><div><span className="eyebrow">AUDIT TRAIL</span><h1>Activity</h1><p>Operator controls and their acknowledged outcomes.</p></div></header><div className="table-wrap"><table><thead><tr><th>Time</th><th>Action</th><th>Target</th><th>Result</th><th>Request ID</th></tr></thead><tbody>{data.items.map(item => <tr key={item.id}><td><Time value={item.timestamp_ms} /></td><td>{item.action}</td><td>{item.target ?? "fleet"}</td><td><StatusBadge value={item.result} />{item.error && <small className="negative">{item.error}</small>}</td><td className="mono">{item.request_id}</td></tr>)}</tbody></table>{!data.items.length && <p className="empty">No control actions recorded.</p>}</div></>;
}
