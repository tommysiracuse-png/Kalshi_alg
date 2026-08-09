import Link from "next/link";
import { signOut } from "@/auth";

const links = [["/", "Overview"], ["/markets", "Markets"], ["/monitoring", "Monitoring"], ["/activity", "Activity"], ["/system", "System"]];

export function Nav() {
  return <aside className="sidebar">
    <Link href="/" className="brand"><span className="brand-mark">K</span><span>Kalshi Ops<small>Market maker console</small></span></Link>
    <nav aria-label="Primary">{links.map(([href, label]) => <Link href={href} key={href}>{label}</Link>)}</nav>
    <form action={async () => { "use server"; await signOut({ redirectTo: "/login" }); }}><button className="link-button">Sign out</button></form>
  </aside>;
}
