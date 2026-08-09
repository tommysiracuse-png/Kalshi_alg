"use client";

import { useActionState } from "react";
import { login } from "./actions";

export function LoginForm() {
  const [error, action, pending] = useActionState(login, undefined);
  return <form action={action} className="login-card">
    <div><span className="eyebrow">KALSHI ALG</span><h1>Operator sign in</h1><p>Access live fleet health and guarded controls.</p></div>
    <label>Username<input name="username" autoComplete="username" required autoFocus /></label>
    <label>Password<input name="password" type="password" autoComplete="current-password" required /></label>
    {error && <p className="error" role="alert">{error}</p>}
    <button className="button primary" disabled={pending}>{pending ? "Signing in…" : "Sign in"}</button>
  </form>;
}
