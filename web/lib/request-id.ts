export function createRequestId(): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }

  // randomUUID is restricted to secure browser contexts. Request IDs are
  // correlation values, not credentials, so a timestamp/random fallback is
  // sufficient when the UI is reached over plain HTTP on a trusted network.
  const random = Math.random().toString(36).slice(2);
  return `request-${Date.now().toString(36)}-${random}`;
}
