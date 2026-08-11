export function hasSameOrigin(
  originHeader: string | null,
  hostHeader: string | null,
  requestProtocol: string,
): boolean {
  if (!originHeader || !hostHeader) return false;

  try {
    const supplied = new URL(originHeader);
    const expected = new URL(`${requestProtocol}//${hostHeader}`);
    return supplied.origin === expected.origin;
  } catch {
    return false;
  }
}
