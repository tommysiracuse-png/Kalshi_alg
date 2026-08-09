export function MiniChart({ values }: { values: number[] }) {
  if (values.length < 2) return <div className="chart-empty">Not enough data to chart</div>;
  const min = Math.min(...values), max = Math.max(...values), span = max - min || 1;
  const points = values.map((value, index) => `${(index / (values.length - 1)) * 100},${100 - ((value - min) / span) * 90}`).join(" ");
  return <svg className="chart" viewBox="0 0 100 100" preserveAspectRatio="none" role="img" aria-label="Recent telemetry trend"><polyline points={points} fill="none" vectorEffect="non-scaling-stroke" /></svg>;
}
