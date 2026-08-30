"use client";

import { useMemo, useState } from "react";
import type { PortfolioHistoryMetric, PortfolioSummaryAnalytics } from "@/lib/types";

export type PortfolioWindow = "24h" | "7d" | "30d";
type SeriesKey = "availableCash" | "totalPortfolioValue" | "positionsLiquidationValue";

const SERIES: Array<{ key: SeriesKey; label: string; color: string }> = [
  { key: "availableCash", label: "Available Cash", color: "#5fd3bc" },
  { key: "totalPortfolioValue", label: "Total Portfolio Value", color: "#63a9ff" },
  { key: "positionsLiquidationValue", label: "Liquidation Value", color: "#f5bd54" },
];
const WINDOWS: Array<{ value: PortfolioWindow; label: string }> = [
  { value: "24h", label: "1D" },
  { value: "7d", label: "1W" },
  { value: "30d", label: "1M" },
];
const WIDTH = 960;
const HEIGHT = 340;
const MARGIN = { top: 20, right: 24, bottom: 42, left: 76 };

function dollars(value: number) {
  return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value / 10_000);
}

function axisDollars(value: number) {
  return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value / 10_000);
}

function timeLabel(timestamp: number, window: PortfolioWindow) {
  return new Intl.DateTimeFormat(undefined, window === "24h"
    ? { hour: "numeric", minute: "2-digit" }
    : { month: "short", day: "numeric" }).format(timestamp);
}

function tooltipTime(timestamp: number) {
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(timestamp);
}

function metric(data: PortfolioSummaryAnalytics, key: SeriesKey): PortfolioHistoryMetric | undefined {
  return data.history[key];
}

export function PortfolioHistoryChart({ data, window, loading, onWindowChange }: {
  data: PortfolioSummaryAnalytics;
  window: PortfolioWindow;
  loading: boolean;
  onWindowChange: (window: PortfolioWindow) => void;
}) {
  const [enabled, setEnabled] = useState<Record<SeriesKey, boolean>>({
    availableCash: false,
    totalPortfolioValue: true,
    positionsLiquidationValue: false,
  });
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [pointerX, setPointerX] = useState<number | null>(null);

  const chart = useMemo(() => {
    const activeSeries = SERIES.filter(series => enabled[series.key]);
    const timestamps = [...new Set(activeSeries.flatMap(series => (metric(data, series.key)?.points ?? []).map(point => point.timestampMs)))].sort((a, b) => a - b);
    const values = activeSeries.flatMap(series => (metric(data, series.key)?.points ?? []).map(point => point.valueUnits));
    if (!timestamps.length || !values.length) return { activeSeries, timestamps, xPositions: [], lines: [], yTicks: [], xTicks: [], minimum: 0, maximum: 1 };
    const rawMinimum = Math.min(...values);
    const rawMaximum = Math.max(...values);
    const padding = Math.max((rawMaximum - rawMinimum) * 0.08, Math.abs(rawMaximum || 1) * 0.01, 1);
    const minimum = rawMinimum - padding;
    const maximum = rawMaximum + padding;
    const first = timestamps[0];
    const last = timestamps[timestamps.length - 1];
    const timeSpan = Math.max(1, last - first);
    const valueSpan = Math.max(1, maximum - minimum);
    const x = (timestamp: number) => MARGIN.left + ((timestamp - first) / timeSpan) * (WIDTH - MARGIN.left - MARGIN.right);
    const y = (value: number) => MARGIN.top + (1 - (value - minimum) / valueSpan) * (HEIGHT - MARGIN.top - MARGIN.bottom);
    const lines = activeSeries.map(series => ({
      ...series,
      points: metric(data, series.key)?.points ?? [],
      path: (metric(data, series.key)?.points ?? []).map((point, index) => `${index ? "L" : "M"}${x(point.timestampMs).toFixed(2)},${y(point.valueUnits).toFixed(2)}`).join(" "),
    }));
    const yTicks = Array.from({ length: 5 }, (_, index) => {
      const value = minimum + ((4 - index) / 4) * valueSpan;
      return { value, y: y(value) };
    });
    const xTickCount = Math.min(5, timestamps.length);
    const xTicks = Array.from({ length: xTickCount }, (_, index) => {
      const timestamp = first + (index / Math.max(1, xTickCount - 1)) * timeSpan;
      return { timestamp, x: x(timestamp) };
    });
    return { activeSeries, timestamps, xPositions: timestamps.map(x), lines, yTicks, xTicks, minimum, maximum };
  }, [data, enabled]);

  const selectedTimestamp = activeIndex == null ? null : chart.timestamps[Math.min(activeIndex, chart.timestamps.length - 1)] ?? null;
  const cursorX = activeIndex == null ? null : pointerX ?? chart.xPositions[Math.min(activeIndex, chart.xPositions.length - 1)] ?? null;
  const toggleSeries = (key: SeriesKey) => {
    const enabledCount = Object.values(enabled).filter(Boolean).length;
    if (enabled[key] && enabledCount === 1) return;
    setEnabled(previous => ({ ...previous, [key]: !previous[key] }));
    setActiveIndex(null);
    setPointerX(null);
  };
  const selectFromPointer = (clientX: number, plotLeft: number, plotWidth: number) => {
    if (!chart.timestamps.length) return;
    const fraction = Math.max(0, Math.min(1, (clientX - plotLeft) / Math.max(1, plotWidth)));
    setPointerX(MARGIN.left + fraction * (WIDTH - MARGIN.left - MARGIN.right));
    const target = chart.timestamps[0] + fraction * (chart.timestamps.at(-1)! - chart.timestamps[0]);
    const nearest = chart.timestamps.reduce((best, timestamp, index) =>
      Math.abs(timestamp - target) < Math.abs(chart.timestamps[best] - target) ? index : best, 0);
    setActiveIndex(nearest);
  };

  return <section className="panel portfolio-history-panel" aria-labelledby="portfolio-history-title">
    <div className="portfolio-history-heading">
      <div><span className="eyebrow">PORTFOLIO HISTORY</span><h2 id="portfolio-history-title">Portfolio Value</h2></div>
      <div className="portfolio-history-windows" aria-label="History range">
        {WINDOWS.map(item => <button key={item.value} type="button" aria-pressed={window === item.value} onClick={() => onWindowChange(item.value)} disabled={loading}>{item.label}</button>)}
      </div>
    </div>
    <div className="portfolio-history-series" aria-label="Chart series">
      {SERIES.map(series => <label key={series.key} style={{ "--series-color": series.color } as React.CSSProperties}>
        <input type="checkbox" checked={enabled[series.key]} onChange={() => toggleSeries(series.key)} aria-label={`Show ${series.label}`} />
        <span />{series.label}
      </label>)}
    </div>
    {loading && <p className="portfolio-chart-status">Refreshing history…</p>}
    {chart.timestamps.length < 2 ? <div className="portfolio-chart-empty">Collecting enough portfolio history to draw this range.</div> : <div className="portfolio-chart-wrap">
      <svg
        className="portfolio-history-chart"
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label="Portfolio value history chart. Use left and right arrow keys to inspect points."
        tabIndex={0}
        onFocus={() => { setPointerX(null); setActiveIndex(previous => previous ?? chart.timestamps.length - 1); }}
        onBlur={() => { setActiveIndex(null); setPointerX(null); }}
        onKeyDown={event => {
          if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
          event.preventDefault();
          setPointerX(null);
          const direction = event.key === "ArrowRight" ? 1 : -1;
          setActiveIndex(previous => Math.max(0, Math.min(chart.timestamps.length - 1, (previous ?? chart.timestamps.length - 1) + direction)));
        }}
      >
        {chart.yTicks.map(tick => <g key={tick.y}><line className="portfolio-chart-grid" x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} /><text className="portfolio-chart-axis" x={MARGIN.left - 10} y={tick.y + 4} textAnchor="end">{axisDollars(tick.value)}</text></g>)}
        {chart.xTicks.map(tick => <text key={tick.x} className="portfolio-chart-axis" x={tick.x} y={HEIGHT - 12} textAnchor="middle">{timeLabel(tick.timestamp, window)}</text>)}
        {chart.lines.map(line => <path key={line.key} d={line.path} fill="none" stroke={line.color} strokeWidth="2.5" vectorEffect="non-scaling-stroke" />)}
        {selectedTimestamp != null && cursorX != null && <line className="portfolio-chart-cursor" x1={cursorX} x2={cursorX} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} />}
        <rect
          className="portfolio-chart-hit-area"
          x={MARGIN.left}
          y={MARGIN.top}
          width={WIDTH - MARGIN.left - MARGIN.right}
          height={HEIGHT - MARGIN.top - MARGIN.bottom}
          fill="transparent"
          pointerEvents="all"
          aria-hidden="true"
          onPointerMove={event => {
            const box = event.currentTarget.getBoundingClientRect();
            selectFromPointer(event.clientX, box.left, box.width);
          }}
          onPointerLeave={() => { setActiveIndex(null); setPointerX(null); }}
        />
      </svg>
      {selectedTimestamp != null && <div className="portfolio-chart-tooltip" role="status">
        <strong>{tooltipTime(selectedTimestamp)}</strong>
        {chart.activeSeries.map(series => {
          const point = metric(data, series.key)?.points.find(item => item.timestampMs === selectedTimestamp);
          return <span key={series.key}><i style={{ background: series.color }} />{series.label}<b>{point ? dollars(point.valueUnits) : "Unavailable"}</b></span>;
        })}
      </div>}
    </div>}
    <p className={data.coverage.partial ? "portfolio-chart-coverage partial" : "portfolio-chart-coverage"}>{data.coverage.partial ? `Partial history · ${Math.max(0, Math.round((data.coverage.actualWindowMs ?? 0) / 3_600_000))} hours recorded` : "Complete selected range"}</p>
  </section>;
}
