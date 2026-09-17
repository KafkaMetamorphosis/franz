// Pure transforms over an Indicator's sample history — grouping, decimation,
// value parsing and state-run detection. No JSX, so the rendering components in
// components/indicatorViz.tsx stay fast-refreshable and these stay directly
// unit-testable.
//
// Which transform applies is decided by the indicator's `family`
// (`IndicatorFamily`, 003.8), not by parsing its free-form `unit` string: the
// server owns that classification and exposes it precisely so the console does
// not re-implement `indicator.Unit.Family`'s alias table and drift from it.

import type { components } from "./api/schema";

type Schemas = components["schemas"];
export type Sample = Schemas["v1IndicatorSampleView"];
export type Family = Schemas["v1IndicatorFamily"];

// --- ranges ---------------------------------------------------------------

export type RangeKey = "1h" | "6h" | "24h";

// maxPoints caps what one series draws. Gregor Samsa's default sweep is 60s, so
// 24h is ~1440 points per resource — enough to make an SVG line chart sluggish
// and, at a few pixels per point, indistinguishable anyway. Client-side
// decimation keeps this simple; server-side bucketing would be the right answer
// if ranges ever grow past a day, and is deliberately not built yet.
export const RANGES: Record<RangeKey, { label: string; hours: number; maxPoints: number }> = {
  "1h": { label: "1h", hours: 1, maxPoints: 240 },
  "6h": { label: "6h", hours: 6, maxPoints: 360 },
  "24h": { label: "24h", hours: 24, maxPoints: 480 },
};

// pageSizeFor asks for enough rows to cover the window across every resource the
// indicator spans, with headroom, bounded by the API's 1000-row maximum.
export function pageSizeFor(range: RangeKey): number {
  return Math.min(1000, Math.max(200, RANGES[range].hours * 60 * 4));
}

export function fromISOFor(range: RangeKey, now: number = Date.now()): string {
  return new Date(now - RANGES[range].hours * 3600_000).toISOString();
}

// --- series identity ------------------------------------------------------

// The series palette, ordered so adjacent series stay distinguishable, drawn
// from the console's own tokens rather than recharts' defaults so a chart looks
// like the rest of the page.
const SERIES_COLORS = [
  "#147eb3", // --blue
  "#1d8102", // --green
  "#b55c00", // --warning
  "#7c3aed",
  "#0f766e",
  "#be185d",
];

/** Beyond this many distinct labels a timeline stops being readable. */
export const MAX_TIMELINE_VALUES = 6;

export function colorFor(index: number): string {
  return SERIES_COLORS[index % SERIES_COLORS.length];
}

// shortFRN trims the FRN prefix for a legend or a narrow label column. The full
// value stays in the samples table and in every title attribute.
export function shortFRN(frn: string): string {
  const parts = frn.split(":");
  return parts.length > 1 ? parts.slice(2).join(":") || frn : frn;
}

export function sampleTime(s: Sample): number {
  return s.sampleAt ? new Date(s.sampleAt).getTime() : 0;
}

/** byResource groups samples per resource FRN, each series oldest-first. */
export function byResource(samples: Sample[]): { frn: string; points: Sample[] }[] {
  const groups = new Map<string, Sample[]>();
  for (const s of samples) {
    const frn = s.resourceFrn ?? "";
    const list = groups.get(frn);
    if (list) list.push(s);
    else groups.set(frn, [s]);
  }
  return [...groups.entries()]
    .map(([frn, points]) => ({
      frn,
      points: [...points].sort((a, b) => sampleTime(a) - sampleTime(b)),
    }))
    .sort((a, b) => a.frn.localeCompare(b.frn));
}

// decimate keeps at most max points, evenly spaced, and always keeps the last
// one — the newest sample is the one an operator is looking for, so it must
// never be the point that gets dropped.
export function decimate<T>(points: T[], max: number): T[] {
  if (points.length <= max) return points;
  const step = points.length / max;
  const out: T[] = [];
  for (let i = 0; i < max - 1; i++) out.push(points[Math.floor(i * step)]);
  out.push(points[points.length - 1]);
  return out;
}

// --- numeric values -------------------------------------------------------

// Bytes and durations arrive as suffixed strings ("150Gi", "90d"). Franz
// compares them server-side, but a chart needs a magnitude, so the same suffix
// tables are applied here. Longest suffix first, so "Ki" is matched before "K".
const BYTE_SUFFIXES: [string, number][] = [
  ["Ei", 2 ** 60], ["Pi", 2 ** 50], ["Ti", 2 ** 40], ["Gi", 2 ** 30],
  ["Mi", 2 ** 20], ["Ki", 2 ** 10],
  ["E", 1e18], ["P", 1e15], ["T", 1e12], ["G", 1e9], ["M", 1e6], ["K", 1e3], ["k", 1e3],
];

const DURATION_SUFFIXES: [string, number][] = [
  ["ms", 1], ["s", 1_000], ["m", 60_000], ["h", 3_600_000], ["d", 86_400_000],
  ["w", 604_800_000],
];

/**
 * parseNumeric turns a stored value into a plottable number, or null when it
 * cannot be read — a chart then leaves a gap rather than drawing a zero that
 * never happened.
 */
export function parseNumeric(raw: string | undefined, family: Family): number | null {
  const s = (raw ?? "").trim();
  if (!s) return null;

  if (family === "INDICATOR_FAMILY_BYTES") {
    // A trailing "B" is decoration: "10MB" == "10M", "512B" == "512".
    const body = s.endsWith("B") && s.length > 1 ? s.slice(0, -1) : s;
    for (const [suffix, mult] of BYTE_SUFFIXES) {
      if (body.endsWith(suffix)) {
        const n = Number(body.slice(0, -suffix.length));
        return Number.isFinite(n) ? n * mult : null;
      }
    }
    const n = Number(body);
    return Number.isFinite(n) ? n : null;
  }

  if (family === "INDICATOR_FAMILY_DURATION") {
    for (const [suffix, mult] of DURATION_SUFFIXES) {
      if (s.endsWith(suffix)) {
        const n = Number(s.slice(0, -suffix.length));
        if (Number.isFinite(n)) return n * mult;
      }
    }
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

/** formatValue renders a magnitude back into the indicator's own vocabulary. */
export function formatValue(v: number, family: Family): string {
  if (family === "INDICATOR_FAMILY_BYTES") {
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let n = v;
    let i = 0;
    while (n >= 1024 && i < units.length - 1) {
      n /= 1024;
      i++;
    }
    return `${Number.isInteger(n) ? n : n.toFixed(1)} ${units[i]}`;
  }
  if (family === "INDICATOR_FAMILY_DURATION") {
    if (v >= 86_400_000) return `${(v / 86_400_000).toFixed(1)}d`;
    if (v >= 3_600_000) return `${(v / 3_600_000).toFixed(1)}h`;
    if (v >= 60_000) return `${(v / 60_000).toFixed(1)}m`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(1)}s`;
    return `${v}ms`;
  }
  return Number.isInteger(v) ? String(v) : v.toFixed(2);
}

export function formatClock(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

// --- categorical values ---------------------------------------------------

export type Span = { value: string; from: number; to: number };

/**
 * spansFor collapses consecutive equal samples into one span per state run.
 *
 * The final run extends to windowEnd: a state holds until something says
 * otherwise, and drawing it zero-width would read as "this stopped being true",
 * which is the opposite of what it means.
 */
export function spansFor(points: Sample[], windowEnd: number): Span[] {
  const spans: Span[] = [];
  for (const p of points) {
    const t = sampleTime(p);
    if (!t) continue;
    const value = p.value ?? "";
    const last = spans[spans.length - 1];
    if (last && last.value === value) {
      last.to = t;
      continue;
    }
    if (last) last.to = t;
    spans.push({ value, from: t, to: t });
  }
  if (spans.length > 0) spans[spans.length - 1].to = windowEnd;
  return spans;
}

export function distinctValues(samples: Sample[]): string[] {
  return [...new Set(samples.map((s) => s.value ?? ""))].sort();
}

// stateColor keeps booleans semantic (true = green, false = muted grey) and
// assigns enum labels palette colours in a stable, value-sorted order so a given
// state keeps its colour across renders.
export function stateColor(value: string, family: Family, labels: string[]): string {
  if (family === "INDICATOR_FAMILY_BOOLEAN") {
    return value === "true" ? "#1d8102" : "#9aa5b1";
  }
  return colorFor(labels.indexOf(value));
}

/**
 * useTimeline decides whether a categorical indicator can be drawn as a
 * coloured timeline at all. An opaque id (kafka.cluster.controller_id) has an
 * effectively unbounded label set, where twenty indistinguishable bands teach
 * nothing and a transition list is the honest view.
 */
export function canRenderTimeline(samples: Sample[]): boolean {
  return distinctValues(samples).length <= MAX_TIMELINE_VALUES;
}
