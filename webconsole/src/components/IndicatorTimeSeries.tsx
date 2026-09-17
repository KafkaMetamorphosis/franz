// The recharts-backed line chart, kept in its own module so it can be loaded
// lazily. recharts is ~385 kB raw / ~115 kB gzip — more than the whole console
// was before it — and only the Indicator detail page of a numeric indicator ever
// needs it, so it must not sit in the main bundle. indicatorViz.tsx imports this
// through React.lazy; nothing else should import it directly.

import { useMemo } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  RANGES,
  byResource,
  colorFor,
  decimate,
  formatClock,
  formatValue,
  parseNumeric,
  sampleTime,
  shortFRN,
  type Family,
  type RangeKey,
  type Sample,
} from "../indicatorSamples";

export default function IndicatorTimeSeries({
  samples,
  family,
  range,
}: {
  samples: Sample[];
  family: Family;
  range: RangeKey;
}) {
  const { rows, series, decimated } = useMemo(() => {
    const groups = byResource(samples);
    const max = RANGES[range].maxPoints;
    let anyDecimated = false;

    // One row per timestamp, one column per resource — recharts' wide format.
    const byTime = new Map<number, Record<string, number | null>>();
    for (const { frn, points } of groups) {
      if (points.length > max) anyDecimated = true;
      for (const p of decimate(points, max)) {
        const t = sampleTime(p);
        if (!t) continue;
        const row = byTime.get(t) ?? {};
        row[frn] = parseNumeric(p.value, family);
        byTime.set(t, row);
      }
    }

    return {
      rows: [...byTime.entries()]
        .sort(([a], [b]) => a - b)
        .map(([t, values]) => ({ t, ...values })),
      series: groups.map((g) => g.frn),
      decimated: anyDecimated,
    };
  }, [samples, family, range]);

  if (rows.length === 0) {
    return <p className="panel-note">No numeric samples in this window.</p>;
  }

  return (
    <>
      <div className="chart-wrap" data-testid="indicator-timeseries">
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 4, left: 8 }}>
            <CartesianGrid stroke="#d5dbdb" strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="t"
              type="number"
              scale="time"
              domain={["dataMin", "dataMax"]}
              tickFormatter={(v: number) => formatClock(v)}
              stroke="#5f6b7a"
              fontSize={12}
            />
            <YAxis
              tickFormatter={(v: number) => formatValue(v, family)}
              stroke="#5f6b7a"
              fontSize={12}
              width={70}
            />
            <Tooltip
              labelFormatter={(v) => new Date(Number(v)).toLocaleString()}
              formatter={(v, name) => [
                v == null ? "—" : formatValue(Number(v), family),
                shortFRN(String(name)),
              ]}
            />
            {series.length > 1 ? <Legend formatter={(v) => shortFRN(String(v))} /> : null}
            {series.map((frn, i) => (
              <Line
                key={frn}
                type="monotone"
                dataKey={frn}
                name={frn}
                stroke={colorFor(i)}
                strokeWidth={2}
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
      {decimated ? (
        <p className="panel-note">
          Thinned to at most {RANGES[range].maxPoints} points per series for display; the newest
          sample is always kept. The table below is unthinned.
        </p>
      ) : null}
    </>
  );
}
