// Visualisations for one Indicator's sample history, picked by the indicator's
// `family` (`IndicatorFamily`, 003.8) rather than by parsing its free-form unit
// string — the server owns that classification, the console just switches on it.
//
//   NUMERIC / BYTES / DURATION  → IndicatorTimeSeries (a line per resource)
//   BOOLEAN / STRING            → StateTimeline (a band per resource)
//
// A STRING indicator with more distinct labels than the palette can carry (an
// opaque id like kafka.cluster.controller_id) falls back to StateTransitions:
// twenty indistinguishable colour bands teach nothing.
//
// Every pure transform lives in ../indicatorSamples so this file exports only
// components.

import { Suspense, lazy, useMemo } from "react";
import {
  RANGES,
  byResource,
  canRenderTimeline,
  distinctValues,
  shortFRN,
  spansFor,
  stateColor,
  type Family,
  type RangeKey,
  type Sample,
} from "../indicatorSamples";

// recharts is larger than the rest of the console put together and only a
// numeric indicator's detail page needs it, so it is fetched on demand rather
// than shipped in the main bundle.
const IndicatorTimeSeries = lazy(() => import("./IndicatorTimeSeries"));

export function RangeSelector({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (r: RangeKey) => void;
}) {
  return (
    <div className="range-selector" role="group" aria-label="Time range">
      {(Object.keys(RANGES) as RangeKey[]).map((key) => (
        <button
          key={key}
          type="button"
          className={`button range-option${key === value ? " selected" : ""}`}
          aria-pressed={key === value}
          onClick={() => onChange(key)}
        >
          {RANGES[key].label}
        </button>
      ))}
    </div>
  );
}

// --- boolean / enum: state timeline --------------------------------------

export function StateTimeline({
  samples,
  family,
  range,
  now = Date.now(),
}: {
  samples: Sample[];
  family: Family;
  range: RangeKey;
  now?: number;
}) {
  const labels = useMemo(() => distinctValues(samples), [samples]);
  const groups = useMemo(() => byResource(samples), [samples]);

  const windowStart = now - RANGES[range].hours * 3600_000;
  const span = Math.max(1, now - windowStart);

  if (groups.length === 0) {
    return <p className="panel-note">No samples in this window.</p>;
  }

  return (
    <div data-testid="indicator-state-timeline">
      {groups.map(({ frn, points }) => (
        <div className="state-row" key={frn}>
          <div className="state-row-label" title={frn}>
            <code>{shortFRN(frn)}</code>
          </div>
          <div className="state-track">
            {spansFor(points, now).map((s, i) => {
              const left = ((Math.max(s.from, windowStart) - windowStart) / span) * 100;
              const width = ((s.to - Math.max(s.from, windowStart)) / span) * 100;
              if (width <= 0) return null;
              return (
                <div
                  key={`${s.from}-${i}`}
                  className="state-span"
                  style={{
                    left: `${left}%`,
                    // Floor the width so a single-sample blip stays visible.
                    width: `${Math.max(width, 0.4)}%`,
                    background: stateColor(s.value, family, labels),
                  }}
                  title={`${s.value} — ${new Date(s.from).toLocaleString()} to ${new Date(
                    s.to,
                  ).toLocaleString()}`}
                />
              );
            })}
          </div>
        </div>
      ))}

      <div className="state-legend">
        {labels.map((value) => (
          <span className="state-legend-item" key={value}>
            <span className="state-swatch" style={{ background: stateColor(value, family, labels) }} />
            <code>{value}</code>
          </span>
        ))}
      </div>
    </div>
  );
}

// --- high-cardinality fallback -------------------------------------------

// StateTransitions is the fallback for a categorical indicator whose values are
// effectively unbounded — an opaque id, not a state machine. It answers "what is
// it now, and when did it last change?", the only useful question when the label
// set has no fixed meaning.
export function StateTransitions({
  samples,
  now = Date.now(),
}: {
  samples: Sample[];
  now?: number;
}) {
  const groups = useMemo(() => byResource(samples), [samples]);

  if (groups.length === 0) {
    return <p className="panel-note">No samples in this window.</p>;
  }

  return (
    <div data-testid="indicator-state-transitions">
      <p className="panel-note">
        Too many distinct values for a colour-coded timeline — showing current value and recent
        changes instead.
      </p>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Resource</th>
              <th>Current</th>
              <th>Since</th>
              <th>Changes in window</th>
            </tr>
          </thead>
          <tbody>
            {groups.map(({ frn, points }) => {
              const spans = spansFor(points, now);
              const last = spans[spans.length - 1];
              return (
                <tr key={frn}>
                  <td title={frn}>
                    <code>{shortFRN(frn)}</code>
                  </td>
                  <td>{last ? <code>{last.value}</code> : "—"}</td>
                  <td>{last ? new Date(last.from).toLocaleString() : "—"}</td>
                  <td>{Math.max(0, spans.length - 1)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- entry point ----------------------------------------------------------

/** IndicatorVisualisation picks the right view for the indicator's family. */
export function IndicatorVisualisation({
  samples,
  family,
  range,
  now,
}: {
  samples: Sample[];
  family: Family | undefined;
  range: RangeKey;
  now?: number;
}) {
  // An indicator predating the family field reads as UNSPECIFIED; numeric is the
  // same fallback Franz uses for an unrecognised unit.
  const fam: Family = family && family !== "INDICATOR_FAMILY_UNSPECIFIED"
    ? family
    : "INDICATOR_FAMILY_NUMERIC";

  if (fam === "INDICATOR_FAMILY_BOOLEAN") {
    return <StateTimeline samples={samples} family={fam} range={range} now={now} />;
  }
  if (fam === "INDICATOR_FAMILY_STRING") {
    return canRenderTimeline(samples) ? (
      <StateTimeline samples={samples} family={fam} range={range} now={now} />
    ) : (
      <StateTransitions samples={samples} now={now} />
    );
  }
  return (
    <Suspense fallback={<p className="panel-note">Loading chart…</p>}>
      <IndicatorTimeSeries samples={samples} family={fam} range={range} />
    </Suspense>
  );
}
