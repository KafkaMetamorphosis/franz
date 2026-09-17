import { describe, expect, it } from "vitest";
import {
  RANGES,
  byResource,
  canRenderTimeline,
  decimate,
  distinctValues,
  formatValue,
  pageSizeFor,
  parseNumeric,
  spansFor,
  stateColor,
  type Sample,
} from "./indicatorSamples";

function sample(value: string, sampleAt: string, resourceFrn = "frn:default:kafka-topic:a"): Sample {
  return { value, sampleAt, resourceFrn, resourceEntity: "ENTITY_KAFKA_TOPIC" };
}

describe("decimate", () => {
  it("leaves a series shorter than the cap untouched", () => {
    expect(decimate([1, 2, 3], 10)).toEqual([1, 2, 3]);
  });

  it("caps the series length", () => {
    expect(decimate(Array.from({ length: 1000 }, (_, i) => i), 50)).toHaveLength(50);
  });

  // The newest sample is the one an operator is looking for, so it is the one
  // point decimation must never drop.
  it("always keeps the newest point", () => {
    const points = Array.from({ length: 1440 }, (_, i) => i);
    const out = decimate(points, 480);
    expect(out[out.length - 1]).toBe(1439);
  });
});

describe("parseNumeric", () => {
  it("reads plain numbers for a gauge", () => {
    expect(parseNumeric("18", "INDICATOR_FAMILY_NUMERIC")).toBe(18);
    expect(parseNumeric("1.5", "INDICATOR_FAMILY_NUMERIC")).toBe(1.5);
  });

  it("reads binary and SI byte suffixes, and a decorative trailing B", () => {
    expect(parseNumeric("1Ki", "INDICATOR_FAMILY_BYTES")).toBe(1024);
    expect(parseNumeric("150Gi", "INDICATOR_FAMILY_BYTES")).toBe(150 * 2 ** 30);
    expect(parseNumeric("10M", "INDICATOR_FAMILY_BYTES")).toBe(10e6);
    expect(parseNumeric("10MB", "INDICATOR_FAMILY_BYTES")).toBe(10e6);
    expect(parseNumeric("512", "INDICATOR_FAMILY_BYTES")).toBe(512);
  });

  it("reads duration suffixes as milliseconds", () => {
    expect(parseNumeric("5m", "INDICATOR_FAMILY_DURATION")).toBe(300_000);
    expect(parseNumeric("90d", "INDICATOR_FAMILY_DURATION")).toBe(90 * 86_400_000);
    expect(parseNumeric("250ms", "INDICATOR_FAMILY_DURATION")).toBe(250);
  });

  // A gap is honest; a zero is a value that never happened.
  it("returns null for an unreadable or empty value", () => {
    expect(parseNumeric("provisioned", "INDICATOR_FAMILY_NUMERIC")).toBeNull();
    expect(parseNumeric("", "INDICATOR_FAMILY_NUMERIC")).toBeNull();
    expect(parseNumeric(undefined, "INDICATOR_FAMILY_NUMERIC")).toBeNull();
  });
});

describe("formatValue", () => {
  it("renders bytes and durations in their own vocabulary", () => {
    expect(formatValue(1024, "INDICATOR_FAMILY_BYTES")).toBe("1 KiB");
    expect(formatValue(300_000, "INDICATOR_FAMILY_DURATION")).toBe("5.0m");
    expect(formatValue(18, "INDICATOR_FAMILY_NUMERIC")).toBe("18");
  });
});

describe("byResource", () => {
  // An indicator spans resources — replicas_per_broker is one sample per
  // broker — so a chart needs one series each, oldest-first.
  it("groups by resource FRN and sorts each series oldest-first", () => {
    const groups = byResource([
      sample("2", "2026-09-17T10:02:00Z", "frn:default:kafka-topic:b"),
      sample("1", "2026-09-17T10:01:00Z", "frn:default:kafka-topic:a"),
      sample("3", "2026-09-17T10:03:00Z", "frn:default:kafka-topic:a"),
    ]);
    expect(groups.map((g) => g.frn)).toEqual([
      "frn:default:kafka-topic:a",
      "frn:default:kafka-topic:b",
    ]);
    expect(groups[0].points.map((p) => p.value)).toEqual(["1", "3"]);
  });
});

describe("spansFor", () => {
  const windowEnd = new Date("2026-09-17T11:00:00Z").getTime();

  it("collapses consecutive equal samples into one run", () => {
    const spans = spansFor(
      [
        sample("false", "2026-09-17T10:00:00Z"),
        sample("false", "2026-09-17T10:01:00Z"),
        sample("true", "2026-09-17T10:02:00Z"),
      ],
      windowEnd,
    );
    expect(spans.map((s) => s.value)).toEqual(["false", "true"]);
  });

  // A state holds until something says otherwise; a zero-width final span would
  // read as "this stopped being true", the opposite of what it means.
  it("extends the final run to the end of the window", () => {
    const spans = spansFor([sample("provisioned", "2026-09-17T10:00:00Z")], windowEnd);
    expect(spans).toHaveLength(1);
    expect(spans[0].to).toBe(windowEnd);
  });

  it("ignores samples with no timestamp", () => {
    expect(spansFor([{ value: "true", resourceFrn: "x" }], windowEnd)).toHaveLength(0);
  });
});

describe("canRenderTimeline", () => {
  it("accepts a small label set like a topic state machine", () => {
    const states = ["provisioned", "diverged", "missing"].map((v, i) =>
      sample(v, `2026-09-17T10:0${i}:00Z`),
    );
    expect(canRenderTimeline(states)).toBe(true);
  });

  // kafka.cluster.controller_id is an opaque id, not a state machine.
  it("rejects a high-cardinality label set", () => {
    const ids = Array.from({ length: 9 }, (_, i) => sample(String(i), `2026-09-17T10:0${i}:00Z`));
    expect(canRenderTimeline(ids)).toBe(false);
  });
});

describe("stateColor", () => {
  it("keeps booleans semantic — true reads as healthy, false as muted", () => {
    const labels = ["false", "true"];
    expect(stateColor("true", "INDICATOR_FAMILY_BOOLEAN", labels)).toBe("#1d8102");
    expect(stateColor("false", "INDICATOR_FAMILY_BOOLEAN", labels)).toBe("#9aa5b1");
  });

  it("gives an enum label a stable colour across renders", () => {
    const labels = distinctValues(
      ["missing", "provisioned", "diverged"].map((v, i) => sample(v, `2026-09-17T10:0${i}:00Z`)),
    );
    const first = stateColor("provisioned", "INDICATOR_FAMILY_STRING", labels);
    expect(stateColor("provisioned", "INDICATOR_FAMILY_STRING", labels)).toBe(first);
    expect(stateColor("missing", "INDICATOR_FAMILY_STRING", labels)).not.toBe(first);
  });
});

describe("range plumbing", () => {
  // The default page is 50 rows across every resource, which is minutes of
  // history for a multi-resource indicator — the window has to raise it.
  it("asks for enough rows to cover the window, within the API cap", () => {
    expect(pageSizeFor("1h")).toBeGreaterThanOrEqual(200);
    expect(pageSizeFor("24h")).toBeLessThanOrEqual(1000);
  });

  it("caps points per series for every range", () => {
    for (const key of Object.keys(RANGES) as (keyof typeof RANGES)[]) {
      expect(RANGES[key].maxPoints).toBeGreaterThan(0);
    }
  });
});
