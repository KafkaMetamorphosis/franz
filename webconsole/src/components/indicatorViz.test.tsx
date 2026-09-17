import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { IndicatorVisualisation, RangeSelector, StateTimeline } from "./indicatorViz";
import type { Family, Sample } from "../indicatorSamples";

// Two things shape these tests. recharts' ResponsiveContainer measures its
// parent, which is 0×0 in jsdom, so the SVG internals are left to recharts and
// the numeric branch is identified by its wrapper testid instead. And the chart
// is behind React.lazy to keep recharts out of the main bundle, so that branch
// resolves asynchronously — hence findByTestId rather than getByTestId.
const NOW = new Date("2026-09-17T11:00:00Z").getTime();

function sample(value: string, minutesAgo: number, resourceFrn = "frn:default:kafka-topic:shipments-0"): Sample {
  return {
    value,
    sampleAt: new Date(NOW - minutesAgo * 60_000).toISOString(),
    resourceFrn,
    resourceEntity: "ENTITY_KAFKA_TOPIC",
  };
}

function renderViz(samples: Sample[], family: Family | undefined) {
  return render(
    <IndicatorVisualisation samples={samples} family={family} range="1h" now={NOW} />,
  );
}

describe("IndicatorVisualisation dispatch", () => {
  it("charts a gauge indicator as a time series", async () => {
    renderViz([sample("3", 5), sample("3", 4), sample("4", 3)], "INDICATOR_FAMILY_NUMERIC");
    expect(await screen.findByTestId("indicator-timeseries")).toBeInTheDocument();
    expect(screen.queryByTestId("indicator-state-timeline")).not.toBeInTheDocument();
  });

  it("charts bytes and durations as a time series too", async () => {
    for (const family of ["INDICATOR_FAMILY_BYTES", "INDICATOR_FAMILY_DURATION"] as Family[]) {
      const { unmount } = renderViz([sample("1Ki", 5), sample("2Ki", 4)], family);
      expect(await screen.findByTestId("indicator-timeseries")).toBeInTheDocument();
      unmount();
    }
  });

  it("shows a boolean indicator as a state timeline, not a chart", () => {
    renderViz(
      [sample("false", 30), sample("false", 20), sample("true", 10)],
      "INDICATOR_FAMILY_BOOLEAN",
    );
    expect(screen.getByTestId("indicator-state-timeline")).toBeInTheDocument();
    expect(screen.queryByTestId("indicator-timeseries")).not.toBeInTheDocument();
    // Both observed states appear in the legend.
    expect(screen.getByText("true")).toBeInTheDocument();
    expect(screen.getByText("false")).toBeInTheDocument();
  });

  it("shows a small enum as a state timeline", () => {
    renderViz(
      [sample("provisioned", 30), sample("diverged", 20), sample("provisioned", 10)],
      "INDICATOR_FAMILY_STRING",
    );
    expect(screen.getByTestId("indicator-state-timeline")).toBeInTheDocument();
    expect(screen.queryByTestId("indicator-state-transitions")).not.toBeInTheDocument();
  });

  // kafka.cluster.controller_id is an opaque id — a colour band per value would
  // be meaningless, so the high-cardinality fallback takes over.
  it("falls back to a transition list for a high-cardinality label set", () => {
    const ids = Array.from({ length: 9 }, (_, i) => sample(`broker-${i}`, 50 - i * 5));
    renderViz(ids, "INDICATOR_FAMILY_STRING");

    expect(screen.getByTestId("indicator-state-transitions")).toBeInTheDocument();
    expect(screen.queryByTestId("indicator-state-timeline")).not.toBeInTheDocument();
    expect(screen.getByText(/Too many distinct values/)).toBeInTheDocument();
    // The current value and its change count are what the fallback is for.
    expect(screen.getByText("broker-8")).toBeInTheDocument();
  });

  // An indicator stored before the family field existed reads as UNSPECIFIED;
  // numeric is the same fallback Franz uses for an unrecognised unit.
  it("treats an unspecified or missing family as numeric", async () => {
    const { unmount } = renderViz([sample("1", 5)], "INDICATOR_FAMILY_UNSPECIFIED");
    expect(await screen.findByTestId("indicator-timeseries")).toBeInTheDocument();
    unmount();

    renderViz([sample("1", 5)], undefined);
    expect(await screen.findByTestId("indicator-timeseries")).toBeInTheDocument();
  });
});

describe("StateTimeline", () => {
  it("draws one track per resource the indicator covers", () => {
    render(
      <StateTimeline
        samples={[
          sample("true", 20, "frn:default:kafka-topic:shipments-0"),
          sample("false", 20, "frn:default:kafka-topic:shipments-1"),
        ]}
        family="INDICATOR_FAMILY_BOOLEAN"
        range="1h"
        now={NOW}
      />,
    );
    expect(screen.getByText("kafka-topic:shipments-0")).toBeInTheDocument();
    expect(screen.getByText("kafka-topic:shipments-1")).toBeInTheDocument();
  });

  it("says so when the window holds no samples", () => {
    render(
      <StateTimeline samples={[]} family="INDICATOR_FAMILY_BOOLEAN" range="1h" now={NOW} />,
    );
    expect(screen.getByText(/No samples in this window/)).toBeInTheDocument();
  });
});

describe("RangeSelector", () => {
  it("marks the active range and reports a change", async () => {
    const onChange = vi.fn();
    render(<RangeSelector value="1h" onChange={onChange} />);

    expect(screen.getByRole("button", { name: "1h" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "24h" })).toHaveAttribute("aria-pressed", "false");

    await userEvent.click(screen.getByRole("button", { name: "24h" }));
    expect(onChange).toHaveBeenCalledWith("24h");
  });
});
