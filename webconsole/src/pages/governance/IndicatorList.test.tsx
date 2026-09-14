import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen } from "@testing-library/react";
import { renderApp } from "../../test/render";
import { IndicatorList } from "./IndicatorList";

const fetchMock = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
});
afterEach(() => vi.unstubAllGlobals());

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const listBody = {
  indicators: [
    {
      name: "kafka.topic.disk_used",
      unit: "bytes",
      appliesTo: "ENTITY_KAFKA_TOPIC",
      health: "INDICATOR_HEALTH_HEALTHY",
      lastSampleAt: "2026-09-14T10:00:00Z",
    },
    {
      name: "kafka.cluster.controller_id",
      unit: "string",
      appliesTo: "ENTITY_KAFKA_CLUSTER",
      health: "INDICATOR_HEALTH_STALE",
    },
  ],
};

test("lists indicators with unit, applies-to, health and last sample", async () => {
  fetchMock.mockResolvedValue(jsonResponse(listBody));

  renderApp(<IndicatorList />);

  const diskRow = (await screen.findByRole("link", { name: "kafka.topic.disk_used" })).closest("tr")!;
  expect(diskRow).toHaveTextContent("bytes");
  expect(diskRow).toHaveTextContent("Kafka Topic");
  expect(diskRow).toHaveTextContent("Healthy");

  const controllerRow = screen.getByRole("link", { name: "kafka.cluster.controller_id" }).closest("tr")!;
  expect(controllerRow).toHaveTextContent("Kafka Cluster");
  expect(controllerRow).toHaveTextContent("Stale");

  expect(screen.getByText("2 indicators")).toBeInTheDocument();
  expect(new URL((fetchMock.mock.calls[0][0] as Request).url).pathname).toBe("/v1/governance/indicators");
});

test("offers a register link when no indicators exist", async () => {
  fetchMock.mockResolvedValue(jsonResponse({ indicators: [] }));

  renderApp(<IndicatorList />);

  expect(await screen.findByText(/No Indicators yet/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Register Indicator" })).toHaveAttribute(
    "href",
    "/governance/indicators/register",
  );
});
