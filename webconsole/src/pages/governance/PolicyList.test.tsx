import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen } from "@testing-library/react";
import { renderApp } from "../../test/render";
import { PolicyList } from "./PolicyList";

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
  policies: [
    {
      name: "scale-out",
      indicator: "kafka.topic.disk_used",
      matcher: { entity: "ENTITY_KAFKA_TOPIC", selector: "env=prod" },
      limit: { operator: "OPERATOR_GREATER_THAN", value: "150Gi" },
      actions: [{ kind: "ACTION_KIND_INCREASE_FIELD_BY", args: ["partitions", "2", "max=64"] }],
      weight: 5,
      enabled: true,
      lastFiredAt: "2026-09-14T10:00:00Z",
    },
    {
      name: "pause-noisy",
      indicator: "kafka.topic.error_rate",
      matcher: { entity: "ENTITY_ASYNC_CHANNEL", selector: "" },
      limit: { operator: "OPERATOR_GREATER_THAN_OR_EQUAL", value: "10" },
      actions: [{ kind: "ACTION_KIND_SET_STATUS", args: ["CHANNEL_STATE_PAUSED"] }],
      weight: 0,
      enabled: false,
    },
  ],
};

test("lists policies with indicator, matcher, limit, action count, weight and enabled", async () => {
  fetchMock.mockResolvedValue(jsonResponse(listBody));

  renderApp(<PolicyList />);

  const scaleRow = (await screen.findByRole("link", { name: "scale-out" })).closest("tr")!;
  expect(scaleRow).toHaveTextContent("kafka.topic.disk_used");
  expect(scaleRow).toHaveTextContent("env=prod");
  expect(scaleRow).toHaveTextContent("> 150Gi");
  expect(scaleRow).toHaveTextContent("Yes");

  const pauseRow = screen.getByRole("link", { name: "pause-noisy" }).closest("tr")!;
  expect(pauseRow).toHaveTextContent("every resource");
  expect(pauseRow).toHaveTextContent(">= 10");
  expect(pauseRow).toHaveTextContent("No");

  expect(screen.getByText("2 policies")).toBeInTheDocument();
});

test("offers a register link when no policies exist", async () => {
  fetchMock.mockResolvedValue(jsonResponse({ policies: [] }));

  renderApp(<PolicyList />);

  expect(await screen.findByText(/No Policies yet/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Register Policy" })).toHaveAttribute(
    "href",
    "/governance/policies/register",
  );
});
