import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen } from "@testing-library/react";
import { renderApp } from "../../test/render";
import { ChannelList } from "./ChannelList";

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
  asyncChannels: [
    {
      name: "billing-events",
      frn: "frn:default:async-channel:billing-events",
      type: "CHANNEL_TYPE_KAFKA_TOPIC",
      channelPartitions: 3,
      labels: { "franz.placement/env": "prod" },
      state: "CHANNEL_STATE_ACTIVE",
    },
    {
      name: "order-events",
      frn: "frn:default:async-channel:order-events",
      type: "CHANNEL_TYPE_KAFKA_TOPIC",
      channelPartitions: 1,
      labels: {},
      state: "CHANNEL_STATE_PAUSED",
    },
  ],
};

test("lists channels with FRN, type, partitions, labels and state", async () => {
  fetchMock.mockResolvedValue(jsonResponse(listBody));

  renderApp(<ChannelList />);

  const billingRow = (await screen.findByRole("link", { name: "billing-events" })).closest("tr")!;
  expect(billingRow).toHaveTextContent("frn:default:async-channel:billing-events");
  expect(billingRow).toHaveTextContent("Kafka topic");
  expect(billingRow).toHaveTextContent("3");
  expect(billingRow).toHaveTextContent("franz.placement/env=prod");
  expect(billingRow).toHaveTextContent("Active");

  const orderRow = screen.getByRole("link", { name: "order-events" }).closest("tr")!;
  expect(orderRow).toHaveTextContent("Paused");

  expect(screen.getByText("2 channels")).toBeInTheDocument();
  expect(new URL((fetchMock.mock.calls[0][0] as Request).url).pathname).toBe("/v1/async-channels");
});

test("offers a create link when no channels exist", async () => {
  fetchMock.mockResolvedValue(jsonResponse({ asyncChannels: [] }));

  renderApp(<ChannelList />);

  expect(await screen.findByText(/No Async Channels yet/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Create Async Channel" })).toHaveAttribute(
    "href",
    "/async-channels/register",
  );
});
