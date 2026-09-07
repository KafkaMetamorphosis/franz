import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { ChannelEdit } from "./ChannelEdit";

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

function channelBody(state = "CHANNEL_STATE_ACTIVE") {
  return {
    asyncChannel: {
      name: "billing-events",
      frn: "frn:default:async-channel:billing-events",
      type: "CHANNEL_TYPE_KAFKA_TOPIC",
      channelPartitions: 3,
      labels: { "franz.placement/env": "prod" },
      state,
    },
  };
}

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/async-channels/:name/edit" element={<ChannelEdit />} />
    </Routes>,
    { route: "/async-channels/billing-events/edit" },
  );
}

test("patches labels only, and keeps Save disabled until something changes", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(channelBody())));

  renderEdit();

  const save = await screen.findByRole("button", { name: "Save changes" });
  expect(save).toBeDisabled();

  await user.type(screen.getByLabelText("Label key"), "team");
  await user.type(screen.getByLabelText("Label value"), "payments");
  await user.click(screen.getByRole("button", { name: "Add label" }));
  expect(save).toBeEnabled();

  await user.click(save);

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "PATCH")![0] as Request;
  expect(await patch.clone().json()).toEqual({
    updateMask: "labels",
    labels: { "franz.placement/env": "prod", team: "payments" },
  });
});

test("shows type and channel partitions as immutable, never as inputs", async () => {
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(channelBody())));

  renderEdit();

  await screen.findByRole("button", { name: "Save changes" });
  expect(screen.queryByLabelText(/Channel partitions/)).not.toBeInTheDocument();
  expect(screen.queryByLabelText("Channel type")).not.toBeInTheDocument();
  expect(screen.getByText("Kafka topic")).toBeInTheDocument();
});

test("blocks editing a deleted channel", async () => {
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(channelBody("CHANNEL_STATE_DELETED"))));

  renderEdit();

  expect(await screen.findByText(/deleted and cannot be edited/)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Save changes" })).toBeDisabled();
});
