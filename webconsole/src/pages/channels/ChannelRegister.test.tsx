import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../../test/render";
import { ChannelRegister } from "./ChannelRegister";

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

test("creates a channel with name, type, partitions and labels — and no access policy", async () => {
  const user = userEvent.setup();
  fetchMock.mockResolvedValueOnce(
    jsonResponse({ asyncChannel: { name: "order-events", channelPartitions: 4 } }),
  );

  renderApp(<ChannelRegister />);

  await user.type(screen.getByLabelText(/Channel name/), "order-events");
  await user.clear(screen.getByLabelText(/Channel partitions/));
  await user.type(screen.getByLabelText(/Channel partitions/), "4");
  await user.type(screen.getByLabelText("Label key"), "franz.placement/env");
  await user.type(screen.getByLabelText("Label value"), "prod");
  await user.click(screen.getByRole("button", { name: "Add label" }));
  await user.click(screen.getByRole("button", { name: "Create Async Channel" }));

  await waitFor(() => expect(fetchMock).toHaveBeenCalled());
  const request = fetchMock.mock.calls[0][0] as Request;
  expect(request.method).toBe("POST");
  expect(new URL(request.url).pathname).toBe("/v1/async-channels");
  const body = await request.clone().json();
  expect(body).toEqual({
    name: "order-events",
    type: "CHANNEL_TYPE_KAFKA_TOPIC",
    channelPartitions: 4,
    labels: { "franz.placement/env": "prod" },
  });
  // The access-policy editor ships with deliverable 17 — an empty policy means a
  // closed channel, so the field must not be sent at all.
  expect(body).not.toHaveProperty("accessPolicy");
});

test("offers Kafka topic as the only channel type", async () => {
  renderApp(<ChannelRegister />);

  const typeSelect = screen.getByLabelText("Channel type") as HTMLSelectElement;
  expect(Array.from(typeSelect.options).map((option) => option.text)).toEqual(["Kafka topic"]);
  expect(typeSelect.value).toBe("CHANNEL_TYPE_KAFKA_TOPIC");
});

test("constrains channel partitions to whole numbers of 1 or more", () => {
  renderApp(<ChannelRegister />);

  const partitions = screen.getByLabelText(/Channel partitions/);
  expect(partitions).toHaveAttribute("type", "number");
  expect(partitions).toHaveAttribute("min", "1");
  expect(partitions).toHaveAttribute("step", "1");
  expect(partitions).toBeRequired();
  expect(partitions).toHaveValue(1);
});

// The native min/step constraints above stop a bad count at the browser, so the
// submit handler's own guard is only reachable when constraint validation is
// bypassed — as a direct submit event does. It is the last line of defence
// before the API call, so it is worth covering.
test("rejects a channel-partition count below 1 before calling the API", async () => {
  const user = userEvent.setup();

  renderApp(<ChannelRegister />);

  await user.type(screen.getByLabelText(/Channel name/), "order-events");
  await user.clear(screen.getByLabelText(/Channel partitions/));
  await user.type(screen.getByLabelText(/Channel partitions/), "0");
  fireEvent.submit(screen.getByRole("button", { name: "Create Async Channel" }).closest("form")!);

  expect(await screen.findByRole("alert")).toHaveTextContent(/whole number of 1 or more/);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("surfaces a field violation from the gateway", async () => {
  const user = userEvent.setup();
  fetchMock.mockResolvedValueOnce(
    jsonResponse(
      {
        message: "invalid request",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.BadRequest",
            fieldViolations: [{ field: "name", description: "must be lower-case" }],
          },
        ],
      },
      400,
    ),
  );

  renderApp(<ChannelRegister />);
  await user.type(screen.getByLabelText(/Channel name/), "BAD");
  await user.click(screen.getByRole("button", { name: "Create Async Channel" }));

  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("must be lower-case"));
});
