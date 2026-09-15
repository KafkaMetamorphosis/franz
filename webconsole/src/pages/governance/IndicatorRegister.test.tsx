import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../../test/render";
import { IndicatorRegister } from "./IndicatorRegister";

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

test("creates an indicator with name, unit, applies-to, staleness threshold and source agents", async () => {
  const user = userEvent.setup();
  fetchMock.mockResolvedValueOnce(jsonResponse({ indicator: { name: "kafka.topic.disk_used" } }));

  renderApp(<IndicatorRegister />);

  await user.type(screen.getByLabelText(/Name/), "kafka.topic.disk_used");
  await user.type(screen.getByLabelText(/Unit/), "bytes");
  await user.selectOptions(screen.getByLabelText(/Applies to/), "Kafka Topic");
  await user.clear(screen.getByLabelText(/Staleness threshold/));
  await user.type(screen.getByLabelText(/Staleness threshold/), "10m");
  await user.type(screen.getByLabelText("Source agent"), "gregor-samsa");
  await user.click(screen.getByRole("button", { name: "Add agent" }));
  await user.click(screen.getByRole("button", { name: "Register Indicator" }));

  await waitFor(() => expect(fetchMock).toHaveBeenCalled());
  const request = fetchMock.mock.calls[0][0] as Request;
  expect(request.method).toBe("POST");
  expect(new URL(request.url).pathname).toBe("/v1/governance/indicators");
  expect(await request.clone().json()).toEqual({
    name: "kafka.topic.disk_used",
    unit: "bytes",
    appliesTo: "ENTITY_KAFKA_TOPIC",
    stalenessThreshold: "10m",
    sourceAgents: ["gregor-samsa"],
  });
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
            fieldViolations: [{ field: "unit", description: "must not be empty" }],
          },
        ],
      },
      400,
    ),
  );

  renderApp(<IndicatorRegister />);
  await user.type(screen.getByLabelText(/Name/), "x");
  await user.type(screen.getByLabelText(/Unit/), "x");
  await user.click(screen.getByRole("button", { name: "Register Indicator" }));

  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("must not be empty"));
});
