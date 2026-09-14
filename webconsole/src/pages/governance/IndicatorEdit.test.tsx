import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { IndicatorEdit } from "./IndicatorEdit";

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

function indicatorBody() {
  return {
    indicator: {
      name: "kafka.topic.disk_used",
      unit: "bytes",
      appliesTo: "ENTITY_KAFKA_TOPIC",
      health: "INDICATOR_HEALTH_HEALTHY",
      stalenessThreshold: "5m",
      sourceAgents: ["gregor-samsa"],
    },
  };
}

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/governance/indicators/:name/edit" element={<IndicatorEdit />} />
    </Routes>,
    { route: "/governance/indicators/kafka.topic.disk_used/edit" },
  );
}

test("patches only the changed fields, and keeps Save disabled until something changes", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(indicatorBody())));

  renderEdit();

  const save = await screen.findByRole("button", { name: "Save changes" });
  expect(save).toBeDisabled();

  const staleness = screen.getByLabelText("Staleness threshold");
  await user.clear(staleness);
  await user.type(staleness, "15m");
  expect(save).toBeEnabled();

  await user.click(save);

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "PATCH")![0] as Request;
  const body = await patch.clone().json();
  expect(body.updateMask).toBe("staleness_threshold");
  expect(body.stalenessThreshold).toBe("15m");
  expect(body.unit).toBe("bytes");
});

test("shows applies-to as immutable, never as an input", async () => {
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(indicatorBody())));

  renderEdit();

  await screen.findByRole("button", { name: "Save changes" });
  expect(screen.queryByLabelText(/Applies to/)).not.toBeInTheDocument();
  expect(screen.getByText("Kafka Topic")).toBeInTheDocument();
});
