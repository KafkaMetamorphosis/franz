import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { PolicyEdit } from "./PolicyEdit";

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

function policyBody() {
  return {
    policy: {
      name: "scale-out",
      frn: "frn:default:policy:scale-out",
      indicator: "kafka.topic.disk_used",
      matcher: { entity: "ENTITY_KAFKA_TOPIC", selector: "env=prod" },
      limit: { operator: "OPERATOR_GREATER_THAN", value: "150Gi" },
      actions: [{ kind: "ACTION_KIND_INCREASE_FIELD_BY", args: ["partitions", "2", "max=64"] }],
      weight: 5,
      enabled: true,
    },
  };
}

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/governance/policies/:name/edit" element={<PolicyEdit />} />
    </Routes>,
    { route: "/governance/policies/scale-out/edit" },
  );
}

test("patches only the changed fields, and keeps Save disabled until something changes", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(policyBody())));

  renderEdit();

  const save = await screen.findByRole("button", { name: "Save changes" });
  expect(save).toBeDisabled();

  const weight = screen.getByLabelText("Weight");
  await user.clear(weight);
  await user.type(weight, "10");
  expect(save).toBeEnabled();

  await user.click(save);

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "PATCH")![0] as Request;
  const body = await patch.clone().json();
  expect(body.updateMask).toBe("weight");
  expect(body.weight).toBe(10);
  // matcher/limit/actions are unchanged but still forwarded — the mask is what
  // tells the server which fields to apply, not what's present in the body.
  expect(body.matcher).toEqual({ entity: "ENTITY_KAFKA_TOPIC", selector: "env=prod" });
});

test("shows the indicator as immutable, never as an input", async () => {
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(policyBody())));

  renderEdit();

  await screen.findByRole("button", { name: "Save changes" });
  expect(screen.queryByLabelText(/Indicator/)).not.toBeInTheDocument();
  expect(screen.getByText("kafka.topic.disk_used")).toBeInTheDocument();
});

test("removing the only action disables Save until a replacement is added", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(policyBody())));

  renderEdit();

  await screen.findByRole("button", { name: "Save changes" });
  await user.click(screen.getByRole("button", { name: "Remove" }));

  const save = screen.getByRole("button", { name: "Save changes" });
  expect(save).toBeEnabled();
  await user.click(save);

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "PATCH")![0] as Request;
  const body = await patch.clone().json();
  expect(body.updateMask).toBe("actions");
  expect(body.actions).toEqual([]);
});
