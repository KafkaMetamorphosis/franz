import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { AgentEdit } from "./AgentEdit";

const fetchMock = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
});
afterEach(() => vi.unstubAllGlobals());

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

const agentBody = {
  agent: {
    name: "prov-1",
    frn: "frn:default:agent:prov-1",
    type: "AGENT_TYPE_CLUSTER_PROVIDER",
    status: "AGENT_STATUS_ACTIVE",
    labels: { team: "infra" },
    provisioningLabels: [],
  },
};

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/agents/:name/edit" element={<AgentEdit />} />
    </Routes>,
    { route: "/agents/prov-1/edit" },
  );
}

test("sends only the changed field in the update mask", async () => {
  const user = userEvent.setup();
  fetchMock
    .mockResolvedValueOnce(json(agentBody)) // useAgent
    .mockResolvedValueOnce(json({ agent: { ...agentBody.agent, type: "AGENT_TYPE_TELEMETRY_AGENT" } })); // PATCH

  renderEdit();
  await screen.findByDisplayValue("Cluster Provider");

  await user.selectOptions(screen.getByLabelText("Agent type"), "Telemetry Agent");
  await user.click(screen.getByRole("button", { name: "Save changes" }));

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((c) => (c[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((c) => (c[0] as Request).method === "PATCH")![0] as Request;
  const sent = await patch.clone().json();
  expect(sent).toEqual({ updateMask: "type", type: "AGENT_TYPE_TELEMETRY_AGENT" });
});

test("Save is disabled until something changes", async () => {
  fetchMock.mockResolvedValueOnce(json(agentBody));
  renderEdit();
  await screen.findByDisplayValue("Cluster Provider");
  expect(screen.getByRole("button", { name: "Save changes" })).toBeDisabled();
});

test("a 409 offers a reload-and-re-apply path", async () => {
  const user = userEvent.setup();
  fetchMock
    .mockResolvedValueOnce(json(agentBody))
    .mockResolvedValueOnce(json({ message: "stale" }, 409));

  renderEdit();
  await screen.findByDisplayValue("Cluster Provider");
  await user.selectOptions(screen.getByLabelText("Agent type"), "Custom");
  await user.click(screen.getByRole("button", { name: "Save changes" }));

  await waitFor(() =>
    expect(screen.getByRole("button", { name: /Reload and re-apply/ })).toBeInTheDocument(),
  );
});
