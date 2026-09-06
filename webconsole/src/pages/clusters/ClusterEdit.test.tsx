import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { ClusterEdit } from "./ClusterEdit";

const fetchMock = vi.fn();
beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
});
afterEach(() => vi.unstubAllGlobals());

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

const clusterBody = {
  kafkaCluster: {
    name: "east-1",
    frn: "frn:default:kafka-cluster:east-1",
    state: "KAFKA_CLUSTER_STATE_ACTIVE",
    connectionStrings: [{ bootstrapUrls: ["localhost:9092"], type: "CONNECTION_TYPE_PLAINTEXT" }],
    labels: {},
    clusterConfiguration: {},
    clusterProviderAgent: "prov-1",
  },
};
const agentsBody = {
  agents: [
    { name: "prov-1", type: "AGENT_TYPE_CLUSTER_PROVIDER", provisioningLabels: [] },
    { name: "prov-2", type: "AGENT_TYPE_CLUSTER_PROVIDER", provisioningLabels: [] },
  ],
};

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/kafka/clusters/:name/edit" element={<ClusterEdit />} />
    </Routes>,
    { route: "/kafka/clusters/east-1/edit" },
  );
}

test("changing the provider agent gates Save behind an explicit confirm", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation((req: Request) => {
    const url = new URL(req.url);
    if (url.pathname === "/v1/kafka/clusters/east-1") return Promise.resolve(json(clusterBody));
    if (url.pathname === "/v1/kafka/agents") return Promise.resolve(json(agentsBody));
    return Promise.resolve(json(clusterBody));
  });

  renderEdit();
  await screen.findByDisplayValue("localhost:9092");

  await user.selectOptions(screen.getByLabelText("Cluster Provider agent"), "prov-2");
  await user.click(screen.getByRole("button", { name: "Save changes" }));

  // blocked — no PATCH yet, warning shown
  expect(screen.getByRole("alert")).toHaveTextContent(/Confirm the provider re-assignment/);
  expect(fetchMock.mock.calls.filter((c) => (c[0] as Request).method === "PATCH")).toHaveLength(0);

  await user.click(screen.getByRole("checkbox", { name: /will tear its substrate down/ }));
  await user.click(screen.getByRole("button", { name: "Save changes" }));

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((c) => (c[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((c) => (c[0] as Request).method === "PATCH")![0] as Request;
  expect(await patch.clone().json()).toEqual({
    updateMask: "clusterProviderAgent",
    clusterProviderAgent: "prov-2",
  });
});
