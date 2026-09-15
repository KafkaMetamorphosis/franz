import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { renderApp } from "../../test/render";
import { ClientEdit } from "./ClientEdit";

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

function clientBody() {
  return {
    client: {
      name: "billing",
      frn: "frn:default:client:billing",
      labels: { "org.com/owner": "payments-team" },
    },
  };
}

function renderEdit() {
  return renderApp(
    <Routes>
      <Route path="/clients/:name/edit" element={<ClientEdit />} />
    </Routes>,
    { route: "/clients/billing/edit" },
  );
}

test("patches labels only, and keeps Save disabled until something changes", async () => {
  const user = userEvent.setup();
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(clientBody())));

  renderEdit();

  const save = await screen.findByRole("button", { name: "Save changes" });
  expect(save).toBeDisabled();

  await user.type(screen.getByLabelText("Label key"), "tier");
  await user.type(screen.getByLabelText("Label value"), "gold");
  await user.click(screen.getByRole("button", { name: "Add label" }));
  expect(save).toBeEnabled();

  await user.click(save);

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "PATCH")).toBe(true),
  );
  const patch = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "PATCH")![0] as Request;
  expect(await patch.clone().json()).toEqual({
    updateMask: "labels",
    labels: { "org.com/owner": "payments-team", tier: "gold" },
  });
});

test("shows the client name as immutable, never as an input", async () => {
  fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(clientBody())));

  renderEdit();

  await screen.findByRole("button", { name: "Save changes" });
  expect(screen.queryByLabelText(/Client name/)).not.toBeInTheDocument();
  expect(screen.getByText("billing", { selector: "code" })).toBeInTheDocument();
});
