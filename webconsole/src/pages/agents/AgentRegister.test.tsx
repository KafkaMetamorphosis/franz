import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../../test/render";
import { AgentRegister } from "./AgentRegister";

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

test("registers an agent and reveals the token once", async () => {
  const user = userEvent.setup();
  fetchMock.mockResolvedValueOnce(
    jsonResponse({ agent: { name: "local-docker", type: "AGENT_TYPE_CLUSTER_PROVIDER" }, token: "frnat_secret123" }),
  );

  renderApp(<AgentRegister />);

  await user.type(screen.getByLabelText(/Agent name/), "local-docker");
  await user.click(screen.getByRole("button", { name: "Register Agent" }));

  await waitFor(() => expect(screen.getByTestId("agent-token")).toHaveTextContent("frnat_secret123"));
  expect(screen.getByText(/registered/i)).toBeInTheDocument();

  const request = fetchMock.mock.calls[0][0] as Request;
  expect(request.method).toBe("POST");
  expect(new URL(request.url).pathname).toBe("/v1/kafka/agents");
  expect(await request.clone().json()).toMatchObject({
    name: "local-docker",
    type: "AGENT_TYPE_CLUSTER_PROVIDER",
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
            fieldViolations: [{ field: "name", description: "must be lower-case" }],
          },
        ],
      },
      400,
    ),
  );

  renderApp(<AgentRegister />);
  await user.type(screen.getByLabelText(/Agent name/), "BAD");
  await user.click(screen.getByRole("button", { name: "Register Agent" }));

  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("must be lower-case"));
});
