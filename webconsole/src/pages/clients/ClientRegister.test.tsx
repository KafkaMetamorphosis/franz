import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../../test/render";
import { ClientRegister } from "./ClientRegister";

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

test("creates a client with name and labels", async () => {
  const user = userEvent.setup();
  fetchMock.mockResolvedValueOnce(jsonResponse({ client: { name: "billing" } }));

  renderApp(<ClientRegister />);

  await user.type(screen.getByLabelText(/Client name/), "billing");
  await user.type(screen.getByLabelText("Label key"), "org.com/owner");
  await user.type(screen.getByLabelText("Label value"), "payments-team");
  await user.click(screen.getByRole("button", { name: "Add label" }));
  await user.click(screen.getByRole("button", { name: "Register Client" }));

  await waitFor(() => expect(fetchMock).toHaveBeenCalled());
  const request = fetchMock.mock.calls[0][0] as Request;
  expect(request.method).toBe("POST");
  expect(new URL(request.url).pathname).toBe("/v1/clients");
  expect(await request.clone().json()).toEqual({
    name: "billing",
    labels: { "org.com/owner": "payments-team" },
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
            fieldViolations: [{ field: "name", description: "already reserved" }],
          },
        ],
      },
      400,
    ),
  );

  renderApp(<ClientRegister />);
  await user.type(screen.getByLabelText(/Client name/), "billing");
  await user.click(screen.getByRole("button", { name: "Register Client" }));

  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("already reserved"));
});
