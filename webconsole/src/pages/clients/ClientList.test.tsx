import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen } from "@testing-library/react";
import { renderApp } from "../../test/render";
import { ClientList } from "./ClientList";

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

const listBody = {
  clients: [
    {
      name: "billing",
      frn: "frn:default:client:billing",
      labels: { "org.com/owner": "payments-team", tier: "gold" },
    },
    {
      name: "payments-consumer",
      frn: "frn:default:client:payments-consumer",
      labels: {},
    },
  ],
};

test("lists clients with FRN, owner and other labels", async () => {
  fetchMock.mockResolvedValue(jsonResponse(listBody));

  renderApp(<ClientList />);

  const billingRow = (await screen.findByRole("link", { name: "billing" })).closest("tr")!;
  expect(billingRow).toHaveTextContent("frn:default:client:billing");
  expect(billingRow).toHaveTextContent("payments-team");
  expect(billingRow).toHaveTextContent("tier=gold");
  // the owner label is pulled into its own column, not duplicated as a tag
  expect(billingRow).not.toHaveTextContent("org.com/owner=payments-team");

  const consumerRow = screen.getByRole("link", { name: "payments-consumer" }).closest("tr")!;
  expect(consumerRow).toHaveTextContent("—");

  expect(screen.getByText("2 clients")).toBeInTheDocument();
  expect(new URL((fetchMock.mock.calls[0][0] as Request).url).pathname).toBe("/v1/clients");
});

test("offers a register link when no clients exist", async () => {
  fetchMock.mockResolvedValue(jsonResponse({ clients: [] }));

  renderApp(<ClientList />);

  expect(await screen.findByText(/No Clients yet/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Register Client" })).toHaveAttribute("href", "/clients/register");
});
