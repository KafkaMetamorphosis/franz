import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../../test/render";
import { PolicyRegister } from "./PolicyRegister";

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

const indicatorsBody = {
  indicators: [{ name: "kafka.topic.disk_used", unit: "bytes", appliesTo: "ENTITY_KAFKA_TOPIC" }],
};

function mockIndicatorsThenPolicyResponse(policyResponse: Response) {
  fetchMock.mockImplementation((req: Request) => {
    if (req.method === "GET") return Promise.resolve(jsonResponse(indicatorsBody));
    return Promise.resolve(policyResponse);
  });
}

test("creates a policy with indicator, matcher, limit, actions, weight and enabled", async () => {
  const user = userEvent.setup();
  mockIndicatorsThenPolicyResponse(jsonResponse({ policy: { name: "scale-out" } }));

  renderApp(<PolicyRegister />);

  await user.type(screen.getByLabelText(/Name/), "scale-out");
  await user.selectOptions(await screen.findByLabelText(/Indicator/), "kafka.topic.disk_used (bytes)");
  await user.type(screen.getByLabelText("Label selector"), "env=prod");
  await user.selectOptions(screen.getByLabelText("Operator"), "OPERATOR_GREATER_THAN");
  await user.type(screen.getByLabelText(/Value/), "150Gi");
  await user.click(screen.getByRole("button", { name: "Add action" }));
  await user.selectOptions(screen.getByLabelText("Action 1 kind"), "Increase field by");
  await user.type(screen.getByLabelText("Action 1 arg 1"), "partitions");
  await user.type(screen.getByLabelText("Action 1 arg 2"), "2");
  await user.type(screen.getByLabelText("Action 1 cap"), "max=64");
  await user.clear(screen.getByLabelText(/Weight/));
  await user.type(screen.getByLabelText(/Weight/), "5");
  await user.click(screen.getByRole("button", { name: "Register Policy" }));

  await waitFor(() =>
    expect(fetchMock.mock.calls.some((call) => (call[0] as Request).method === "POST")).toBe(true),
  );
  const post = fetchMock.mock.calls.find((call) => (call[0] as Request).method === "POST")![0] as Request;
  expect(new URL(post.url).pathname).toBe("/v1/governance/policies");
  expect(await post.clone().json()).toEqual({
    name: "scale-out",
    indicator: "kafka.topic.disk_used",
    matcher: { entity: "ENTITY_KAFKA_TOPIC", selector: "env=prod" },
    limit: { operator: "OPERATOR_GREATER_THAN", value: "150Gi" },
    actions: [{ kind: "ACTION_KIND_INCREASE_FIELD_BY", args: ["partitions", "2", "max=64"] }],
    weight: 5,
    enabled: true,
  });
});

test("rejects submitting with no actions before calling the API", async () => {
  const user = userEvent.setup();
  mockIndicatorsThenPolicyResponse(jsonResponse({ policy: {} }));

  renderApp(<PolicyRegister />);

  await user.type(screen.getByLabelText(/Name/), "no-actions");
  await user.selectOptions(await screen.findByLabelText(/Indicator/), "kafka.topic.disk_used (bytes)");
  await user.type(screen.getByLabelText(/Value/), "1");
  await user.click(screen.getByRole("button", { name: "Register Policy" }));

  expect(await screen.findByRole("alert")).toHaveTextContent(/at least one action/);
  expect(fetchMock.mock.calls.every((call) => (call[0] as Request).method === "GET")).toBe(true);
});

test("renders a whitelist-violation error inline on the offending action row", async () => {
  const user = userEvent.setup();
  mockIndicatorsThenPolicyResponse(
    jsonResponse(
      {
        message: "invalid request",
        details: [
          {
            "@type": "type.googleapis.com/google.rpc.BadRequest",
            fieldViolations: [
              { field: "actions[0]", description: "UPDATE_FIELD on replication_factor is not in the whitelist" },
            ],
          },
        ],
      },
      400,
    ),
  );

  renderApp(<PolicyRegister />);

  await user.type(screen.getByLabelText(/Name/), "bad-policy");
  await user.selectOptions(await screen.findByLabelText(/Indicator/), "kafka.topic.disk_used (bytes)");
  await user.type(screen.getByLabelText(/Value/), "1");
  await user.click(screen.getByRole("button", { name: "Add action" }));
  await user.click(screen.getByRole("button", { name: "Register Policy" }));

  // The generic ErrorBanner at the top of the page also lists every field
  // violation (established pattern) — the point of ActionEditor's own
  // `.field-error` list is that the SAME violation additionally renders
  // inline on the offending row, so an operator does not have to cross-
  // reference "actions[0]" back to a row by hand.
  const inline = await screen.findByText(/not in the whitelist/, {
    selector: ".field-error li",
  });
  expect(inline).toBeInTheDocument();
});
