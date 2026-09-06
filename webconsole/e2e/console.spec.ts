import { test, expect } from "@playwright/test";

// Scoped-down 06.7 smoke (the real local-kafka-agent lands in deliverable 07, so
// nothing reports READY yet): from the browser only, sign in, register a
// CLUSTER_PROVIDER agent, copy its token, register a Kafka Cluster pointing at
// it, and confirm the detail page renders the provider-status panel + timeline
// (empty — "no report yet"). The full "reaches READY" flow is 07's e2e.

const stamp = Date.now().toString(36);
const AGENT = `e2e-agent-${stamp}`;
const CLUSTER = `e2e-cluster-${stamp}`;

test.beforeAll(async ({ request }) => {
  const health = await request.get("http://localhost:8080/healthz").catch(() => null);
  test.skip(!health || !health.ok(), "Franz gateway not reachable on :8080 — start Franz + Postgres first");
});

test("register agent → token → register cluster → provider status panel", async ({ page }) => {
  const nav = page.locator(".sidebar");

  await page.goto("/login");
  await page.getByLabel("Organization or account ID").fill("acme-platform");
  await page.getByLabel("Email address").fill("op@acme.com");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Console Home" })).toBeVisible();

  // --- register a Cluster Provider agent ---
  await nav.getByRole("link", { name: "Agents" }).click();
  await page.getByRole("link", { name: "Register Agent" }).click();
  await page.getByLabel(/Agent name/).fill(AGENT);
  await page.getByLabel("Agent type").selectOption({ label: "Cluster Provider" });
  await page.getByRole("button", { name: "Register Agent" }).click();

  const token = page.getByTestId("agent-token");
  await expect(token).toBeVisible();
  await expect(token).toContainText("frnat_");
  await page.getByRole("button", { name: "Copy token" }).click();

  // --- register a cluster pointing at it ---
  await page.getByRole("link", { name: "Open agent" }).click();
  await expect(page.getByRole("heading", { name: AGENT })).toBeVisible();

  await nav.getByRole("link", { name: "Clusters" }).click();
  await page.getByRole("link", { name: "Register Kafka Cluster" }).click();
  await page.getByLabel(/Cluster name/).fill(CLUSTER);
  await page.getByLabel(/Bootstrap URL/).fill("localhost:9092");
  await page.getByLabel("Cluster Provider").selectOption(AGENT);
  await page.getByLabel("deployment-type").fill("local-docker");
  await page.getByRole("button", { name: "Register Kafka Cluster" }).click();

  // --- detail page: intent + provider-status panel + timeline ---
  await expect(page.getByRole("heading", { name: CLUSTER })).toBeVisible();
  await expect(page.getByText("Provider status")).toBeVisible();
  await expect(page.getByTestId("provider-phase")).toContainText(/no report yet/i);
  await expect(page.getByText("Provider event timeline")).toBeVisible();
  await expect(page.getByText("No provider events yet.")).toBeVisible();
  await expect(page.getByRole("link", { name: AGENT })).toBeVisible();
});
