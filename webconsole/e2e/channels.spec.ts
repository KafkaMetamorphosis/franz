import { test, expect } from "@playwright/test";

// Async Channel console smoke (deliverable 19): from the browser only, sign in,
// create an Async Channel, see it in the list, open its detail page and pause it.
// Access-policy and client-access screens are deliverable 17, so nothing here
// touches them.

const stamp = Date.now().toString(36);
const CHANNEL = `e2e-channel-${stamp}`;

test.beforeAll(async ({ request }) => {
  const health = await request.get("http://localhost:8080/healthz").catch(() => null);
  test.skip(!health || !health.ok(), "Franz gateway not reachable on :8080 — start Franz + Postgres first");
});

test("create an Async Channel → list → detail → pause", async ({ page }) => {
  const nav = page.locator(".sidebar");

  await page.goto("/login");
  await page.getByLabel("Organization or account ID").fill("acme-platform");
  await page.getByLabel("Email address").fill("op@acme.com");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Console Home" })).toBeVisible();

  // --- create the channel ---
  await nav.getByRole("link", { name: "Channels" }).click();
  await expect(page.getByRole("heading", { name: "Async Channels" })).toBeVisible();
  await page.getByRole("link", { name: "Create Async Channel" }).click();

  await page.getByLabel(/Channel name/).fill(CHANNEL);
  await page.getByLabel(/Channel partitions/).fill("2");
  await page.getByLabel("Label key").fill("franz.placement/env");
  await page.getByLabel("Label value").fill("prod");
  await page.getByRole("button", { name: "Add label" }).click();
  await page.getByRole("button", { name: "Create Async Channel" }).click();

  // --- detail page renders the declared intent ---
  await expect(page.getByRole("heading", { name: CHANNEL })).toBeVisible();
  await expect(page.getByTestId("channel-state")).toContainText("Active");
  await expect(page.getByTestId("shard-placement")).toContainText("0 of 2 placed");
  await expect(page.getByText("franz.placement/env=prod")).toBeVisible();

  // --- it shows up in the list ---
  await nav.getByRole("link", { name: "Channels" }).click();
  const row = page.locator("tr", { has: page.getByRole("link", { name: CHANNEL }) });
  await expect(row).toContainText("Kafka topic");
  await expect(row).toContainText("Active");

  // --- pause it, and the state sticks across a reload ---
  await page.getByRole("link", { name: CHANNEL }).click();
  await page.getByRole("button", { name: "Pause" }).click();
  await expect(page.getByTestId("channel-state")).toContainText("Paused");
  await page.reload();
  await expect(page.getByTestId("channel-state")).toContainText("Paused");
  await expect(page.getByRole("button", { name: "Resume" })).toBeVisible();
});
