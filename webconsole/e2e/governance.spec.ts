import { test, expect } from "@playwright/test";

// Governance console smoke (deliverable 20): from the browser only, sign in,
// register an Indicator, register a Policy against it, dry-run it, and see
// the dry-run result render without applying anything.

const stamp = Date.now().toString(36);
const INDICATOR = `e2e-indicator-${stamp}`;
const POLICY = `e2e-policy-${stamp}`;

test.beforeAll(async ({ request }) => {
  const health = await request.get("http://localhost:8080/healthz").catch(() => null);
  test.skip(!health || !health.ok(), "Franz gateway not reachable on :8080 — start Franz + Postgres first");
});

test("register an Indicator → register a Policy → dry-run → not applied", async ({ page }) => {
  const nav = page.locator(".sidebar");

  await page.goto("/login");
  await page.getByLabel("Organization or account ID").fill("acme-platform");
  await page.getByLabel("Email address").fill("op@acme.com");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Console Home" })).toBeVisible();

  // --- register the indicator ---
  await nav.getByRole("link", { name: "Indicators" }).click();
  await expect(page.getByRole("heading", { name: "Indicators" })).toBeVisible();
  await page.getByRole("link", { name: "Register Indicator" }).click();

  await page.getByLabel(/Name/).fill(INDICATOR);
  await page.getByLabel(/Unit/).fill("count");
  await page.getByLabel(/Applies to/).selectOption({ label: "Async Channel" });
  await page.getByRole("button", { name: "Register Indicator" }).click();

  await expect(page.getByRole("heading", { name: INDICATOR })).toBeVisible();
  // A never-sampled indicator's health is STALE, not "unspecified" — the
  // server derives health from last_sample_at, and no sample trivially counts
  // as beyond the staleness threshold (governance/indicator.go).
  await expect(page.getByTestId("indicator-health")).toContainText("Stale");

  // --- register a policy against it ---
  await nav.getByRole("link", { name: "Policies" }).click();
  await page.getByRole("link", { name: "Register Policy" }).click();

  await page.getByLabel(/Name/).fill(POLICY);
  await page.getByLabel(/Indicator/).selectOption({ label: `${INDICATOR} (count)` });
  await page.getByLabel("Label selector").fill("env=prod");
  await page.getByLabel(/Value/).fill("100");
  await page.getByRole("button", { name: "Add action" }).click();
  await page.getByLabel("Action 1 kind").selectOption({ label: "Set status" });
  await page.getByLabel("Action 1 arg 1").selectOption("PAUSED");
  await page.getByRole("button", { name: "Register Policy" }).click();

  await expect(page.getByRole("heading", { name: POLICY })).toBeVisible();
  await expect(page.getByTestId("policy-enabled")).toContainText("Yes");

  // --- it shows up in the list ---
  await nav.getByRole("link", { name: "Policies" }).click();
  const row = page.locator("tr", { has: page.getByRole("link", { name: POLICY }) });
  await expect(row).toContainText(INDICATOR);

  // --- dry-run: no resources match yet (no channel carries env=prod with this
  // fresh indicator sampled), but the "not applied" note always renders ---
  await page.getByRole("link", { name: POLICY }).click();
  await page.getByRole("button", { name: "Dry run" }).click();
  await expect(page.getByTestId("dry-run-not-applied")).toBeVisible();
});
