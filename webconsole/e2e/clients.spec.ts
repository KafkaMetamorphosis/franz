import { test, expect } from "@playwright/test";

// Client console smoke (deliverable 21): from the browser only, sign in,
// register a Client, see it listed, open its detail page, edit its labels,
// delete it, and confirm re-registering the same name is rejected (003.10
// "DeleteClient does not free the name / FRN").

const stamp = Date.now().toString(36);
const CLIENT = `e2e-client-${stamp}`;

test.beforeAll(async ({ request }) => {
  const health = await request.get("http://localhost:8080/healthz").catch(() => null);
  test.skip(!health || !health.ok(), "Franz gateway not reachable on :8080 — start Franz + Postgres first");
});

test("register a Client → list → detail → edit labels → delete → recreate rejected", async ({ page }) => {
  const nav = page.locator(".sidebar");

  await page.goto("/login");
  await page.getByLabel("Organization or account ID").fill("acme-platform");
  await page.getByLabel("Email address").fill("op@acme.com");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Console Home" })).toBeVisible();

  // --- register the client ---
  await nav.getByRole("link", { name: "Clients" }).click();
  await expect(page.getByRole("heading", { name: "Clients" })).toBeVisible();
  await page.getByRole("link", { name: "Register Client" }).click();

  await page.getByLabel(/Client name/).fill(CLIENT);
  await page.getByLabel("Label key").fill("org.com/owner");
  await page.getByLabel("Label value").fill("platform-team");
  await page.getByRole("button", { name: "Add label" }).click();
  await page.getByRole("button", { name: "Register Client" }).click();

  await expect(page.getByRole("heading", { name: CLIENT })).toBeVisible();

  // --- it shows up in the list ---
  await nav.getByRole("link", { name: "Clients" }).click();
  const row = page.locator("tr", { has: page.getByRole("link", { name: CLIENT }) });
  await expect(row).toContainText("platform-team");

  // --- edit its labels ---
  await page.getByRole("link", { name: CLIENT }).click();
  await page.getByRole("link", { name: "Edit" }).click();
  await page.getByLabel("Label key").fill("tier");
  await page.getByLabel("Label value").fill("gold");
  await page.getByRole("button", { name: "Add label" }).click();
  await page.getByRole("button", { name: "Save changes" }).click();
  await expect(page.getByText("tier=gold")).toBeVisible();

  // --- delete it, then confirm re-registering the same name is rejected ---
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByRole("heading", { name: "Clients" })).toBeVisible();

  await page.getByRole("link", { name: "Register Client" }).click();
  await page.getByLabel(/Client name/).fill(CLIENT);
  await page.getByRole("button", { name: "Register Client" }).click();
  await expect(page.getByRole("alert")).toContainText("cannot be reused");
});
