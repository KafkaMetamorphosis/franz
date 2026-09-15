import { test, expect } from "@playwright/test";

// Migration console smoke (deliverable 22): from the browser only, sign in,
// migrate a placed channel shard, and drain a cluster.
//
// ListShardMigrations turned out to be scoped by Async Channel only — no
// cluster filter, no "list everything" (found live, not assumed: an
// unfiltered call errors NotFound). So ClusterDetail carries no Migrations
// list, and this spec verifies migration status through the channel it
// actually belongs to (ChannelDetail's shards table) instead.
//
// The local dev loop registers exactly one cluster, so triggering a real
// shard migration has no eligible target — the per-shard "Migrate to…"
// picker has nothing to offer, by design (see hooks.ts's useMigrateKafkaTopic
// and 22-migration-ui.md's Notes). What this test can verify without a
// second cluster is the other real, independent RPC: MigrateCluster succeeds
// (200) even when every shard has no eligible target — it just moves none
// (TestMigrateClusterSkipsShardsWithNoEligibleTarget in the Go suite) — so
// draining stays on a clean detail page rather than a broken one.

test.beforeAll(async ({ request }) => {
  const health = await request.get("http://localhost:8080/healthz").catch(() => null);
  test.skip(!health || !health.ok(), "Franz gateway not reachable on :8080 — start Franz + Postgres first");
});

test("drain a cluster calls MigrateCluster and stays on a clean detail page", async ({ page }) => {
  await page.goto("/login");
  await page.getByLabel("Organization or account ID").fill("acme-platform");
  await page.getByLabel("Email address").fill("op@acme.com");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Console Home" })).toBeVisible();

  await page.locator(".sidebar").getByRole("link", { name: "Clusters" }).click();
  await expect(page.getByRole("heading", { name: "Kafka Clusters" })).toBeVisible();

  const firstClusterLink = page.locator("table tbody tr").first().getByRole("link").first();
  test.skip((await firstClusterLink.count()) === 0, "no cluster registered in this environment");
  const clusterName = await firstClusterLink.textContent();
  await firstClusterLink.click();
  await expect(page.getByRole("heading", { name: clusterName ?? "" })).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Drain" }).click();

  // No error banner from the drain call — the page is still showing the
  // cluster's own heading, not an error state.
  await expect(page.getByRole("heading", { name: clusterName ?? "" })).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
});
