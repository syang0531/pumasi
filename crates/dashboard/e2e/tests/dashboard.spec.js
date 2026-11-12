// The phase 12 gate: from a browser alone, submit a job, watch it run,
// download its outputs, and drain a node; plus roles and the JSON API.
const fs = require("node:fs");
const { test, expect } = require("@playwright/test");
const { state, signIn, submitTicker, zipEntry } = require("./helpers");

test.describe.configure({ mode: "serial" });

test("the sign-in link opens the overview with both nodes", async ({ page }) => {
  await signIn(page);
  await expect(page).toHaveURL(state().base + "/");
  await expect(page.locator("header .cluster")).toHaveText("e2e");
  await expect(page.locator(".tile", { hasText: "Nodes online" }).locator(".tile-value")).toHaveText("2 / 2");
  await page.getByRole("link", { name: "Nodes", exact: true }).click();
  await expect(page.locator("tr[data-node]")).toHaveCount(2);
  await expect(page.locator('tr[data-node="node-a"] [data-state="online"]')).toBeVisible();
});

test("a wrong link and a wrong ticket are refused", async ({ browser }) => {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const res = await page.goto(state().base + "/login?token=nope");
  expect(res.status()).toBe(401);
  await page.locator("#ticket").fill("pumasiadminxyz");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.locator(".flash.bad")).toBeVisible();
  // Pages need a session.
  await page.goto(state().base + "/jobs");
  await expect(page).toHaveURL(/\/login$/);
  // A session's posts need its CSRF token.
  await page.goto(state().url);
  const forged = await page.request.post(`${state().base}/jobs/0000000000000001/cancel`);
  expect(forged.status()).toBe(403);
  // The Host header must be the dashboard's own (DNS rebinding).
  const rebound = await page.request.get(`${state().base}/healthz`, { headers: { Host: "evil.example:80" } });
  expect(rebound.status()).toBe(421);
  await ctx.close();
});

test("submit, watch the output live, download the outputs", async ({ page }) => {
  await signIn(page);
  const id = await submitTicker(page, { name: "e2e-ticker", seconds: 4, shards: 2 });

  // Live output of task 0 while it runs.
  await page.goto(`${state().base}/jobs/${id}/tasks/0`);
  const stdout = page.locator('pre[data-log="stdout"]');
  await expect(stdout).toContainText("tick", { timeout: 60_000 });
  await expect(stdout).toContainText("done", { timeout: 60_000 });
  await expect(page.locator("[data-log-status]")).toHaveText("finished");

  // Progress on the job page until it succeeds.
  await page.goto(`${state().base}/jobs/${id}`);
  await expect(page.locator('#job-state [data-state="succeeded"]')).toBeVisible({ timeout: 90_000 });
  await expect(page.locator("tr[data-task]")).toHaveCount(2);

  // Where each task ran, by name, and where its time went (1.4).
  for (const t of [0, 1]) {
    const row = page.locator(`tr[data-task="${t}"]`);
    await expect(row.locator("a", { hasText: /^node-[ab]$/ })).toBeVisible();
    await expect(row.locator("td.queued")).toHaveText(/^\d+(\.\d)? m?s$/);
    await expect(row.locator("td.run")).toContainText(/^\d+\.\d s/);
  }
  await page.goto(`${state().base}/jobs/${id}/tasks/0`);
  for (const part of ["queued", "fetch", "pull", "run", "upload"]) {
    await expect(page.locator(`dd[data-time="${part}"]`)).toHaveText(/^\d+(\.\d)? m?s$/);
  }
  await expect(page.locator('dd[data-time="pull"]')).toHaveText("0 ms");
  await page.goto(`${state().base}/jobs/${id}`);

  // One file.
  const one = page.waitForEvent("download");
  await page.locator('tr[data-task="1"] td.files a', { hasText: "result.txt" }).click();
  const file = await one;
  expect(file.suggestedFilename()).toBe("result.txt");
  expect(fs.readFileSync(await file.path(), "utf8")).toBe("pumasi dashboard ok\n");

  // Everything as a zip.
  const all = page.waitForEvent("download");
  await page.locator("#download-all").click();
  const zip = await all;
  expect(zip.suggestedFilename()).toBe(`pumasi-${id}-outputs.zip`);
  const buf = fs.readFileSync(await zip.path());
  for (const t of [0, 1]) {
    expect(zipEntry(buf, `task-${t}/result.txt`)?.toString()).toBe("pumasi dashboard ok\n");
  }

  // The job is in the list and on the overview.
  await page.goto(`${state().base}/jobs?state=succeeded`);
  await expect(page.locator(`tr[data-job="${id}"]`)).toBeVisible();
});

test("drain moves a running task to the other node", async ({ page }) => {
  await signIn(page);
  const id = await submitTicker(page, { name: "e2e-drain", seconds: 25 });
  const row = page.locator('tr[data-task="0"]');
  await expect(row.locator(".badge", { hasText: "running" })).toBeVisible({ timeout: 60_000 });
  const node = (await row.locator("td").nth(3).innerText()).trim();
  expect(["node-a", "node-b"]).toContain(node);
  const other = node === "node-a" ? "node-b" : "node-a";

  await page.goto(`${state().base}/nodes`);
  await page.locator(`tr[data-node="${node}"] a`).first().click();
  page.once("dialog", (d) => d.accept());
  await page.getByRole("button", { name: "Drain" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Tasks moved to other nodes: 1");
  await expect(page.locator('[data-state="cordoned"]').first()).toBeVisible();

  // The task runs again on the other node and the job still succeeds.
  await page.goto(`${state().base}/jobs/${id}`);
  await expect(row.locator("td").nth(3)).toHaveText(other, { timeout: 60_000 });
  await expect(row).toContainText("1 moved");
  await expect(page.locator('#job-state [data-state="succeeded"]')).toBeVisible({ timeout: 120_000 });

  // Back to normal.
  await page.goto(`${state().base}/nodes`);
  await page.locator(`tr[data-node="${node}"] a`).first().click();
  await page.getByRole("button", { name: "Uncordon" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Uncordoned");
});

test("a viewer ticket reads but cannot change anything", async ({ page, browser }) => {
  await signIn(page);
  await page.goto(`${state().base}/access`);
  const form = page.locator('form[hx-post="/access/create"]');
  await form.locator('input[name="name"]').fill("e2e-viewer");
  await form.locator('select[name="role"]').selectOption("viewer");
  await form.getByRole("button", { name: "Create" }).click();
  const ticket = (await page.locator("#access-result [data-ticket]").inputValue()).trim();
  expect(ticket).toMatch(/^pumasiadmin/);
  await expect(page.locator("#live")).toContainText("e2e-viewer");

  const ctx = await browser.newContext();
  const viewer = await ctx.newPage();
  await viewer.goto(state().base + "/login");
  await viewer.locator("#ticket").fill(ticket);
  await viewer.getByRole("button", { name: "Sign in" }).click();
  await expect(viewer).toHaveURL(state().base + "/");
  await expect(viewer.locator("header .role")).toContainText("viewer");
  await expect(viewer.getByRole("link", { name: "Invites" })).toHaveCount(0);
  await expect(viewer.getByRole("link", { name: "Submit a job" })).toHaveCount(0);
  await viewer.goto(`${state().base}/nodes`);
  await viewer.locator('tr[data-node="node-a"] a').first().click();
  await expect(viewer.getByRole("button", { name: "Drain" })).toHaveCount(0);

  // The control plane refuses it even when asked directly.
  const res = await viewer.request.post(`${state().base}/api/v1/nodes/node-a/drain`, {
    headers: { Authorization: `Bearer ${ticket}` },
  });
  expect(res.status()).toBe(403);
  expect((await res.json()).error).toContain("viewer ticket cannot drain nodes");
  await ctx.close();
});

test("an admin edits an account's quota and job policy", async ({ page, browser, request }) => {
  const base = state().base;
  const auth = { Authorization: `Bearer ${state().adminTicket}` };
  await signIn(page);
  await page.getByRole("link", { name: "Accounts", exact: true }).click();
  await expect(page.locator("h1").first()).toHaveText("Accounts");

  // A new account, from the list.
  await page.locator('form[action="/accounts/open"] input[name="account"]').fill("e2e-lab");
  await page.getByRole("button", { name: "Open or create" }).click();
  await expect(page).toHaveURL(`${base}/accounts/e2e-lab`);
  const form = page.locator("#account-form");
  await expect(form.locator("#images")).toHaveValue("");
  await form.locator("#images").fill("ghcr.io/lab/*\npython:3.12-slim");
  await form.locator('input[name="exec_wasm"]').check();
  await form.locator('input[name="exec_docker"]').check();
  await form.locator("#network").selectOption("deny");
  await form.locator("#max_timeout").fill("30m");
  await form.locator("#slots").fill("4");
  await form.getByRole("button", { name: "Save" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Saved e2e-lab");

  // The API answers what the form saved.
  const url = `${base}/api/v1/accounts/e2e-lab/policy`;
  const saved = await (await request.get(url, { headers: auth })).json();
  expect(saved.account).toBe("e2e-lab");
  expect(saved.images).toEqual(["ghcr.io/lab/*", "python:3.12-slim"]);
  expect(saved.executors).toEqual(["docker", "wasm"]);
  expect(saved.network).toBe("deny");
  expect(saved.max_timeout_ms).toBe(30 * 60 * 1000);
  expect(saved.tolerations).toBeNull();
  expect(saved.quota.slots).toBe(4);
  expect(saved.quota.gpus).toBeNull();

  // One more image through the API: the form shows it, and the rest is
  // as it was.
  const patched = await request.patch(url, {
    headers: { ...auth, "Content-Type": "application/json" },
    data: JSON.stringify({ images: { add: ["node:22-slim"] } }),
  });
  expect(patched.status()).toBe(200);
  await page.reload();
  await expect(form.locator("#images")).toHaveValue("ghcr.io/lab/*\npython:3.12-slim\nnode:22-slim");
  await expect(form.locator('input[name="exec_wasm"]')).toBeChecked();
  await expect(form.locator('input[name="exec_docker"]')).toBeChecked();
  await expect(form.locator('input[name="exec_microvm"]')).not.toBeChecked();
  await expect(form.locator("#network")).toHaveValue("deny");
  await expect(form.locator("#max_timeout")).toHaveValue("30m");
  await expect(form.locator("#slots")).toHaveValue("4");

  // Change one field in the form; a wrong value is refused and says why.
  await form.locator("#max_timeout").fill("soon");
  await form.getByRole("button", { name: "Save" }).click();
  await expect(page.locator("#flash .flash.bad")).toBeVisible();
  await form.locator("#max_timeout").fill("");
  await form.locator("#tolerations_mode").selectOption("none");
  await form.getByRole("button", { name: "Save" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Saved e2e-lab");
  const again = await (await request.get(url, { headers: auth })).json();
  expect(again.max_timeout_ms).toBeNull();
  expect(again.tolerations).toEqual([]);
  expect(again.images).toEqual(["ghcr.io/lab/*", "python:3.12-slim", "node:22-slim"]);
  expect(again.quota.slots).toBe(4);

  // The list shows the account with its use, limit and policy.
  await page.goto(`${base}/accounts`);
  const row = page.locator('tr[data-account="e2e-lab"]');
  await expect(row).toContainText("0 / 4");
  await expect(row.locator("td.policy")).toContainText("node:22-slim");

  // A viewer reads the page and cannot change it.
  const made = await request.post(`${base}/api/v1/op`, {
    headers: { ...auth, "Content-Type": "application/json" },
    data: JSON.stringify({ CreateAccess: { name: "e2e-accounts-viewer", role: "Viewer", ttl_secs: 3600 } }),
  });
  const ticket = (await made.json()).ticket;
  const ctx = await browser.newContext();
  const viewer = await ctx.newPage();
  await viewer.goto(base + "/login");
  await viewer.locator("#ticket").fill(ticket);
  await viewer.getByRole("button", { name: "Sign in" }).click();
  await viewer.goto(`${base}/accounts/e2e-lab`);
  await expect(viewer.locator("#account-form #images")).toHaveValue("ghcr.io/lab/*\npython:3.12-slim\nnode:22-slim");
  await expect(viewer.locator("#account-form #images")).toBeDisabled();
  await expect(viewer.getByRole("button", { name: "Save" })).toHaveCount(0);
  await viewer.goto(`${base}/accounts`);
  await expect(viewer.locator('form[action="/accounts/open"]')).toHaveCount(0);
  const refused = await viewer.request.patch(url, {
    headers: { Authorization: `Bearer ${ticket}`, "Content-Type": "application/json" },
    data: JSON.stringify({ network: "allow" }),
  });
  expect(refused.status()).toBe(403);
  await ctx.close();
});

test("the JSON API takes a bearer ticket", async ({ request }) => {
  const base = state().base;
  expect((await request.get(`${base}/api/v1/status`)).status()).toBe(401);
  const auth = { Authorization: `Bearer ${state().adminTicket}` };
  const status = await request.get(`${base}/api/v1/status`, { headers: auth });
  expect(status.status()).toBe(200);
  const body = await status.json();
  expect(body.cluster_name).toBe("e2e");
  expect(body.nodes).toHaveLength(2);
  expect(body.jobs.length).toBeGreaterThanOrEqual(2);
  expect(body.jobs[0].id).toMatch(/^[0-9a-f]{16}$/);
  const who = await request.post(`${base}/api/v1/op`, {
    headers: { ...auth, "Content-Type": "application/json" },
    data: JSON.stringify("WhoAmI"),
  });
  expect((await who.json()).role).toBe("Admin");
  const doc = await request.get(`${base}/api/openapi.json`);
  expect((await doc.json()).openapi).toBe("3.1.0");
  const ev = await request.get(`${base}/api/v1/events?limit=5`, { headers: auth });
  expect(Array.isArray(await ev.json())).toBe(true);
});

test("upload a dataset and download it as a zip", async ({ page }) => {
  await signIn(page);
  await page.goto(`${state().base}/storage`);
  await page.locator("#ds-name").fill("e2e-data");
  await page.locator("#ds-files").setInputFiles({
    name: "hello.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("안녕 pumasi\n"),
  });
  await page.getByRole("button", { name: "Upload", exact: true }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("e2e-data@1");
  const row = page.locator('tr[data-dataset="e2e-data"]');
  await expect(row).toBeVisible();
  const dl = page.waitForEvent("download");
  await row.locator("a[download]").click();
  const zip = await dl;
  expect(zip.suggestedFilename()).toBe("e2e-data-v1.zip");
  expect(zipEntry(fs.readFileSync(await zip.path()), "hello.txt")?.toString()).toBe("안녕 pumasi\n");
});

test("the other pages load", async ({ page }) => {
  await signIn(page);
  for (const [path, heading] of [
    ["/cu", "Compute units (CU)"],
    ["/accounts", "Accounts"],
    ["/prices", "Prices"],
    ["/events", "Events and alerts"],
    ["/access", "Invites and access"],
    ["/storage/e2e-data", "Storage · e2e-data"],
  ]) {
    const res = await page.goto(state().base + path);
    expect(res.status(), path).toBe(200);
    await expect(page.locator("h1").first()).toHaveText(heading);
  }
  await expect(page.locator("#live")).toContainText("v1");
  // An invite shows the install commands.
  await page.goto(`${state().base}/access`);
  const invite = page.locator('form[hx-post="/access/invite"]');
  await invite.locator('input[name="account"]').fill("friend");
  await invite.getByRole("button", { name: "Create invite" }).click();
  await expect(page.locator("#invite-result pre").first()).toContainText("install.sh | sudo sh -s -- --ticket pumasijoin");
});

test("Korean and back", async ({ page }) => {
  await signIn(page);
  await page.getByRole("link", { name: "한국어" }).click();
  await expect(page.getByRole("link", { name: "노드", exact: true })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "ko");
  await page.getByRole("link", { name: "English" }).click();
  await expect(page.getByRole("link", { name: "Nodes", exact: true })).toBeVisible();
});
