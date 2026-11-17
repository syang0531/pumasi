// The phase 31 gate (1.15): secrets from the Settings page, a schedule and
// per-node thresholds and a silence through the cluster.toml editor, node
// charts pushed by the control plane, events in Korean, and the store in
// the network matrix.
const { test, expect } = require("@playwright/test");
const { state, signIn } = require("./helpers");

test.describe.configure({ mode: "serial" });

const api = (request, p, opts = {}) =>
  request.fetch(`${state().base}/api/v1${p}`, {
    ...opts,
    headers: { Authorization: `Bearer ${state().adminTicket}`, ...(opts.headers || {}) },
  });

/// Accepts every dialog on the page from now on.
function acceptDialogs(page) {
  if (page._acceptsDialogs) return;
  page._acceptsDialogs = true;
  page.on("dialog", (d) => d.accept().catch(() => {}));
}

async function until(what, fn, ms = 60_000) {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn().catch(() => null);
    if (v) return v;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 500));
  }
}

test("secrets: add, rotate, remove, and never shown", async ({ page }) => {
  await signIn(page);
  await page.goto(state().base + "/settings");
  const form = page.locator("#secret-form");
  await form.locator('input[name="name"]').fill("registry/e2e.example");
  await form.locator('input[name="value"]').fill("first-value-e2e");
  await form.getByRole("button", { name: "Add secret" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("registry/e2e.example");
  const row = page.locator('tr[data-secret="registry/e2e.example"]');
  await expect(row).toBeVisible();
  expect(await page.content()).not.toContain("first-value-e2e");
  await expect(form.locator('input[name="value"]')).toHaveValue("");

  // Adding it again does not replace it by accident.
  await form.locator('input[name="name"]').fill("registry/e2e.example");
  await form.locator('input[name="value"]').fill("other");
  await form.getByRole("button", { name: "Add secret" }).click();
  await expect(page.locator("#flash .flash.bad")).toContainText("rotate");

  // Rotate: a new value, still not shown.
  await row.locator('input[name="value"]').fill("second-value-e2e");
  await row.getByRole("button", { name: "Rotate" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Rotated");
  expect(await page.content()).not.toContain("second-value-e2e");

  page.once("dialog", (d) => d.accept());
  await page.locator('tr[data-secret="registry/e2e.example"]').getByRole("button", { name: "Remove" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("Removed");
  await expect(page.locator('tr[data-secret="registry/e2e.example"]')).toHaveCount(0);
});

test("cluster.toml: a schedule's cron changes and its job stays", async ({ page }) => {
  await signIn(page);
  await page.goto(state().base + "/schedules");
  const form = page.locator("#schedule-form");
  await form.locator('input[name="name"]').fill("e2e-edit");
  await form.locator('input[name="cron"]').fill("0 4 * * *");
  await form.locator('input[name="tz"]').fill("Asia/Seoul");
  await form.locator('textarea[name="job"]').fill(
    ['name = "e2e-edit-job"', 'module = "ticker.wasm"', 'args = ["1"]', "deterministic = false", ""].join("\n"),
  );
  await form.locator('input[name="files"]').setInputFiles(state().module);
  await form.getByRole("button", { name: "Save schedule" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("e2e-edit");

  await page.goto(state().base + "/settings");
  const text = page.locator('#apply-form textarea[name="text"]');
  const before = await text.inputValue();
  expect(before).toContain('[schedules.e2e-edit]\ncron = "0 4 * * *"\ntz = "Asia/Seoul"\noverlap = false');
  await text.fill(before.replace('cron = "0 4 * * *"', 'cron = "15 5 * * *"'));
  await page.getByRole("button", { name: "Preview" }).click();
  await expect(page.locator('#apply-result [data-applied="preview"]')).toContainText(
    "schedule e2e-edit: updated (cron 0 4 * * * -> 15 5 * * *)",
  );
  // (The Apply button's confirm is answered when a dialog comes.)
  acceptDialogs(page);
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(page.locator('#apply-result [data-applied="applied"]')).toContainText("schedule e2e-edit");

  await page.goto(state().base + "/schedules");
  const row = page.locator('tr[data-schedule="e2e-edit"]');
  await expect(row).toContainText("15 5 * * * (Asia/Seoul)");
  await expect(row).toContainText("e2e-edit-job");
  await row.getByRole("button", { name: "Remove" }).click();
  await expect(page.locator('tr[data-schedule="e2e-edit"]')).toHaveCount(0);
});

test("cluster.toml: thresholds for one node and a silence", async ({ page, request }) => {
  await signIn(page);
  await page.goto(state().base + "/settings");
  const text = page.locator('#apply-form textarea[name="text"]');
  const before = await text.inputValue();
  expect(before).toContain("repeat = ");
  const until = new Date(Date.now() + 86_400_000).toISOString().replace(/\.\d+Z$/, "Z");
  const added = [
    "",
    "[alerts.nodes.node-b]",
    'temperature = "off"',
    'disk = "97%"',
    "",
    "[silences.e2e-quiet]",
    'rule = "disk"',
    'nodes = "name=node-a"',
    `until = "${until}"`,
    'comment = "from the editor"',
    "",
  ].join("\n");
  await text.fill(before + added);
  await page.getByRole("button", { name: "Preview" }).click();
  const preview = page.locator('#apply-result [data-applied="preview"]');
  await expect(preview).toContainText("alerts on name=node-b: disk 97%, temperature off");
  await expect(preview).toContainText("silence e2e-quiet: created");
  // (The Apply button's confirm is answered when a dialog comes.)
  acceptDialogs(page);
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(page.locator('#apply-result [data-applied="applied"]')).toContainText("silence e2e-quiet");

  // The editor shows the thresholds and the silence now, and applying it
  // again changes nothing.
  await page.goto(state().base + "/settings");
  const after = await text.inputValue();
  expect(after).toContain('[alerts.nodes.node-b]\ndisk = "97%"\ntemperature = "off"');
  // And the silence, by its name (1.17).
  expect(after).toContain(`[silences.e2e-quiet]\nrule = "disk"\nnodes = "name=node-a"\nuntil = ${until}\ncomment = "from the editor"`);
  await page.getByRole("button", { name: "Preview" }).click();
  await expect(page.locator('#apply-result [data-applied="preview"]')).toContainText("Nothing to change");

  // The silence is listed on the Events page.
  await page.goto(state().base + "/events");
  await expect(page.locator("body")).toContainText("from the editor");
  const alerts = await (await api(request, "/alerts")).json();
  expect(alerts).toBeTruthy();
});

test("the node charts are pushed, not polled", async ({ page }) => {
  await signIn(page);
  await page.goto(state().base + "/nodes/node-a");
  const pushed = page.locator('#charts [data-source="push"]');
  await expect(pushed).toHaveAttribute("data-latest", /^[1-9]\d*$/, { timeout: 30_000 });
  const first = Number(await pushed.getAttribute("data-latest"));
  // The next sample comes with the next push and moves the charts.
  await until(
    "a newer sample",
    async () => Number(await page.locator('#charts [data-source="push"]').getAttribute("data-latest", { timeout: 1000 })) > first,
    60_000,
  );
  await expect(page.locator('#charts [data-source="poll"]')).toHaveCount(0);
});

test("events in Korean on the Korean dashboard", async ({ page }) => {
  await signIn(page);
  await page.getByRole("link", { name: "한국어" }).click();
  await page.goto(state().base + "/events");
  const table = page.locator("table").first();
  await expect(table).toContainText(/노드 node-[abc] (온라인|참여)/);
  await expect(table).not.toContainText(/node node-[abc] (online|joined)/);
  await page.getByRole("link", { name: "English" }).click();
  await page.goto(state().base + "/events");
  await expect(page.locator("table").first()).toContainText(/node node-[abc] (online|joined)/);
});

test("the network matrix has the store", async ({ page }) => {
  await signIn(page);
  // One pair is probed every 20 s: the store's turn comes within a few.
  await until(
    "a probe to the store",
    async () => {
      await page.goto(state().base + "/nodes");
      return (await page.locator("#net th[data-store]").count()) > 0;
    },
    150_000,
  );
});
