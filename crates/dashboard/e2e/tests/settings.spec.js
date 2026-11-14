// The phase 23 gate (1.7): settings changed in the browser, sign-ins that
// survive a `pumasi ui` restart, a node's availability written to its
// agent.toml, and a canary rollout between the two local agents.
const fs = require("node:fs");
const path = require("node:path");
const { spawn, execFileSync } = require("node:child_process");
const { test, expect } = require("@playwright/test");
const { state, signIn, saveState } = require("./helpers");

test.describe.configure({ mode: "serial" });

const api = (request, p, opts = {}) =>
  request.fetch(`${state().base}/api/v1${p}`, {
    ...opts,
    headers: { Authorization: `Bearer ${state().adminTicket}`, ...(opts.headers || {}) },
  });

async function until(what, fn, ms = 60_000) {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn().catch(() => null);
    if (v) return v;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 500));
  }
}

function pumasi(...args) {
  return execFileSync(state().pumasi, ["--home", state().clusterHome, ...args]).toString();
}

test("a pumasi ui restart keeps the sign-in", async ({ page, request }) => {
  await signIn(page);
  await page.goto(state().base + "/settings");
  await expect(page.locator("tr[data-session]").first()).toBeVisible();
  const mine = page.locator("tr[data-session]", { hasText: "this browser" });
  await expect(mine).toHaveCount(1);
  const id = await mine.getAttribute("data-session");

  // Stop the dashboard and start it again on the same address.
  const st = state();
  const port = new URL(st.base).port;
  try {
    process.kill(st.uiPid);
  } catch (_) {}
  await until("the old dashboard to stop", async () => {
    try {
      await request.get(`${st.base}/healthz`, { timeout: 1000 });
      return false;
    } catch (_) {
      return true;
    }
  });
  const urlFile = path.join(st.work, "ui2.url");
  const log = fs.openSync(path.join(st.work, "ui2.log"), "w");
  const ui = spawn(
    st.pumasi,
    ["--home", st.clusterHome, "ui", "--no-open", "--listen", `127.0.0.1:${port}`, "--url-file", urlFile],
    { stdio: ["ignore", log, log], detached: process.platform !== "win32" },
  );
  ui.unref();
  const url = await until("the new dashboard", async () => fs.existsSync(urlFile) && fs.readFileSync(urlFile, "utf8").trim());
  saveState({ uiPid: ui.pid, pids: [...st.pids, ui.pid], url });
  await until("the new dashboard to answer", async () => (await request.get(`${st.base}/healthz`)).ok());

  // The same browser is still signed in, as the same session.
  await page.goto(state().base + "/settings");
  await expect(page).toHaveURL(state().base + "/settings");
  await expect(page.locator(`tr[data-session="${id}"]`, { hasText: "this browser" })).toBeVisible();
  // And its buttons still pass the CSRF check.
  await page.goto(state().base + "/events");
  await expect(page.locator("#alerts-form")).toBeVisible();
});

test("an admin revokes another browser's session", async ({ page, browser }) => {
  const other = await browser.newContext();
  const theirs = await other.newPage();
  await signIn(theirs);
  await expect(theirs).toHaveURL(state().base + "/");

  // The other browser's own session, as its Settings page shows it.
  await theirs.goto(state().base + "/settings");
  const id = await theirs.locator("tr[data-session]", { hasText: "this browser" }).getAttribute("data-session");

  await signIn(page);
  await page.goto(state().base + "/settings");
  const target = page.locator(`tr[data-session="${id}"]`);
  await expect(target).not.toContainText("this browser");
  page.once("dialog", (d) => d.accept());
  await target.getByRole("button", { name: "Revoke" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText(id);
  await expect(page.locator(`tr[data-session="${id}"]`)).toHaveCount(0);

  // That browser is signed out.
  await theirs.goto(state().base + "/jobs");
  await expect(theirs).toHaveURL(/\/login$/);
  await other.close();
});

test("alert rules and webhooks from the Events page", async ({ page, request }) => {
  await signIn(page);
  await page.goto(state().base + "/events");
  const form = page.locator("#alerts-form");
  await form.locator('input[name="disk"]').fill("80%");
  await form.locator('input[name="node_offline"]').fill("3m");
  await form.locator('input[name="perf_drop"]').fill("off");
  await form.locator('input[name="repeat"]').fill("2h");
  await form.locator('input[name="resolved"]').check();
  page.once("dialog", (d) => d.accept());
  await form.getByRole("button", { name: "Save rules" }).click();
  await expect(page.locator("#alerts-form input[name='disk']")).toHaveValue("80%");
  const alerts = await (await api(request, "/alerts")).json();
  expect(alerts.policy.disk_percent).toBe(80);
  expect(alerts.policy.node_offline_secs).toBe(180);
  expect(alerts.policy.perf_drop_percent).toBeNull();
  await expect(page.locator("#alerts-form input[name='repeat']")).toHaveValue("2h");

  // A bad value is refused and changes nothing.
  await page.locator("#alerts-form input[name='disk']").fill("lots");
  page.once("dialog", (d) => d.accept());
  await page.locator("#alerts-form").getByRole("button", { name: "Save rules" }).click();
  await expect(page.locator("#flash .flash.bad")).toContainText("disk");
  expect((await (await api(request, "/alerts")).json()).policy.disk_percent).toBe(80);

  // Add a webhook, then remove it.
  await page.goto(state().base + "/events");
  const hook = page.locator("#webhook-form");
  await hook.locator('input[name="name"]').fill("e2e-hook");
  await hook.locator('input[name="url"]').fill("http://127.0.0.1:9/hook");
  await hook.locator('select[name="format"]').selectOption("json");
  await hook.getByRole("button", { name: "Add webhook" }).click();
  const item = page.locator("li", { hasText: "e2e-hook" });
  await expect(item).toBeVisible();
  page.once("dialog", (d) => d.accept());
  await item.getByRole("button", { name: "Remove" }).click();
  await expect(page.locator("li", { hasText: "e2e-hook" })).toHaveCount(0);
});

test("prices from the Prices page", async ({ page, request }) => {
  await signIn(page);
  await page.goto(state().base + "/prices");
  const form = page.locator("#prices-form");
  await form.locator('input[name="memory_gib_hour"]').fill("0.3");
  await form.locator('input[name="hold"]').fill("2h");
  page.once("dialog", (d) => d.accept());
  await form.getByRole("button", { name: "Save prices" }).click();
  await expect(page.locator("#prices-form input[name='memory_gib_hour']")).toHaveValue("0.3");
  const prices = await (await api(request, "/prices")).json();
  expect(JSON.stringify(prices)).toContain("300000");
  expect(JSON.stringify(prices)).toContain("7200");
  await expect(page.locator("dl.facts").first()).toContainText("0.3 CU");
});

test("a cron schedule: add, run now, remove", async ({ page }) => {
  await signIn(page);
  await page.goto(state().base + "/schedules");
  const form = page.locator("#schedule-form");
  await form.locator('input[name="name"]').fill("e2e-nightly");
  await form.locator('input[name="cron"]').fill("0 3 * * *");
  await form.locator('textarea[name="job"]').fill(
    ['name = "e2e-scheduled"', 'module = "ticker.wasm"', 'args = ["1"]', "deterministic = false", ""].join("\n"),
  );
  await form.locator('input[name="files"]').setInputFiles(state().module);
  await form.getByRole("button", { name: "Save schedule" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("e2e-nightly");
  const row = page.locator('tr[data-schedule="e2e-nightly"]');
  await expect(row).toBeVisible();
  await expect(row).toContainText("0 3 * * *");

  await row.getByRole("button", { name: "Run now" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("e2e-nightly");
  await expect(page.locator('tr[data-schedule="e2e-nightly"] a[href^="/jobs/"]')).toBeVisible();

  page.once("dialog", (d) => d.accept());
  await page.locator('tr[data-schedule="e2e-nightly"]').getByRole("button", { name: "Remove" }).click();
  await expect(page.locator('tr[data-schedule="e2e-nightly"]')).toHaveCount(0);
});

test("cluster.toml: preview the diff, then apply", async ({ page, request }) => {
  await signIn(page);
  await page.goto(state().base + "/settings");
  const text = page.locator('#apply-form textarea[name="text"]');
  const before = await text.inputValue();
  expect(before).toContain("[nodes.node-a]");
  expect(before).toContain("[alerts]");
  expect(before).toContain('disk = "80%"');
  // A label for node-a.
  const edited = before.replace(/(\[nodes\.node-a\]\nlabels = )\{[^}\n]*\}/, '$1{ room = "e2e" }');
  expect(edited).not.toBe(before);
  await text.fill(edited);
  await page.getByRole("button", { name: "Preview" }).click();
  const result = page.locator('#apply-result [data-applied="preview"]');
  await expect(result).toContainText("room");
  let status = await (await api(request, "/status")).json();
  let a = status.nodes.find((n) => n.info.name === "node-a");
  expect(a.config.labels.room).toBeUndefined();

  page.once("dialog", (d) => d.accept());
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(page.locator('#apply-result [data-applied="applied"]')).toContainText("room");
  status = await (await api(request, "/status")).json();
  a = status.nodes.find((n) => n.info.name === "node-a");
  expect(a.config.labels.room).toBe("e2e");

  // Schedules are refused: they name job files.
  await text.fill('[schedules.x]\ncron = "@daily"\njob = "/etc/passwd"\n');
  await page.getByRole("button", { name: "Preview" }).click();
  await expect(page.locator("#flash .flash.bad")).toContainText("Schedules page");
});

test("a node's availability is written to its agent.toml", async ({ page }) => {
  // A third agent, run as a service would run it: options in agent.toml.
  const st = state();
  const home = path.join(st.work, "node-c");
  fs.mkdirSync(path.join(home, "agent"), { recursive: true });
  fs.writeFileSync(
    path.join(home, "agent.toml"),
    'options = ["--net", "local", "--name", "node-c", "--slots", "1"]\n',
  );
  fs.writeFileSync(path.join(home, "agent", "join.ticket"), st.join);
  const log = fs.openSync(path.join(st.work, "node-c.log"), "w");
  const p = spawn(st.pumasi, ["--home", home, "service", "run", "--role", "agent"], {
    stdio: ["ignore", log, log],
    detached: process.platform !== "win32",
  });
  p.unref();
  saveState({ pids: [...state().pids, p.pid] });
  await until("node-c online", async () => / online/.test(pumasi("status").split("\n").find((l) => l.includes("node-c")) || ""), 120_000);

  await signIn(page);
  await until("node-c's settings", async () => {
    await page.goto(state().base + "/nodes/node-c");
    return (await page.locator("#node-settings").count()) > 0;
  });
  await expect(page.locator("[data-settings]")).toHaveText("1 slots");
  await expect(page.locator("#availability")).toContainText("agent.toml");
  const form = page.locator("#node-settings");
  await form.locator('input[name="slots"]').fill("2");
  await form.locator('input[name="hours"]').fill("00:00-23:59");
  await form.locator('input[name="max_cpu_percent"]').fill("50");
  page.once("dialog", (d) => d.accept());
  await form.getByRole("button", { name: "Save settings" }).click();

  // Written to the file, and the agent comes back with them.
  await until("agent.toml to change", async () => fs.readFileSync(path.join(home, "agent.toml"), "utf8").includes("00:00-23:59"));
  const opts = fs.readFileSync(path.join(home, "agent.toml"), "utf8");
  expect(opts).toContain('"--slots", "2"');
  expect(opts).toContain('"--max-cpu-percent", "50"');
  expect(opts).toContain('"--name", "node-c"');
  await until(
    "node-c back with its new settings",
    async () => {
      await page.goto(state().base + "/nodes/node-c");
      const t = await page.locator("[data-settings]").textContent({ timeout: 2000 });
      return t && t.includes("2 slots") && t.includes("00:00-23:59") && (await page.locator("[data-restarting]").count()) === 0;
    },
    120_000,
  );
  await expect(page.locator("[data-settings]")).toHaveText("2 slots · 50% CPU · 00:00-23:59");

  // node-a was started by hand: no form, and why.
  await page.goto(state().base + "/nodes/node-a");
  await expect(page.locator("#availability")).toContainText("started by hand");
  await expect(page.locator("#node-settings")).toHaveCount(0);
});

test("the node charts stream", async ({ page }) => {
  await signIn(page);
  await page.goto(state().base + "/nodes/node-a");
  const charts = page.locator("#charts[data-sse]");
  await expect(charts).toHaveAttribute("data-updated", /\d+/, { timeout: 30_000 });
  const first = await charts.getAttribute("data-updated");
  // A new sample arrives within a minute and replaces the charts.
  await expect(charts).not.toHaveAttribute("data-updated", first, { timeout: 90_000 });
});

/// A file that looks like a pumasi binary for this machine to the
/// control plane, but cannot run: the canary's smoke test fails.
function fakeBinary(file) {
  const version = pumasi("--version");
  const m = version.match(/protocol (\d+)/);
  const header = Buffer.alloc(64);
  Buffer.from("\x7fELF\x02\x01", "latin1").copy(header);
  header.writeUInt16LE(process.arch === "arm64" ? 0xb7 : 0x3e, 18);
  const marker = Buffer.from(`pumasi-build-info:v=9.9.9-e2e;p=${m ? m[1] : 14};\n`);
  fs.writeFileSync(file, Buffer.concat([header, marker]));
}

test("a canary rollout stops at the canary when it fails", async ({ page, request }) => {
  test.skip(process.platform !== "linux", "the fake binary is an ELF file");
  const bin = path.join(state().work, "pumasi-9.9.9");
  fakeBinary(bin);
  pumasi("upgrade", "rollout", bin, "--canary", "node-a", "--canary-soak", "0s");

  await signIn(page);
  await page.goto(state().base + "/nodes");
  const canary = page.locator("#canary");
  await expect(canary).toHaveAttribute("data-phase", "halted", { timeout: 90_000 });
  await expect(canary).toContainText("node-a");
  // The other nodes wait; node-b is offered for selection.
  const held = page.locator('#promote-form input[name="node"]');
  await expect(page.locator('#promote-form input[value="node-b"]')).toBeVisible();
  expect(await held.count()).toBeGreaterThanOrEqual(1);
  const view = await (await api(request, "/upgrade/canary")).json();
  expect(view.held).toContain("node-b");
  expect(Object.keys(view.phase)).toContain("Halted");

  // Upgrade node-b next by hand (it fails the same way), then cancel.
  await page.locator('#promote-form input[value="node-b"]').check();
  await page.getByRole("button", { name: "Upgrade checked next" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("node-b");
  await until("node-b to be a canary", async () => {
    const v = await (await api(request, "/upgrade/canary")).json();
    return v && v.canaries.some((c) => c.name === "node-b");
  });
  page.once("dialog", (d) => d.accept());
  await page.locator("#upgrade").getByRole("button", { name: "Cancel rollout" }).click();
  await expect(page.locator("#flash .flash.ok")).toContainText("cancelled");
  await until("the canary to end", async () => (await (await api(request, "/upgrade/canary")).json()) === null);
  // Both nodes kept their binary and are online.
  const status = await (await api(request, "/status")).json();
  for (const n of ["node-a", "node-b"]) {
    expect(status.nodes.find((x) => x.info.name === n).online).toBe(true);
  }
});

test("pages fit a phone", async ({ browser }) => {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
  const page = await ctx.newPage();
  await signIn(page);
  for (const p of ["/", "/nodes/node-a", "/events", "/prices", "/schedules", "/settings", "/jobs/new"]) {
    await page.goto(state().base + p);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
    expect(overflow, `${p} scrolls sideways`).toBeLessThanOrEqual(1);
  }
  // The alert form stacks in one column.
  await page.goto(state().base + "/events");
  const boxes = await page.locator("#alerts-form .grid2 label").evaluateAll((ls) => ls.slice(0, 2).map((l) => l.getBoundingClientRect().left));
  expect(Math.abs(boxes[0] - boxes[1])).toBeLessThan(2);
  await ctx.close();
});
