// Starts a local cluster for the tests and records where it is in
// .state.json (read by tests/helpers.js and global-teardown.js).
const { spawn, execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const STATE = path.join(__dirname, ".state.json");

function bin() {
  const exe = process.platform === "win32" ? "pumasi.exe" : "pumasi";
  return process.env.PUMASI_BIN || path.resolve(__dirname, "../../../target/debug", exe);
}

async function waitFor(what, fn, ms = 60_000) {
  const end = Date.now() + ms;
  for (;;) {
    try {
      const v = fn();
      if (v) return v;
    } catch (_) {}
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 300));
  }
}

module.exports = async () => {
  const pumasi = bin();
  const work = fs.mkdtempSync(path.join(os.tmpdir(), "pumasi-ui-e2e-"));
  const home = (n) => path.join(work, n);
  const pids = [];
  const start = (name, args) => {
    const log = fs.openSync(path.join(work, `${name}.log`), "w");
    const p = spawn(pumasi, args, { stdio: ["ignore", log, log], detached: process.platform !== "win32" });
    p.unref();
    pids.push(p.pid);
    return p;
  };
  const save = (extra) =>
    fs.writeFileSync(STATE, JSON.stringify({ work, pumasi, pids, ...extra }, null, 2));
  save({});

  start("cluster", ["--home", home("cluster"), "cluster", "start", "--net", "local", "--no-token", "--name", "e2e"]);
  const adminTicketFile = path.join(home("cluster"), "cluster", "admin.ticket");
  await waitFor("the control plane", () => fs.existsSync(adminTicketFile) && fs.statSync(adminTicketFile).size > 0);
  const join = execFileSync(pumasi, ["--home", home("cluster"), "token", "--ttl", "1h"]).toString().trim();
  for (const n of ["node-a", "node-b"]) {
    start(n, ["--home", home(n), "agent", "join", join, "--net", "local", "--name", n, "--slots", "1"]);
  }
  await waitFor(
    "both nodes online",
    () => {
      const s = execFileSync(pumasi, ["--home", home("cluster"), "status"]).toString();
      return (s.match(/ online/g) || []).length >= 2;
    },
    120_000,
  );
  const urlFile = path.join(work, "ui.url");
  const ui = start("ui", ["--home", home("cluster"), "ui", "--no-open", "--listen", "127.0.0.1:0", "--url-file", urlFile]);
  const url = await waitFor("the dashboard", () => fs.existsSync(urlFile) && fs.readFileSync(urlFile, "utf8").trim());
  const fixtures = path.join(work, "job");
  fs.mkdirSync(fixtures);
  fs.copyFileSync(path.join(__dirname, "fixtures", "ticker.wasm"), path.join(fixtures, "ticker.wasm"));
  save({
    url,
    base: new URL(url).origin,
    adminTicket: fs.readFileSync(adminTicketFile, "utf8").trim(),
    module: path.join(fixtures, "ticker.wasm"),
    // For tests that restart the dashboard or add a node (1.7).
    uiPid: ui.pid,
    clusterHome: home("cluster"),
    join,
  });
};
