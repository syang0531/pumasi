const fs = require("node:fs");
const path = require("node:path");

module.exports = async () => {
  const file = path.join(__dirname, ".state.json");
  if (!fs.existsSync(file)) return;
  const st = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const pid of st.pids.reverse()) {
    try {
      process.kill(pid);
    } catch (_) {}
  }
  if (process.env.CI && st.work) {
    // Logs for the CI artifact.
    const out = path.join(__dirname, "test-results", "cluster-logs");
    fs.mkdirSync(out, { recursive: true });
    for (const f of fs.readdirSync(st.work).filter((f) => f.endsWith(".log"))) {
      fs.copyFileSync(path.join(st.work, f), path.join(out, f));
    }
  }
};
