const fs = require("node:fs");
const path = require("node:path");

function state() {
  return JSON.parse(fs.readFileSync(path.join(__dirname, "..", ".state.json"), "utf8"));
}

/// Opens the sign-in link `pumasi ui` printed.
async function signIn(page) {
  await page.goto(state().url);
}

/// Submits a ticker job from the form; returns its id.
async function submitTicker(page, { name, seconds, shards = 1 }) {
  await page.goto(state().base + "/jobs/new");
  await page.locator("#job").fill(
    [
      `name = "${name}"`,
      `module = "ticker.wasm"`,
      `args = ["${seconds}"]`,
      `shards = ${shards}`,
      `deterministic = false`,
      ``,
    ].join("\n"),
  );
  await page.locator("#files").setInputFiles(state().module);
  await page.getByRole("button", { name: "Submit", exact: true }).click();
  await page.waitForURL(/\/jobs\/[0-9a-f]{16}$/);
  return page.url().split("/").pop();
}

/// The bytes of a stored (uncompressed) zip entry.
function zipEntry(buf, name) {
  // End of central directory record.
  const eocd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  if (eocd < 0) throw new Error("not a zip");
  const count = buf.readUInt16LE(eocd + 10);
  let p = buf.readUInt32LE(eocd + 16);
  for (let i = 0; i < count; i++) {
    const size = buf.readUInt32LE(p + 24);
    const nameLen = buf.readUInt16LE(p + 28);
    const extra = buf.readUInt16LE(p + 30);
    const comment = buf.readUInt16LE(p + 32);
    const local = buf.readUInt32LE(p + 42);
    const entry = buf.subarray(p + 46, p + 46 + nameLen).toString("utf8");
    if (entry === name) {
      const ln = buf.readUInt16LE(local + 26);
      const le = buf.readUInt16LE(local + 28);
      const start = local + 30 + ln + le;
      return buf.subarray(start, start + size);
    }
    p += 46 + nameLen + extra + comment;
  }
  return null;
}

module.exports = { state, signIn, submitTicker, zipEntry };
