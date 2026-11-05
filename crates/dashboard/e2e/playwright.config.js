// Runs against a real cluster: global-setup.js starts a control plane,
// two WASM agents and `pumasi ui` from PUMASI_BIN (default: the debug
// build), and global-teardown.js stops them.
const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: "./tests",
  globalSetup: require.resolve("./global-setup.js"),
  globalTeardown: require.resolve("./global-teardown.js"),
  // One cluster, shared state: run the tests in order.
  workers: 1,
  fullyParallel: false,
  timeout: 180_000,
  expect: { timeout: 30_000 },
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  use: {
    headless: true,
    locale: "en-US",
    acceptDownloads: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
});
