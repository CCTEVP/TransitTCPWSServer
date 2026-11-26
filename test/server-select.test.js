const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

// The display picks its server from ?server=...; default is the page's own origin,
// so the same files work when served locally and from Cloud Run.
const APP_PATH = path.join(__dirname, "..", "public", "content", "transit", "v4", "app.js");

function resolveFor(href) {
  const source = fs.readFileSync(APP_PATH, "utf8");
  const start = source.indexOf("const SERVER_PRESETS");
  const end = source.indexOf("const WS_URL");
  assert.ok(start >= 0 && end > start, "server selector not found in app.js");

  const url = new URL(href);
  const context = vm.createContext({
    URLSearchParams,
    location: {
      protocol: url.protocol,
      host: url.host,
      hostname: url.hostname,
      search: url.search,
    },
  });
  vm.runInContext(source.slice(start, end), context);
  return vm.runInContext("resolveServerBase()", context);
}

test("defaults to the server that served the page", () => {
  assert.equal(resolveFor("http://localhost:8282/content/"), "ws://localhost:8282");
  assert.equal(
    resolveFor("https://transit.example.run.app/content/"),
    "wss://transit.example.run.app",
  );
});

test("?server=local and ?server=remote pick the presets", () => {
  assert.equal(resolveFor("http://localhost:8282/content/?server=local"), "ws://localhost:8282");
  assert.match(
    resolveFor("http://localhost:8282/content/?server=remote"),
    /^wss:\/\/.+\.run\.app$/,
  );
});

test("?server accepts a host, an http(s) URL or a ws(s) URL", () => {
  assert.equal(resolveFor("http://localhost:8282/?server=192.168.1.20:8282"), "ws://192.168.1.20:8282");
  assert.equal(resolveFor("https://a.run.app/?server=b.run.app"), "wss://b.run.app");
  assert.equal(resolveFor("http://localhost:8282/?server=https://b.run.app/"), "wss://b.run.app");
  assert.equal(resolveFor("http://localhost:8282/?server=wss://b.run.app/content"), "wss://b.run.app");
});
