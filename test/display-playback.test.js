const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

// Loads the /content display script (public/content/transit/v4/app.js) against
// fake DOM, media and WebSocket objects, so playback races can be driven step by step.
const APP_PATH = path.join(__dirname, "..", "public", "content", "transit", "v4", "app.js");

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

function makeElement(id) {
  return {
    id,
    className: "",
    hidden: false,
    textContent: "",
    src: "",
    readyState: 0,
    currentTime: 0,
    muted: false,
    loop: false,
    playsInline: false,
    onended: null,
    plays: [],
    listeners: [],
    attributes: {},
    classList: {
      classes: new Set(),
      add(c) { this.classes.add(c); },
      remove(c) { this.classes.delete(c); },
      contains(c) { return this.classes.has(c); },
    },
    addEventListener(type, fn, opts = {}) {
      this.listeners.push({ type, fn, once: Boolean(opts.once) });
    },
    removeEventListener(type, fn) {
      this.listeners = this.listeners.filter((l) => !(l.type === type && l.fn === fn));
    },
    fire(type) {
      const current = this.listeners.filter((l) => l.type === type);
      this.listeners = this.listeners.filter((l) => !(l.type === type && l.once));
      for (const l of current) l.fn({ type });
    },
    play() {
      return new Promise((resolve, reject) => {
        this.plays.push({ resolve, reject });
      });
    },
    pause() {},
    load() {},
    removeAttribute(name) { delete this.attributes[name]; },
    setAttribute(name, value) { this.attributes[name] = String(value); },
  };
}

function loadDisplay() {
  const elements = {
    videoA: makeElement("videoA"),
    videoB: makeElement("videoB"),
    connectionDot: makeElement("connectionDot"),
    countdown: makeElement("countdown"),
  };
  const sockets = [];

  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.readyState = 0;
      this.listeners = [];
      sockets.push(this);
    }
    addEventListener(type, fn) { this.listeners.push({ type, fn }); }
    close() { this.readyState = 3; }
    fire(type, data) {
      for (const l of this.listeners) if (l.type === type) l.fn({ type, data });
    }
    open() {
      this.readyState = 1;
      this.fire("open");
    }
    send(command) {
      this.fire("message", JSON.stringify({ type: "transit-command", command }));
    }
  }
  FakeWebSocket.OPEN = 1;
  FakeWebSocket.CONNECTING = 0;

  const context = vm.createContext({
    document: { getElementById: (id) => elements[id] || null },
    HTMLMediaElement: { HAVE_CURRENT_DATA: 2 },
    WebSocket: FakeWebSocket,
    location: { protocol: "http:", host: "localhost:8080" },
    console: { log() {}, error() {} },
    // Timers are stubbed: tests advance state explicitly, never by waiting.
    setTimeout: () => 0,
    clearTimeout() {},
    setInterval: () => 0,
    clearInterval() {},
  });
  vm.runInContext(fs.readFileSync(APP_PATH, "utf8"), context, { filename: APP_PATH });

  return { context, elements, sockets };
}

test("a superseded arriving clip cannot start a countdown over a later departure", async () => {
  const { context, elements, sockets } = loadDisplay();
  const socket = sockets[0];
  socket.open();

  // Startup plays the idle clip on videoB (the inactive element).
  elements.videoB.readyState = 2;
  elements.videoB.fire("loadeddata");
  await flush();
  elements.videoB.plays[0].resolve();
  await flush();

  // Arriving starts loading on videoA, then a departure interrupts it before
  // videoA has any frame. The departure reuses videoA as its inactive element.
  socket.send("RET_TRAIN_ARRIVING_15S");
  await flush();
  socket.send("RET_TRAIN_DEPARTED");
  await flush();

  // Both loads complete on videoA. Only the departure may still play.
  elements.videoA.readyState = 2;
  elements.videoA.fire("loadeddata");
  await flush();
  assert.equal(elements.videoA.plays.length, 1, "superseded arriving must not call play()");
  elements.videoA.plays[0].resolve(); // departure
  await flush();

  assert.equal(vm.runInContext("currentCommand", context), "RET_TRAIN_DEPARTED");
  assert.equal(elements.countdown.hidden, true, "countdown must not show during departure");
});

test("normal arriving shows the countdown, then arrived hides it", async () => {
  const { context, elements, sockets } = loadDisplay();
  const socket = sockets[0];
  socket.open();

  elements.videoB.readyState = 2;
  elements.videoB.fire("loadeddata");
  await flush();
  elements.videoB.plays[0].resolve();
  await flush();

  socket.send("RET_TRAIN_ARRIVING_15S");
  await flush();
  elements.videoA.readyState = 2;
  elements.videoA.fire("loadeddata");
  await flush();
  elements.videoA.plays[0].resolve();
  await flush();

  assert.equal(vm.runInContext("currentCommand", context), "RET_TRAIN_ARRIVING_15S");
  assert.equal(elements.countdown.hidden, false);
  assert.equal(elements.countdown.textContent, "10");

  socket.send("RET_TRAIN_ARRIVED");
  await flush();
  elements.videoB.readyState = 2;
  elements.videoB.fire("loadeddata");
  await flush();
  elements.videoB.plays[1].resolve();
  await flush();

  assert.equal(vm.runInContext("currentCommand", context), "RET_TRAIN_ARRIVED");
  assert.equal(elements.countdown.hidden, true);
});
