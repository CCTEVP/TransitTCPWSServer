const { test } = require("node:test");
const assert = require("node:assert/strict");
const zlib = require("node:zlib");

// Pin the timezone before loading the server so the result does not depend on .env.
process.env.TRANSIT_TIMEZONE = "Europe/Amsterdam";

// server.js creates zeromq sockets at module load. The helpers under test are pure,
// so replace the native zeromq module with a stub to avoid opening real sockets.
const zmqPath = require.resolve("zeromq");
require.cache[zmqPath] = {
  id: zmqPath,
  filename: zmqPath,
  loaded: true,
  exports: { Subscriber: class {} },
};

const {
  secondsSince,
  normalizeWsChannel,
  getVehicleNumberFromMessage,
  safeUtf8,
  toArray,
  detectAndDecodePayload,
  getRowValue,
  parseDayParts,
  zonedWallTimeToUtcDate,
  toDateOnDay,
  extractEtaSeconds,
  extractExpectedArrivalMs,
  containsNoTrainSignal,
  isAllowedRetCommand,
  compareUpcoming,
} = require("../src/server.js");

test("compareUpcoming orders by ETA, then vehicle number, with a stable tie", () => {
  const entries = [
    { vehiclenumber: "300", etaSeconds: 120 },
    { vehiclenumber: "200", etaSeconds: 60 },
    { vehiclenumber: "100", etaSeconds: 120 },
    { vehiclenumber: "150", etaSeconds: 120 },
  ];
  const sorted = [...entries].sort(compareUpcoming).map((e) => e.vehiclenumber);
  assert.deepEqual(sorted, ["200", "100", "150", "300"]);
});

test("secondsSince returns null for empty input and elapsed seconds otherwise", () => {
  assert.equal(secondsSince(null), null);
  assert.equal(secondsSince(""), null);
  const tenSecondsAgo = new Date(Date.now() - 10_000).toISOString();
  const elapsed = secondsSince(tenSecondsAgo);
  assert.ok(elapsed >= 9 && elapsed <= 11, `expected ~10, got ${elapsed}`);
});

test("normalizeWsChannel maps paths to channels", () => {
  assert.equal(normalizeWsChannel("/control"), "control");
  assert.equal(normalizeWsChannel("/CONTROL?token=1"), "control");
  assert.equal(normalizeWsChannel("/content"), "content");
  assert.equal(normalizeWsChannel("/content/"), "content");
  assert.equal(normalizeWsChannel("/"), "content");
  assert.equal(normalizeWsChannel("/?x=1"), "content");
  assert.equal(normalizeWsChannel("/something-else"), "unknown");
  assert.equal(normalizeWsChannel(undefined), "content");
});

test("getVehicleNumberFromMessage reads the vehicle number from known locations", () => {
  assert.equal(getVehicleNumberFromMessage({ vehiclenumber: 2142 }), "2142");
  assert.equal(getVehicleNumberFromMessage({ entity: { vehiclenumber: "7" } }), "7");
  assert.equal(getVehicleNumberFromMessage({ data: { vehiclenumber: "9" } }), "9");
  assert.equal(
    getVehicleNumberFromMessage({ payload: { entity: { vehiclenumber: "11" } } }),
    "11",
  );
  assert.equal(getVehicleNumberFromMessage({}), "");
  assert.equal(getVehicleNumberFromMessage(null), "");
});

test("toArray wraps scalars and keeps arrays", () => {
  assert.deepEqual(toArray(undefined), []);
  assert.deepEqual(toArray(null), []);
  assert.deepEqual(toArray("a"), ["a"]);
  assert.deepEqual(toArray([1, 2]), [1, 2]);
});

test("safeUtf8 decodes buffers and returns null for non-buffers", () => {
  assert.equal(safeUtf8(Buffer.from("hé", "utf8")), "hé");
  assert.equal(safeUtf8(null), null);
});

test("detectAndDecodePayload handles gzip and raw payloads", () => {
  const xml = "<KV6posinfo/>";

  const gzipped = detectAndDecodePayload(zlib.gzipSync(Buffer.from(xml)));
  assert.equal(gzipped.encoding, "gzip+xml");
  assert.equal(gzipped.text, xml);

  const raw = detectAndDecodePayload(Buffer.from(xml));
  assert.equal(raw.encoding, "raw");
  assert.equal(raw.text, xml);
});

test("getRowValue matches keys case-insensitively and skips empty values", () => {
  assert.equal(getRowValue({ VehicleNumber: "5" }, "vehiclenumber"), "5");
  assert.equal(getRowValue({ a: "", b: "x" }, ["a", "b"]), "x");
  assert.equal(getRowValue({ a: "" }, "a"), null);
  assert.equal(getRowValue({ a: 0 }, "a"), 0);
  assert.equal(getRowValue(null, "a"), null);
  assert.equal(getRowValue({ a: 1 }, "missing"), null);
});

test("parseDayParts reads the calendar day from a date string", () => {
  assert.deepEqual(parseDayParts("2026-07-15", null), { year: 2026, month: 7, day: 15 });
});

test("zonedWallTimeToUtcDate converts Amsterdam wall time to UTC (CEST and CET)", () => {
  // Summer: UTC+2
  assert.equal(
    zonedWallTimeToUtcDate(2026, 7, 15, 14, 30, 0, "Europe/Amsterdam").toISOString(),
    "2026-07-15T12:30:00.000Z",
  );
  // Winter: UTC+1
  assert.equal(
    zonedWallTimeToUtcDate(2026, 1, 15, 14, 30, 0, "Europe/Amsterdam").toISOString(),
    "2026-01-15T13:30:00.000Z",
  );
});

test("toDateOnDay handles offsets, wall times and HH:mm values", () => {
  // Explicit zone is trusted as-is.
  assert.equal(
    toDateOnDay("2026-07-15T10:15:00Z", null, null).toISOString(),
    "2026-07-15T10:15:00.000Z",
  );
  // Zoneless date-time is treated as transit-local time.
  assert.equal(
    toDateOnDay("2026-07-15 14:30", null, null).toISOString(),
    "2026-07-15T12:30:00.000Z",
  );
  // HH:mm anchored to the operating day in transit-local time.
  assert.equal(
    toDateOnDay("14:30:00", "2026-07-15", null).toISOString(),
    "2026-07-15T12:30:00.000Z",
  );
  assert.equal(toDateOnDay("not-a-time", "2026-07-15", null), null);
  assert.equal(toDateOnDay(undefined, "2026-07-15", null), null);
});

test("extractEtaSeconds prefers expected arrival, then numeric countdown fields", () => {
  const receivedAt = "2026-07-15T12:00:00.000Z";

  assert.equal(
    extractEtaSeconds({ expectedarrivaltime: "14:30:00", operatingday: "2026-07-15" }, receivedAt),
    1800,
  );
  assert.equal(extractEtaSeconds({ secondsleft: "42" }, receivedAt), 42);
  assert.equal(extractEtaSeconds({ remainingseconds: 15.4 }, receivedAt), 15);
  assert.equal(extractEtaSeconds({}, receivedAt), null);
  assert.equal(extractEtaSeconds(null, receivedAt), null);
});

test("extractExpectedArrivalMs adds the ETA to the reception time", () => {
  const receivedAt = "2026-07-15T12:00:00.000Z";
  assert.equal(
    extractExpectedArrivalMs({ secondsleft: "60" }, receivedAt),
    Date.parse(receivedAt) + 60_000,
  );
  assert.equal(extractExpectedArrivalMs({}, receivedAt), null);
});

test("containsNoTrainSignal detects cancellations and Dutch no-train text", () => {
  assert.equal(containsNoTrainSignal({}, { command: "RET_TRAIN_CANCELLED" }), true);
  assert.equal(containsNoTrainSignal({ note: "Geen trein" }, { command: "RET_X" }), true);
  assert.equal(containsNoTrainSignal({ note: "Rit vervallen" }, { command: "RET_X" }), true);
  assert.equal(containsNoTrainSignal({ status: "on time" }, { command: "RET_TRAIN_ARRIVED" }), false);
});

test("isAllowedRetCommand accepts only RET transit commands from the allow-list", () => {
  const base = { type: "transit-command", protocol: "RET" };
  assert.equal(isAllowedRetCommand({ ...base, command: "RET_TRAIN_ARRIVED" }), true);
  assert.equal(isAllowedRetCommand({ ...base, command: "RET_NO_TRAIN" }), true);
  assert.equal(isAllowedRetCommand({ ...base, command: "RET_UNKNOWN" }), false);
  assert.equal(
    isAllowedRetCommand({ type: "transit-command", protocol: "OTHER", command: "RET_TRAIN_ARRIVED" }),
    false,
  );
  assert.equal(isAllowedRetCommand(null), false);
});
