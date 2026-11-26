/**
 * Record live RIG and GOVI (turbo) frames for later replay (scripts/replay-feed.js).
 *
 *   node scripts/capture-feed.js <seconds> <out.jsonl>
 *
 * Uses ZMQ_ENDPOINT/ZMQ_TOPICS and ZMQ_TURBO_ENDPOINT/ZMQ_TURBO_TOPICS from the
 * environment (same defaults as the server). Only frames that mention one of the
 * tracked stops (USERSTOPCODES + GOVI_USERSTOPCODES) are stored; the server drops
 * all other frames before parsing, so they do not affect replay results.
 *
 * Opens one connection per datastream (GOVI fair use). Keep captures short.
 */
const fs = require("fs");
const zlib = require("zlib");
const zmq = require("zeromq");

const seconds = Number(process.argv[2] || 900);
const outFile = process.argv[3] || "captures/feed.jsonl";

const list = (value, fallback) =>
  String(value || fallback)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

const feeds = [
  {
    name: "rig",
    endpoint: process.env.ZMQ_ENDPOINT || "tcp://pubsub.besteffort.ndovloket.nl:7658",
    topics: list(process.env.ZMQ_TOPICS, "/RIG/KV17cvlinfo,/RIG/KV6posinfo"),
  },
  {
    name: "turbo",
    endpoint: process.env.ZMQ_TURBO_ENDPOINT || "tcp://pubsub.besteffort.ndovloket.nl:7817",
    topics: list(process.env.ZMQ_TURBO_TOPICS, "/GOVI/KV8passtimes/RET"),
  },
];

const stops = [
  ...new Set(
    [...list(process.env.USERSTOPCODES, ""), ...list(process.env.GOVI_USERSTOPCODES, "")].map(
      (stop) => stop.toLowerCase(),
    ),
  ),
];
if (stops.length === 0) {
  console.error("Set USERSTOPCODES and/or GOVI_USERSTOPCODES so the capture can be filtered.");
  process.exit(1);
}

function decodeText(buffer) {
  try {
    const isGzip = buffer.length > 2 && buffer[0] === 0x1f && buffer[1] === 0x8b;
    return (isGzip ? zlib.gunzipSync(buffer) : buffer).toString("utf8").toLowerCase();
  } catch {
    return "";
  }
}

const out = fs.createWriteStream(outFile);
out.write(
  JSON.stringify({ type: "header", startedAt: Date.now(), seconds, stops, feeds }) + "\n",
);

const stats = Object.fromEntries(feeds.map((feed) => [feed.name, { seen: 0, kept: 0 }]));
const sockets = [];

async function record(feed) {
  const socket = new zmq.Subscriber();
  sockets.push(socket);
  socket.connect(feed.endpoint);
  for (const topic of feed.topics) socket.subscribe(topic);
  console.log(`[capture] ${feed.name}: ${feed.endpoint} ${feed.topics.join(", ")}`);

  for await (const frames of socket) {
    if (!Array.isArray(frames) || frames.length === 0) continue;
    const topic = frames[0].toString("utf8");
    const payload = Buffer.concat(frames.slice(1));
    stats[feed.name].seen += 1;

    const text = decodeText(payload);
    if (!stops.some((stop) => text.includes(stop))) continue;

    stats[feed.name].kept += 1;
    out.write(
      JSON.stringify({ t: Date.now(), feed: feed.name, topic, b64: payload.toString("base64") }) +
        "\n",
    );
  }
}

for (const feed of feeds) {
  record(feed).catch((error) => console.error(`[capture] ${feed.name} failed:`, error.message));
}

setTimeout(() => {
  for (const socket of sockets) {
    try {
      socket.close();
    } catch {
      // already closed
    }
  }
  out.end(() => {
    console.log(`[capture] done after ${seconds}s:`, JSON.stringify(stats));
    process.exit(0);
  });
}, seconds * 1000);
