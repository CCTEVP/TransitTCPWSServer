/**
 * Publish a capture from scripts/capture-feed.js over ZeroMQ, at 1x speed, so one
 * or more servers can consume it instead of the live GOVI feed.
 *
 *   node scripts/replay-feed.js <capture.jsonl> <startAtEpochMs> [rigPort] [turboPort] [tailSeconds]
 *
 * Point servers at it with ZMQ_ENDPOINT=tcp://<host>:<rigPort> and
 * ZMQ_TURBO_ENDPOINT=tcp://<host>:<turboPort>, and start them with
 * scripts/clock-shift.js and CLOCK_OFFSET_MS = header.startedAt - startAtEpochMs.
 * Servers must be connected before startAt (ZeroMQ drops frames for late joiners).
 */
const fs = require("fs");
const zmq = require("zeromq");

const [file, startAtArg, rigPort = "7658", turboPort = "7817", tailArg = "90"] =
  process.argv.slice(2);
if (!file || !startAtArg) {
  console.error("usage: replay-feed.js <capture.jsonl> <startAtEpochMs> [rigPort] [turboPort] [tailSeconds]");
  process.exit(1);
}

const lines = fs.readFileSync(file, "utf8").trim().split("\n").map((line) => JSON.parse(line));
const header = lines.find((line) => line.type === "header");
const frames = lines.filter((line) => line.type !== "header").sort((a, b) => a.t - b.t);
const startAt = Number(startAtArg);
const captureStart = header?.startedAt ?? frames[0]?.t;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));

async function main() {
  const sockets = { rig: new zmq.Publisher(), turbo: new zmq.Publisher() };
  await sockets.rig.bind(`tcp://*:${rigPort}`);
  await sockets.turbo.bind(`tcp://*:${turboPort}`);
  console.log(
    `[replay] ${frames.length} frames, capture start ${new Date(captureStart).toISOString()}, ` +
      `publishing at ${new Date(startAt).toISOString()} (rig :${rigPort}, turbo :${turboPort})`,
  );

  const sent = { rig: 0, turbo: 0 };
  for (const frame of frames) {
    await sleep(startAt + (frame.t - captureStart) - Date.now());
    await sockets[frame.feed].send([frame.topic, Buffer.from(frame.b64, "base64")]);
    sent[frame.feed] += 1;
  }

  // Let follow-up timers (arrived countdown, departure no-train) fire before exiting.
  const tailSeconds = Number(tailArg);
  console.log(`[replay] all frames sent ${JSON.stringify(sent)}; holding ${tailSeconds}s`);
  await sleep(tailSeconds * 1000);
  sockets.rig.close();
  sockets.turbo.close();
  console.log("[replay] done");
  process.exit(0);
}

main().catch((error) => {
  console.error("[replay] failed:", error);
  process.exit(1);
});
