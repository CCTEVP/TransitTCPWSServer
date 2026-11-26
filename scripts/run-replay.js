/**
 * Replay one capture into two server images side by side and compare what the
 * display would receive. Needs Docker; nothing connects to the live feed.
 *
 *   node scripts/run-replay.js <capture.jsonl> <imageA> <imageB> [A:KEY=VALUE ...] [B:KEY=VALUE ...]
 *   e.g. node scripts/run-replay.js captures/feed-15min.jsonl transit-tcp-ws-server:main transit-tcp-ws-server:replay
 *
 * The publisher runs in imageB with scripts/replay-feed.js mounted, so any image works.
 * Server settings come from .env (parsed with dotenv, so quoted values work).
 * A:/B: arguments override one setting for that side only, e.g. compare a config
 * change on the same image: ... img img B:ZMQ_TURBO_ON_DEMAND=false
 * Outputs captures/<capture>.<A|B>.jsonl and .log next to the capture.
 */
const fs = require("fs");
const path = require("path");
const { spawn, spawnSync } = require("child_process");
const dotenv = require("dotenv");

const [captureArg, imageA, imageB, ...overrideArgs] = process.argv.slice(2);
const overrides = { A: {}, B: {} };
for (const arg of overrideArgs) {
  const match = /^(A|B):([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(arg);
  if (!match) {
    console.error(
      `bad override "${arg}" (expected A:KEY=VALUE or B:KEY=VALUE)`,
    );
    process.exit(1);
  }
  overrides[match[1]][match[2]] = match[3];
}
if (!captureArg || !imageA || !imageB) {
  console.error("usage: run-replay.js <capture.jsonl> <imageA> <imageB>");
  process.exit(1);
}

const root = path.resolve(__dirname, "..");
const capture = path.resolve(captureArg);
const captureDir = path.dirname(capture);
const captureName = path.basename(capture);
const base = capture.replace(/\.jsonl$/, "");
const NETWORK = "transit-replay";
const TAIL_SECONDS = 90;

const lines = fs
  .readFileSync(capture, "utf8")
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line));
const header = lines.find((line) => line.type === "header");
const frames = lines.filter((line) => line.type !== "header");
const spanSeconds = Math.ceil(
  (frames[frames.length - 1].t - header.startedAt) / 1000,
);

const env = dotenv.parse(fs.readFileSync(path.join(root, ".env")));
delete env.PORT;
env.ZMQ_ENDPOINT = "tcp://transit-replay-feed:7658";
env.ZMQ_TURBO_ENDPOINT = "tcp://transit-replay-feed:7817";

function docker(args, { quiet = false } = {}) {
  const result = spawnSync("docker", args, { encoding: "utf8" });
  if (result.status !== 0 && !quiet) {
    throw new Error(
      `docker ${args.slice(0, 3).join(" ")} failed: ${result.stderr.trim()}`,
    );
  }
  return result.stdout.trim();
}

const servers = [
  { label: "A", image: imageA, name: "transit-replay-a", port: 8081 },
  { label: "B", image: imageB, name: "transit-replay-b", port: 8082 },
];
const allContainers = [
  "transit-replay-feed",
  ...servers.map((server) => server.name),
];

function cleanup() {
  for (const name of allContainers) docker(["rm", "-f", name], { quiet: true });
}

async function main() {
  cleanup();
  docker(["network", "create", NETWORK], { quiet: true });

  const startAt = Date.now() + 30000;
  const offset = header.startedAt - startAt;
  console.log(
    `[run] ${frames.length} frames over ${spanSeconds}s; replay starts in 30s`,
  );

  for (const server of servers) {
    const serverEnv = {
      ...env,
      ...overrides[server.label],
      CLOCK_OFFSET_MS: String(offset),
    };
    const envArgs = Object.entries(serverEnv).flatMap(([key, value]) => [
      "-e",
      `${key}=${value}`,
    ]);
    docker([
      "run",
      "-d",
      "--name",
      server.name,
      "--network",
      NETWORK,
      "-p",
      `${server.port}:8282`,
      "-v",
      `${path.join(root, "scripts", "clock-shift.js")}:/replay/clock-shift.js:ro`,
      ...envArgs,
      "--entrypoint",
      "node",
      server.image,
      "--require",
      "/replay/clock-shift.js",
      "src/server.js",
    ]);
  }

  docker([
    "run",
    "-d",
    "--name",
    "transit-replay-feed",
    "--network",
    NETWORK,
    "-v",
    `${captureDir}:/captures:ro`,
    // Mount the publisher so any image with zeromq installed can run it.
    "-v",
    `${path.join(root, "scripts", "replay-feed.js")}:/replay/replay-feed.js:ro`,
    "-e",
    "NODE_PATH=/app/node_modules",
    "--entrypoint",
    "node",
    imageB,
    "/replay/replay-feed.js",
    `/captures/${captureName}`,
    String(startAt),
    "7658",
    "7817",
    String(TAIL_SECONDS),
  ]);

  const recordSeconds =
    Math.ceil((startAt - Date.now()) / 1000) + spanSeconds + TAIL_SECONDS;
  console.log(`[run] recording both servers for ${recordSeconds}s`);
  const recorders = servers.map(
    (server) =>
      new Promise((resolve) => {
        const child = spawn(
          process.execPath,
          [
            path.join(__dirname, "record-commands.js"),
            `ws://localhost:${server.port}`,
            String(recordSeconds),
            `${base}.${server.label}.jsonl`,
          ],
          { stdio: "inherit" },
        );
        child.on("exit", resolve);
      }),
  );
  await Promise.all(recorders);

  for (const server of servers) {
    fs.writeFileSync(
      `${base}.${server.label}.log`,
      docker(["logs", server.name], { quiet: true }),
    );
  }
  fs.writeFileSync(
    `${base}.feed.log`,
    docker(["logs", "transit-replay-feed"], { quiet: true }),
  );
  cleanup();

  const compare = spawnSync(
    process.execPath,
    [
      path.join(__dirname, "compare-replays.js"),
      `${base}.A.jsonl`,
      `${base}.B.jsonl`,
    ],
    { stdio: "inherit" },
  );
  process.exit(compare.status ?? 1);
}

main().catch((error) => {
  console.error("[run] failed:", error.message);
  cleanup();
  process.exit(1);
});
