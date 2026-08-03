/**
 * Reverse lookup: find tram lines 21-25 (pub or planning 2021-2025)
 * and report which UserStopCodes they use.
 * node scripts/find-lines-21-25-stops.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 90) * 1000;
const TARGET_PUB = new Set(["21", "23", "24", "25"]);
const TARGET_PLAN = new Set(["2021", "2023", "2024", "2025"]);

function decode(buf) {
  if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) {
    return zlib.gunzipSync(buf).toString("utf8");
  }
  return buf.toString("utf8");
}

function parseCtx(text) {
  const lines = String(text || "").split(/\r?\n/);
  let columns = null;
  const rows = [];
  for (const line of lines) {
    if (line.startsWith("\\L")) {
      columns = line.slice(2).split("|");
      continue;
    }
    if (!columns || !line.trim() || line.startsWith("\\")) continue;
    const values = line.split("|");
    const row = {};
    for (let i = 0; i < columns.length; i += 1) {
      const v = values[i];
      row[columns[i]] = v === "\\0" ? "" : (v ?? "");
    }
    rows.push(row);
  }
  return rows;
}

(async () => {
  const sub = new zmq.Subscriber();
  sub.subscribe("/GOVI/KV8passtimes/RET");
  sub.connect("tcp://pubsub.besteffort.ndovloket.nl:7817");

  const byKey = new Map();
  let msgs = 0;
  let hits = 0;
  const started = Date.now();
  console.log(
    `Scanning GOVI for public 21/23/24/25 or planning 2021/2023/2024/2025 (${DURATION_MS / 1000}s)...`,
  );

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    msgs += 1;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    // cheap prefilter
    if (!/\|21\||\|23\||\|24\||\|25\||2021|2023|2024|2025/.test(text)) {
      continue;
    }

    for (const row of parseCtx(text)) {
      const pub = String(row.LinePublicNumber || "").trim();
      const plan = String(row.LinePlanningNumber || "").trim();
      if (!TARGET_PUB.has(pub) && !TARGET_PLAN.has(plan)) continue;

      hits += 1;
      const stop = String(row.UserStopCode || "").toUpperCase() || "?";
      const key = [stop, plan, pub, row.TransportType, row.LineDirection].join("|");
      const cur = byKey.get(key) || {
        stop,
        plan,
        pub,
        type: row.TransportType,
        dir: row.LineDirection,
        quay: row.QuayCode,
        timing: row.TimingPointCode,
        n: 0,
        dests: new Set(),
      };
      cur.n += 1;
      if (row.DestinationName) cur.dests.add(row.DestinationName);
      byKey.set(key, cur);
    }

    if (msgs % 50 === 0) {
      process.stdout.write(
        `\rmsgs=${msgs} hits=${hits} combos=${byKey.size}   `,
      );
    }
  }

  console.log(`\n\nDone. msgs=${msgs} matchingRows=${hits} combos=${byKey.size}`);

  if (byKey.size === 0) {
    console.log("\nNo rows with LinePublicNumber 21/23/24/25 or planning 2021-2025.");
    process.exit(0);
  }

  const byStop = new Map();
  for (const r of byKey.values()) {
    const info = byStop.get(r.stop) || { pubs: new Set(), plans: new Set(), n: 0 };
    info.pubs.add(r.pub);
    info.plans.add(r.plan);
    info.n += r.n;
    byStop.set(r.stop, info);
  }

  console.log("\n=== Stops that carry lines 21-25 ===");
  for (const [stop, info] of [...byStop.entries()].sort(
    (a, b) => b[1].n - a[1].n,
  )) {
    console.log(
      `${stop}\tn=${info.n}\tpubs=${[...info.pubs].sort().join(",")}\tplans=${[...info.plans].sort().join(",")}`,
    );
  }

  console.log("\n=== Detail ===");
  for (const r of [...byKey.values()].sort(
    (a, b) =>
      a.pub.localeCompare(b.pub, undefined, { numeric: true }) ||
      a.stop.localeCompare(b.stop),
  )) {
    console.log(
      `pub=${r.pub}\tplan=${r.plan}\tstop=${r.stop}\ttype=${r.type}\tdir=${r.dir}\tquay=${r.quay}\ttiming=${r.timing}\tn=${r.n}\t${[...r.dests].slice(0, 3).join(" | ")}`,
    );
  }

  const interesting = ["HA1141", "HA1142", "HA1161", "HA1162"];
  console.log("\n=== Our Beurs candidates present? ===");
  for (const stop of interesting) {
    console.log(`${stop}: ${byStop.has(stop) ? "YES" : "no"}`);
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
