/**
 * Validate Beurs axis claims vs live GOVI (quays, timing points, planning #s).
 * node scripts/analyse-beurs-claim.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 75) * 1000;
const STOPS = new Set(["HA1141", "HA1142", "HA1161", "HA1162"]);
const CLAIMED_PLANS = new Set(["2021", "2023", "2024", "2025", "1682", "1683", "1684", "1702", "1704", "44"]);
const CLAIMED_QUAYS = [
  "30001161",
  "30001162",
  "30001141",
  "30001142",
  "31001161",
  "31001162",
  "31001141",
  "31001142",
];

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

  const atStops = new Map();
  const claimedPlanHits = new Map();
  const quayHits = new Map();
  let colsSeen = null;
  const started = Date.now();
  console.log(`Analysing Beurs claim vs live GOVI (${DURATION_MS / 1000}s)...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const text = decode(msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0));

    for (const q of CLAIMED_QUAYS) {
      if (text.includes(q)) quayHits.set(q, (quayHits.get(q) || 0) + 1);
    }

    for (const row of parseCtx(text)) {
      if (!colsSeen && row.UserStopCode !== undefined) {
        // capture once from header via first parse — use keys of row
        colsSeen = Object.keys(row);
      }

      const stop = String(row.UserStopCode || "").toUpperCase();
      const plan = String(row.LinePlanningNumber || "").trim();
      const pub = String(row.LinePublicNumber || "").trim();
      const quay = String(row.QuayCode || "");
      const timing = String(row.TimingPointCode || "");
      const type = String(row.TransportType || "");
      const dest = String(row.DestinationName || "");

      if (CLAIMED_PLANS.has(plan)) {
        const k = `${plan}|pub=${pub}|stop=${stop}|type=${type}`;
        const c = claimedPlanHits.get(k) || { plan, pub, stop, type, n: 0, dests: new Set(), quays: new Set() };
        c.n += 1;
        if (dest) c.dests.add(dest);
        if (quay) c.quays.add(quay);
        claimedPlanHits.set(k, c);
      }

      if (!STOPS.has(stop)) continue;
      const key = [stop, plan, pub, type, row.LineDirection].join("|");
      const cur = atStops.get(key) || {
        stop,
        plan,
        pub,
        type,
        dir: row.LineDirection,
        n: 0,
        quays: new Set(),
        timings: new Set(),
        dests: new Set(),
      };
      cur.n += 1;
      if (quay) cur.quays.add(quay);
      if (timing) cur.timings.add(timing);
      if (dest) cur.dests.add(dest);
      atStops.set(key, cur);
    }
  }

  console.log("\n=== CTX fields present (sample) ===");
  if (colsSeen) {
    const interesting = colsSeen.filter((c) =>
      /Stop|Quay|Timing|Area|Line|Transport|Destination|Side/i.test(c),
    );
    console.log(interesting.join(", "));
    console.log(
      "Has StopAreaCode?",
      colsSeen.some((c) => /stoparea/i.test(c)) ? "YES" : "NO",
    );
  }

  console.log("\n=== Live at HA1141/1142/1161/1162 ===");
  for (const r of [...atStops.values()].sort((a, b) =>
    a.stop.localeCompare(b.stop) || a.plan.localeCompare(b.plan, undefined, { numeric: true }),
  )) {
    console.log(
      `${r.stop}\tplan=${r.plan}\tpub=${r.pub}\ttype=${r.type}\tdir=${r.dir}\tquay=${[...r.quays].join(",")}\ttiming=${[...r.timings].join(",")}\t${[...r.dests].join(" | ")}`,
    );
  }

  console.log("\n=== Claimed planning numbers anywhere in feed ===");
  if (claimedPlanHits.size === 0) {
    console.log("none of 2021/2023/2024/2025/1682/1683/1684/1702/1704/44 seen? (or only some)");
  }
  for (const r of [...claimedPlanHits.values()].sort((a, b) =>
    a.plan.localeCompare(b.plan) || a.stop.localeCompare(b.stop),
  )) {
    console.log(
      `plan=${r.plan}\tpub=${r.pub}\tstop=${r.stop}\ttype=${r.type}\tn=${r.n}\tquay=${[...r.quays].join(",")}\t${[...r.dests].slice(0, 2).join(" | ")}`,
    );
  }

  console.log("\n=== Quay / timing digit substrings in raw messages ===");
  for (const q of CLAIMED_QUAYS) {
    console.log(`${q}: ${quayHits.get(q) || 0} msgs`);
  }

  console.log("\n=== Claim vs live verdict ===");
  const summary = {};
  for (const stop of STOPS) {
    const rows = [...atStops.values()].filter((r) => r.stop === stop);
    summary[stop] = {
      plans: [...new Set(rows.map((r) => r.plan))],
      pubs: [...new Set(rows.map((r) => r.pub))],
      quays: [...new Set(rows.flatMap((r) => [...r.quays]))],
      timings: [...new Set(rows.flatMap((r) => [...r.timings]))],
    };
  }
  console.log(JSON.stringify(summary, null, 2));

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
