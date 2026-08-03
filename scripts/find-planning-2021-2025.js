/**
 * Find LinePlanningNumber 2021-2025 in GOVI (+ optional RIG) and list stops.
 * node scripts/find-planning-2021-2025.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 120) * 1000;
const TARGET = new Set(["2021", "2023", "2024", "2025"]);

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

function collectFromText(text, feed, byKey, stats) {
  // Fast reject unless any target substring appears
  if (!/2021|2023|2024|2025/.test(text)) return;

  // CTX path
  for (const row of parseCtx(text)) {
    const plan = String(row.LinePlanningNumber || "").trim();
    if (!TARGET.has(plan)) continue;
    stats.hits += 1;
    const stop = String(row.UserStopCode || "").toUpperCase() || "?";
    const pub = String(row.LinePublicNumber || "").trim();
    const key = [feed, stop, plan, pub, row.TransportType].join("|");
    const cur = byKey.get(key) || {
      feed,
      stop,
      plan,
      pub,
      type: row.TransportType || "",
      dir: row.LineDirection || "",
      quay: row.QuayCode || "",
      n: 0,
      dests: new Set(),
    };
    cur.n += 1;
    if (row.DestinationName) cur.dests.add(row.DestinationName);
    byKey.set(key, cur);
  }

  // Also catch raw XML mentions (RIG sometimes)
  if (text.trimStart().startsWith("<") && /LinePlanningNumber>\s*202[1-5]\s*</i.test(text)) {
    stats.xmlMentions += 1;
  }
}

(async () => {
  const byKey = new Map();
  const stats = { goviMsgs: 0, rigMsgs: 0, hits: 0, xmlMentions: 0 };
  const started = Date.now();

  const govi = new zmq.Subscriber();
  govi.subscribe("/GOVI/KV8passtimes/RET");
  govi.connect("tcp://pubsub.besteffort.ndovloket.nl:7817");

  const rig = new zmq.Subscriber();
  rig.subscribe("/RIG/KV17cvlinfo");
  rig.subscribe("/RIG/KV6posinfo");
  rig.connect("tcp://pubsub.besteffort.ndovloket.nl:7658");

  console.log(
    `Searching GOVI+RIG for LinePlanningNumber 2021/2023/2024/2025 (${DURATION_MS / 1000}s)...`,
  );

  const goviLoop = (async () => {
    for await (const msg of govi) {
      if (Date.now() - started > DURATION_MS) break;
      stats.goviMsgs += 1;
      const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
      collectFromText(decode(buf), "GOVI", byKey, stats);
    }
  })();

  const rigLoop = (async () => {
    for await (const msg of rig) {
      if (Date.now() - started > DURATION_MS) break;
      stats.rigMsgs += 1;
      const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
      collectFromText(decode(buf), "RIG", byKey, stats);
    }
  })();

  await Promise.race([
    Promise.all([goviLoop, rigLoop]),
    new Promise((r) => setTimeout(r, DURATION_MS + 2000)),
  ]);

  console.log(
    `\nDone. goviMsgs=${stats.goviMsgs} rigMsgs=${stats.rigMsgs} planningHits=${stats.hits} xmlMentions=${stats.xmlMentions}`,
  );

  if (byKey.size === 0) {
    console.log(
      "\nNo LinePlanningNumber 2021/2023/2024/2025 found in this window.",
    );
    console.log(
      "Those codes are not currently present in the live RET GOVI/RIG streams.",
    );
    process.exit(0);
  }

  console.log("\n=== Stops for 2021-2025 ===");
  const byStop = new Map();
  for (const r of byKey.values()) {
    const info =
      byStop.get(r.stop) || { plans: new Set(), pubs: new Set(), feeds: new Set(), n: 0 };
    info.plans.add(r.plan);
    info.pubs.add(r.pub || "-");
    info.feeds.add(r.feed);
    info.n += r.n;
    byStop.set(r.stop, info);
  }
  for (const [stop, info] of [...byStop.entries()].sort((a, b) => b[1].n - a[1].n)) {
    console.log(
      `${stop}\tn=${info.n}\tplans=${[...info.plans].join(",")}\tpubs=${[...info.pubs].join(",")}\tfeeds=${[...info.feeds].join(",")}`,
    );
  }

  console.log("\n=== Detail ===");
  for (const r of [...byKey.values()].sort(
    (a, b) => a.plan.localeCompare(b.plan) || a.stop.localeCompare(b.stop),
  )) {
    console.log(
      `${r.feed}\tplan=${r.plan}\tpub=${r.pub || "-"}\tstop=${r.stop}\ttype=${r.type}\tdir=${r.dir}\tn=${r.n}\t${[...r.dests].slice(0, 2).join(" | ")}`,
    );
  }

  console.log("\n=== Beurs candidates ===");
  for (const stop of ["HA1141", "HA1142", "HA1161", "HA1162"]) {
    console.log(`${stop}: ${byStop.has(stop) ? "YES" : "no"}`);
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
