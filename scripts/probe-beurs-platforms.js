/**
 * Identify Beurs tram direction pair + metro platform stop codes.
 * node scripts/probe-beurs-platforms.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 75) * 1000;
const FOCUS = new Set([
  "HA1162",
  "HA1161",
  "HA1160",
  "HA1163",
  "HA8004",
  "HA8005",
  "HA8035",
  "HA8036",
  "HA8112",
  "HA8121",
  "HA8138",
  "HA8141",
  "HA8149",
  "HA8155",
]);

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

  const tramNear = new Map();
  const metro = new Map();
  const started = Date.now();
  console.log(`Probing Beurs platforms for ${DURATION_MS / 1000}s...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    if (!/HA11|HA80|HA81/i.test(text)) continue;

    for (const row of parseCtx(text)) {
      const stop = String(row.UserStopCode || "").toUpperCase();
      const transport = String(row.TransportType || "");
      const dest = String(row.DestinationName || "");
      const pub = String(row.LinePublicNumber || "");
      const plan = String(row.LinePlanningNumber || "");
      const dir = String(row.LineDirection || "");

      if (transport === "Tram" && /^HA116/.test(stop)) {
        const key = [stop, plan, pub, dir].join("|");
        const cur = tramNear.get(key) || {
          stop,
          plan,
          pub,
          dir,
          n: 0,
          quay: row.QuayCode,
          dests: new Set(),
        };
        cur.n += 1;
        if (dest) cur.dests.add(dest);
        tramNear.set(key, cur);
      }

      if (transport !== "Metro") continue;
      if (!FOCUS.has(stop) && !/^HA80|^HA81/.test(stop)) continue;

      const key = [stop, pub, dir, plan].join("|");
      const cur = metro.get(key) || {
        stop,
        pub,
        dir,
        plan,
        n: 0,
        sides: new Set(),
        quays: new Set(),
        dests: new Set(),
      };
      cur.n += 1;
      if (dest) cur.dests.add(dest);
      if (row.SideCode) cur.sides.add(row.SideCode);
      if (row.QuayCode) cur.quays.add(row.QuayCode);
      metro.set(key, cur);
    }
  }

  console.log("\n=== Tram HA116x ===");
  for (const r of [...tramNear.values()].sort(
    (a, b) => a.stop.localeCompare(b.stop) || a.pub.localeCompare(b.pub),
  )) {
    console.log(
      `${r.stop}\tpub=${r.pub}\tplan=${r.plan}\tdir=${r.dir}\tquay=${r.quay}\t${[...r.dests].join(" | ")}`,
    );
  }

  const byStop = new Map();
  for (const r of metro.values()) {
    const info = byStop.get(r.stop) || { pubs: new Set(), dirs: new Set(), rows: [] };
    info.pubs.add(r.pub);
    info.dirs.add(r.dir);
    info.rows.push(r);
    byStop.set(r.stop, info);
  }

  console.log("\n=== Metro stops with 2+ of A–E (or focus codes) ===");
  for (const [stop, info] of [...byStop.entries()].sort()) {
    const letters = [...info.pubs].filter((p) => /^[A-E]$/.test(p)).sort().join("");
    if (!FOCUS.has(stop) && letters.length < 2) continue;
    console.log(`\n${stop} letters=${letters || "-"} dirs=${[...info.dirs].join(",")}`);
    for (const r of info.rows.sort(
      (a, b) => a.pub.localeCompare(b.pub) || a.dir.localeCompare(b.dir),
    )) {
      console.log(
        `  ${r.pub} dir${r.dir} plan=${r.plan} side=${[...r.sides].join(",") || "-"} quay=${[...r.quays].join(",") || "-"} -> ${[...r.dests].join(" | ")}`,
      );
    }
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
