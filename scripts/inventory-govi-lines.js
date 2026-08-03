/**
 * Inventory LinePublicNumber / LinePlanningNumber values in GOVI RET feed.
 * Optionally filter TransportType=Tram.
 * node scripts/inventory-govi-lines.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 60) * 1000;

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

  const tramPubs = new Map(); // pub -> {plans:Set, stops:Set, n}
  const allPubs = new Map();
  const planToPub = new Map();
  let rows = 0;
  const started = Date.now();
  console.log(`Inventorying GOVI RET lines for ${DURATION_MS / 1000}s...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    for (const row of parseCtx(decode(buf))) {
      rows += 1;
      const pub = String(row.LinePublicNumber || "").trim();
      const plan = String(row.LinePlanningNumber || "").trim();
      const stop = String(row.UserStopCode || "").toUpperCase();
      const type = String(row.TransportType || "");
      if (!pub && !plan) continue;

      const bump = (map, key) => {
        const cur = map.get(key) || { plans: new Set(), stops: new Set(), types: new Set(), n: 0 };
        cur.n += 1;
        if (plan) cur.plans.add(plan);
        if (stop) cur.stops.add(stop);
        if (type) cur.types.add(type);
        map.set(key, cur);
      };
      if (pub) bump(allPubs, pub);
      if (type === "Tram" && pub) bump(tramPubs, pub);
      if (plan) {
        const cur = planToPub.get(plan) || { pubs: new Set(), types: new Set(), n: 0 };
        cur.n += 1;
        if (pub) cur.pubs.add(pub);
        if (type) cur.types.add(type);
        planToPub.set(plan, cur);
      }
    }
  }

  console.log(`\nParsed rows: ${rows}`);
  console.log("\n=== Tram LinePublicNumber values (sorted) ===");
  for (const [pub, info] of [...tramPubs.entries()].sort((a, b) =>
    a[0].localeCompare(b[0], undefined, { numeric: true }),
  )) {
    const sampleStops = [...info.stops].slice(0, 8).join(",");
    console.log(
      `pub=${pub}\tn=${info.n}\tplans=${[...info.plans].join(",")}\tstops~${info.stops.size} e.g. ${sampleStops}`,
    );
  }

  console.log("\n=== Looking for 21-25 / 2021-2025 ===");
  for (const p of ["21", "23", "24", "25"]) {
    console.log(`public ${p}: ${allPubs.has(p) ? "YES n=" + allPubs.get(p).n : "no"}`);
  }
  for (const p of ["2021", "2023", "2024", "2025"]) {
    console.log(`planning ${p}: ${planToPub.has(p) ? "YES n=" + planToPub.get(p).n : "no"}`);
  }

  // nearest public numbers numerically around 20-30
  console.log("\n=== Public numbers in 15-30 range (any mode) ===");
  for (const [pub, info] of [...allPubs.entries()].sort((a, b) =>
    a[0].localeCompare(b[0], undefined, { numeric: true }),
  )) {
    const n = Number(pub);
    if (!Number.isFinite(n) || n < 15 || n > 30) continue;
    console.log(
      `pub=${pub}\tn=${info.n}\ttypes=${[...info.types].join(",")}\tplans=${[...info.plans].join(",")}`,
    );
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
