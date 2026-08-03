/**
 * Find Beurs-area stops: metro vs tram, directions, quays.
 * node scripts/probe-beurs-stops.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 60) * 1000;
const INTERESTING = /beurs|1162|8004|8138|timingpointcode.?31001/i;

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

  // stop|line|dir|transport|public -> stats
  const stops = new Map();
  const started = Date.now();
  console.log(`Scanning Beurs-related stops for ${DURATION_MS / 1000}s...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    // cheap filter
    if (!/HA11|HA80|Beurs|31001/i.test(text)) continue;

    for (const row of parseCtx(text)) {
      const stop = String(row.UserStopCode || "").toUpperCase();
      const dest = String(row.DestinationName || "");
      const quay = String(row.QuayCode || "");
      const timing = String(row.TimingPointCode || "");
      const transport = String(row.TransportType || "");
      if (
        !stop.startsWith("HA") &&
        !/beurs/i.test(dest) &&
        !timing.startsWith("31001")
      ) {
        continue;
      }
      // Focus: known tram HA1162, metro HA8004-ish, any Metro at timing 3100*
      const isFocus =
        stop === "HA1162" ||
        stop === "HA8004" ||
        /^HA80/.test(stop) ||
        /^HA81/.test(stop) ||
        transport === "Metro" ||
        /beurs/i.test(quay);
      if (!isFocus && !/^HA11/.test(stop)) continue;
      if (!isFocus && !["Metro", "Tram"].includes(transport)) continue;

      const plan = String(row.LinePlanningNumber || "").trim();
      const pub = String(row.LinePublicNumber || "").trim();
      const dir = String(row.LineDirection || "");
      const key = [stop, plan, pub, dir, transport].join("|");
      const cur = stops.get(key) || {
        stop,
        plan,
        pub,
        dir,
        transport,
        count: 0,
        dests: new Set(),
        sides: new Set(),
        quays: new Set(),
        timingPts: new Set(),
        vehicles: new Set(),
      };
      cur.count += 1;
      if (dest) cur.dests.add(dest);
      if (row.SideCode) cur.sides.add(row.SideCode);
      if (quay) cur.quays.add(quay);
      if (timing) cur.timingPts.add(timing);
      if (row.VehicleNumber) cur.vehicles.add(row.VehicleNumber);
      stops.set(key, cur);
    }
  }

  const rows = [...stops.values()].sort((a, b) =>
    a.stop.localeCompare(b.stop) ||
    a.transport.localeCompare(b.transport) ||
    a.plan.localeCompare(b.plan, undefined, { numeric: true }),
  );

  console.log("\nstop\ttype\tline\tpub\tdir\tcount\tside\tquay\ttiming\tdests");
  for (const r of rows) {
    // Prefer metro + HA1162 + HA80*
    if (
      r.transport !== "Metro" &&
      r.stop !== "HA1162" &&
      !/^HA80/.test(r.stop) &&
      !/^HA81/.test(r.stop)
    ) {
      continue;
    }
    console.log(
      [
        r.stop,
        r.transport,
        r.plan,
        r.pub,
        r.dir,
        r.count,
        [...r.sides].join(",") || "-",
        [...r.quays].join(",") || "-",
        [...r.timingPts].join(",") || "-",
        [...r.dests].slice(0, 3).join(" | "),
      ].join("\t"),
    );
  }

  // Summary: unique stops with Metro
  console.log("\n=== Metro stops seen ===");
  const metroStops = new Map();
  for (const r of rows) {
    if (r.transport !== "Metro") continue;
    const cur = metroStops.get(r.stop) || { lines: new Set(), dirs: new Set(), quays: new Set(), pubs: new Set() };
    cur.lines.add(r.plan);
    cur.dirs.add(r.dir);
    cur.pubs.add(r.pub);
    for (const q of r.quays) cur.quays.add(q);
    metroStops.set(r.stop, cur);
  }
  for (const [stop, info] of [...metroStops.entries()].sort()) {
    console.log(
      `${stop}\tpublines=${[...info.pubs].sort().join(",")}\tplanning=${[...info.lines].sort().join(",")}\tdirs=${[...info.dirs].join(",")}\tquays=${[...info.quays].join(",")}`,
    );
  }

  console.log("\n=== HA1162 summary ===");
  for (const r of rows.filter((x) => x.stop === "HA1162")) {
    console.log(
      `tram line pub=${r.pub} plan=${r.plan} dir=${r.dir} quay=${[...r.quays].join(",")} dests=${[...r.dests].join(" | ")}`,
    );
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
