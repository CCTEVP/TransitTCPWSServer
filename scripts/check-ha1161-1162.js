/**
 * Validate HA1161/HA1162 Beurs platform claims vs live GOVI.
 * Claimed: HA1161 northbound Centraal lines 23/25; HA1162 southbound Feyenoord/Barendrecht 23/25.
 * node scripts/check-ha1161-1162.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 70) * 1000;
const STOPS = new Set(["HA1161", "HA1162"]);

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

  const byStop = new Map();
  const started = Date.now();
  console.log(`Sampling HA1161 + HA1162 for ${DURATION_MS / 1000}s...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    if (!/HA1161|HA1162/i.test(text)) continue;

    for (const row of parseCtx(text)) {
      const stop = String(row.UserStopCode || "").toUpperCase();
      if (!STOPS.has(stop)) continue;

      const plan = String(row.LinePlanningNumber || "").trim();
      const pub = String(row.LinePublicNumber || "").trim();
      const key = [stop, plan, pub, row.LineDirection, row.TransportType].join("|");
      const cur = byStop.get(key) || {
        stop,
        plan,
        pub,
        dir: row.LineDirection,
        transport: row.TransportType,
        side: row.SideCode,
        quay: row.QuayCode,
        timing: row.TimingPointCode,
        n: 0,
        dests: new Set(),
        statuses: new Set(),
      };
      cur.n += 1;
      if (row.DestinationName) cur.dests.add(row.DestinationName);
      if (row.TripStopStatus) cur.statuses.add(row.TripStopStatus);
      byStop.set(key, cur);
    }
  }

  for (const stop of ["HA1161", "HA1162"]) {
    const rows = [...byStop.values()]
      .filter((r) => r.stop === stop)
      .sort((a, b) =>
        a.pub.localeCompare(b.pub, undefined, { numeric: true }) ||
        String(a.dir).localeCompare(String(b.dir)),
      );
    console.log(`\n=== ${stop} (${rows.length} line/dir combos) ===`);
    if (rows.length === 0) {
      console.log("(no forecasts in window)");
      continue;
    }
    for (const r of rows) {
      console.log(
        `pub=${r.pub}\tplan=${r.plan}\ttype=${r.transport}\tdir=${r.dir}\tside=${JSON.stringify(r.side)}\tquay=${r.quay}\ttiming=${r.timing}\tn=${r.n}\tdests=${[...r.dests].join(" | ")}`,
      );
    }
  }

  console.log("\n=== Claim checks ===");
  const ha1161 = [...byStop.values()].filter((r) => r.stop === "HA1161");
  const ha1162 = [...byStop.values()].filter((r) => r.stop === "HA1162");
  const pubs1161 = new Set(ha1161.map((r) => r.pub));
  const pubs1162 = new Set(ha1162.map((r) => r.pub));
  const dests1161 = new Set(ha1161.flatMap((r) => [...r.dests]));
  const dests1162 = new Set(ha1162.flatMap((r) => [...r.dests]));

  console.log(
    `HA1161 seen: ${ha1161.length > 0 ? "yes" : "no"}; pubs=[${[...pubs1161]}]; dests=[${[...dests1161].join("; ")}]`,
  );
  console.log(
    `HA1162 seen: ${ha1162.length > 0 ? "yes" : "no"}; pubs=[${[...pubs1162]}]; dests=[${[...dests1162].join("; ")}]`,
  );
  console.log(
    `Claim lines 23/25 at HA1161: ${pubs1161.has("23") || pubs1161.has("25") ? "MATCH" : "NO"}`,
  );
  console.log(
    `Claim lines 23/25 at HA1162: ${pubs1162.has("23") || pubs1162.has("25") ? "MATCH" : "NO"}`,
  );
  console.log(
    `Claim Centraal in HA1161 dests: ${[...dests1161].some((d) => /centraal/i.test(d)) ? "MATCH" : "NO"}`,
  );
  console.log(
    `Claim Feyenoord/Barendrecht in HA1162 dests: ${[...dests1162].some((d) => /feyen|baren|carnisse|keizer/i.test(d)) ? "partial/related: " + [...dests1162].join(", ") : "NO — saw: " + [...dests1162].join(", ")}`,
  );

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
