/**
 * One-off probe: what actually serves UserStopCode HA1162?
 * node scripts/probe-ha1162.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const STOP = "HA1162";
const DURATION_MS = (Number(process.argv[2]) || 90) * 1000;
const LETTER = { 1682: "A", 1683: "B", 1684: "C", 1702: "D", 1704: "E" };

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

function etaKey(t) {
  return String(t || "").trim() || "?";
}

(async () => {
  const sub = new zmq.Subscriber();
  sub.subscribe("/GOVI/KV8passtimes/RET");
  sub.connect("tcp://pubsub.besteffort.ndovloket.nl:7817");

  const byJourney = new Map();
  const fieldPresence = new Map();
  const started = Date.now();
  console.log(`Probing ${STOP} for ${DURATION_MS / 1000}s...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    if (!text.toUpperCase().includes(STOP)) continue;

    for (const row of parseCtx(text)) {
      if (String(row.UserStopCode || "").toUpperCase() !== STOP) continue;

      for (const [k, v] of Object.entries(row)) {
        if (v !== undefined && v !== null && String(v).trim() !== "") {
          fieldPresence.set(k, (fieldPresence.get(k) || 0) + 1);
        }
      }

      const plan = String(row.LinePlanningNumber || "").trim();
      const key = [
        plan,
        row.JourneyNumber,
        row.FortifyOrderNumber || "0",
        row.LineDirection,
      ].join("|");

      const cur = byJourney.get(key) || { updates: 0 };
      cur.updates += 1;
      cur.line = plan;
      cur.letterHint = LETTER[plan] || "";
      cur.public = row.LinePublicNumber;
      cur.dir = row.LineDirection;
      cur.dest = row.DestinationName || row.DestinationCode;
      cur.destCode = row.DestinationCode;
      cur.status = row.TripStopStatus;
      cur.side = row.SideCode;
      cur.quay = row.QuayCode;
      cur.aimedQuay = row.AimedQuayRef;
      cur.expQuay = row.ExpectedQuayRef;
      cur.actQuay = row.ActualQuayRef;
      cur.timingPt = row.TimingPointCode;
      cur.stopType = row.JourneyStopType;
      cur.transport = row.TransportType;
      cur.vehicle = row.VehicleNumber;
      cur.journey = row.JourneyNumber;
      cur.eta = row.ExpectedArrivalTime;
      cur.target = row.TargetArrivalTime;
      cur.pattern = row.JourneyPatternCode;
      byJourney.set(key, cur);
    }
  }

  const rows = [...byJourney.values()].sort((a, b) =>
    etaKey(a.eta).localeCompare(etaKey(b.eta)),
  );

  console.log("\n=== Distinct journeys at", STOP, ":", rows.length, "===");
  console.log(
    [
      "line",
      "hint",
      "pub",
      "type",
      "dir",
      "veh",
      "status",
      "eta",
      "target",
      "side",
      "quay",
      "aimedQ",
      "expQ",
      "timingPt",
      "dest",
    ].join("\t"),
  );
  for (const r of rows) {
    console.log(
      [
        r.line,
        r.letterHint || "-",
        r.public,
        r.transport,
        r.dir,
        r.vehicle,
        r.status,
        r.eta,
        r.target,
        JSON.stringify(r.side),
        JSON.stringify(r.quay),
        JSON.stringify(r.aimedQuay),
        JSON.stringify(r.expQuay),
        r.timingPt,
        r.dest,
      ].join("\t"),
    );
  }

  const byLine = new Map();
  for (const r of rows) {
    const k = `${r.line}|pub=${r.public}|type=${r.transport}`;
    byLine.set(k, (byLine.get(k) || 0) + 1);
  }
  console.log("\n=== By LinePlanningNumber ===");
  for (const [k, n] of [...byLine.entries()].sort()) {
    console.log(`${k}\tjourneys=${n}`);
  }

  const byEta = new Map();
  for (const r of rows) {
    const k = etaKey(r.eta);
    if (!byEta.has(k)) byEta.set(k, []);
    byEta.get(k).push(r);
  }
  console.log("\n=== Same / clustered ExpectedArrivalTime ===");
  const etas = [...byEta.keys()].filter((e) => e !== "?").sort();
  for (let i = 0; i < etas.length; i += 1) {
    const list = byEta.get(etas[i]);
    const cluster = [{ eta: etas[i], list }];
    // also show neighbors within 30s if parseable as HH:MM:SS
    // (printed when same second OR we note adjacent)
    if (list.length < 2) continue;
    console.log(`\n${etas[i]} (${list.length} vehicles):`);
    for (const r of list) {
      console.log(
        `  line=${r.line} pub=${r.public} type=${r.transport} dir=${r.dir} veh=${r.vehicle} side=${JSON.stringify(r.side)} quay=${JSON.stringify(r.quay)} dest=${r.dest}`,
      );
    }
  }

  // close pairs within 20s
  function toSec(t) {
    const m = String(t).match(/^(\d{1,2}):(\d{2}):(\d{2})$/);
    if (!m) return null;
    return Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]);
  }
  console.log("\n=== Pairs within 20s ===");
  const timed = rows
    .map((r) => ({ ...r, sec: toSec(r.eta) }))
    .filter((r) => r.sec != null)
    .sort((a, b) => a.sec - b.sec);
  for (let i = 0; i < timed.length; i += 1) {
    for (let j = i + 1; j < timed.length; j += 1) {
      const d = timed[j].sec - timed[i].sec;
      if (d > 20) break;
      const a = timed[i];
      const b = timed[j];
      console.log(
        `${d}s apart: ${a.line}/${a.public}/${a.transport} dir${a.dir} veh${a.vehicle} [${a.eta}] vs ${b.line}/${b.public}/${b.transport} dir${b.dir} veh${b.vehicle} [${b.eta}] | side ${JSON.stringify(a.side)}/${JSON.stringify(b.side)} quay ${JSON.stringify(a.quay)}/${JSON.stringify(b.quay)} | ${a.dest} vs ${b.dest}`,
      );
    }
  }

  console.log("\n=== Non-empty fields (count) ===");
  for (const [k, n] of [...fieldPresence.entries()].sort((a, b) => b[1] - a[1])) {
    if (
      /Quay|Side|Direction|Destination|Transport|Line|Timing|Stop|Vehicle|Journey|Expected|Target|Trip/i.test(
        k,
      )
    ) {
      console.log(`${k}\t${n}`);
    }
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
