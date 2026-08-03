/**
 * Compare: are 1682/44/etc metro or tram in live GOVI?
 * Also hunt 2021-2025 across GOVI topics.
 * node scripts/metro-vs-tram-codes.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 90) * 1000;
const SUSPECT = new Set(["43", "44", "1682", "1683", "1684", "1702", "1703", "1704"]);
const TRAM_CLAIM = new Set(["2021", "2023", "2024", "2025"]);
const METRO_PUB = new Set(["A", "B", "C", "D", "E"]);

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
  // Main turbo + any RET passtimes variants if present
  for (const t of [
    "/GOVI/KV8passtimes/RET",
    "/GOVI/KV8passtimes",
    "/GOVI/",
  ]) {
    sub.subscribe(t);
  }
  sub.connect("tcp://pubsub.besteffort.ndovloket.nl:7817");

  const suspect = new Map(); // plan -> {types, pubs, stops, n}
  const metroLetters = new Map(); // plan -> same
  const claim202x = new Map();
  const topicsSeen = new Map();
  const started = Date.now();
  console.log(`Comparing metro vs tram coding (${DURATION_MS / 1000}s)...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const topic = msg[0] ? msg[0].toString() : "";
    topicsSeen.set(topic, (topicsSeen.get(topic) || 0) + 1);
    const text = decode(msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0));

    if (/2021|2023|2024|2025/.test(text)) {
      // raw hit count even if parse fails
      claim202x.set("_raw_msgs", (claim202x.get("_raw_msgs") || 0) + 1);
    }

    for (const row of parseCtx(text)) {
      const plan = String(row.LinePlanningNumber || "").trim();
      const pub = String(row.LinePublicNumber || "").trim();
      const type = String(row.TransportType || "").trim();
      const stop = String(row.UserStopCode || "").toUpperCase();

      const bump = (map, key) => {
        const cur = map.get(key) || {
          types: new Map(),
          pubs: new Map(),
          stops: new Set(),
          n: 0,
        };
        cur.n += 1;
        cur.types.set(type || "?", (cur.types.get(type || "?") || 0) + 1);
        cur.pubs.set(pub || "?", (cur.pubs.get(pub || "?") || 0) + 1);
        if (stop) cur.stops.add(stop);
        map.set(key, cur);
      };

      if (SUSPECT.has(plan)) bump(suspect, plan);
      if (TRAM_CLAIM.has(plan)) bump(claim202x, plan);
      if (METRO_PUB.has(pub.toUpperCase()) || type === "Metro") {
        bump(metroLetters, plan || `(pub=${pub})`);
      }
    }
  }

  function printMap(title, map, { maxStops = 6 } = {}) {
    console.log(`\n=== ${title} ===`);
    if (map.size === 0) {
      console.log("(none)");
      return;
    }
    for (const [plan, info] of [...map.entries()].sort((a, b) =>
      String(a[0]).localeCompare(String(b[0]), undefined, { numeric: true }),
    )) {
      if (plan === "_raw_msgs") {
        console.log(`raw messages containing 202x digits: ${info}`);
        continue;
      }
      const types = [...info.types.entries()].map(([k, n]) => `${k}:${n}`).join(",");
      const pubs = [...info.pubs.entries()].map(([k, n]) => `${k}:${n}`).join(",");
      const stops = [...info.stops].slice(0, maxStops).join(",");
      console.log(
        `plan=${plan}\tn=${info.n}\ttypes{${types}}\tpubs{${pubs}}\tstops~${info.stops.size} e.g. ${stops}`,
      );
    }
  }

  console.log("\n=== Topics received ===");
  for (const [t, n] of [...topicsSeen.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`${n}\t${t}`);
  }

  printMap("Suspect codes (43/44/1682/…/1704) — mode according to feed", suspect);
  printMap("Real metro (TransportType=Metro or public A–E) — their planning numbers", metroLetters, {
    maxStops: 4,
  });
  printMap("Claimed tram planning 2021-2025", claim202x);

  console.log("\n=== Conclusion helpers ===");
  for (const plan of [...SUSPECT].sort()) {
    const info = suspect.get(plan);
    if (!info) {
      console.log(`${plan}: not seen`);
      continue;
    }
    const topType = [...info.types.entries()].sort((a, b) => b[1] - a[1])[0]?.[0];
    const topPub = [...info.pubs.entries()].sort((a, b) => b[1] - a[1])[0]?.[0];
    console.log(
      `${plan}: feed says TransportType=${topType}, LinePublicNumber=${topPub}`,
    );
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
