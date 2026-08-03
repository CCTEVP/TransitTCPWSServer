/**
 * Validate claimed Beurs tram LinePlanningNumbers vs live GOVI at HA1162 (+ nearby HA116x).
 * node scripts/check-beurs-tram-codes.js [seconds]
 */
const zmq = require("zeromq");
const zlib = require("zlib");

const DURATION_MS = (Number(process.argv[2]) || 60) * 1000;
const CLAIMED = {
  2007: "7 (Meent/Coolsingel)",
  2008: "8 (Beurs)",
  2021: "21 (Beurs)",
  2023: "23 (Beurs)",
  2024: "24 (Beurs)",
  "ret:7": "7",
  "ret:8": "8",
  "ret:21": "21",
  "ret:23": "23",
  "ret:24": "24",
};

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

  const atHa1162 = new Map();
  const claimedAnywhere = new Map();
  const started = Date.now();
  console.log(`Checking claimed tram codes vs live feed (${DURATION_MS / 1000}s)...`);

  for await (const msg of sub) {
    if (Date.now() - started > DURATION_MS) break;
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    if (!/HA1162|2007|2008|2021|2023|2024|LinePlanningNumber/i.test(text)) {
      // still parse RET blocks that may contain HA1162 without those digits in gzip... always parse if HA1162
    }
    if (!/HA1162|2007|2008|2021|2023|2024/i.test(text)) continue;

    for (const row of parseCtx(text)) {
      const stop = String(row.UserStopCode || "").toUpperCase();
      const plan = String(row.LinePlanningNumber || "").trim();
      const pub = String(row.LinePublicNumber || "").trim();
      const transport = String(row.TransportType || "");
      const dest = String(row.DestinationName || "");
      const dir = String(row.LineDirection || "");

      if (stop === "HA1162") {
        const key = `${plan}|pub=${pub}|type=${transport}|dir=${dir}`;
        const cur = atHa1162.get(key) || {
          plan,
          pub,
          transport,
          dir,
          n: 0,
          dests: new Set(),
        };
        cur.n += 1;
        if (dest) cur.dests.add(dest);
        atHa1162.set(key, cur);
      }

      if (CLAIMED[plan] || CLAIMED[plan.toLowerCase()]) {
        const key = `${stop}|${plan}|pub=${pub}|${transport}`;
        const cur = claimedAnywhere.get(key) || {
          stop,
          plan,
          pub,
          transport,
          n: 0,
          dests: new Set(),
        };
        cur.n += 1;
        if (dest) cur.dests.add(dest);
        claimedAnywhere.set(key, cur);
      }
    }
  }

  console.log("\n=== Live at HA1162 ===");
  if (atHa1162.size === 0) {
    console.log("(no rows in window)");
  }
  for (const r of [...atHa1162.values()].sort((a, b) =>
    a.plan.localeCompare(b.plan, undefined, { numeric: true }),
  )) {
    console.log(
      `plan=${r.plan}\tpub=${r.pub}\ttype=${r.transport}\tdir=${r.dir}\tn=${r.n}\t${[...r.dests].join(" | ")}`,
    );
  }

  console.log("\n=== Claimed codes 2007/2008/2021/2023/2024 seen anywhere? ===");
  if (claimedAnywhere.size === 0) {
    console.log("NONE seen in this window.");
  } else {
    for (const r of [...claimedAnywhere.values()].sort((a, b) =>
      a.plan.localeCompare(b.plan) || a.stop.localeCompare(b.stop),
    )) {
      console.log(
        `plan=${r.plan} (${CLAIMED[r.plan] || "?"})\tstop=${r.stop}\tpub=${r.pub}\ttype=${r.transport}\t${[...r.dests].slice(0, 2).join(" | ")}`,
      );
    }
  }

  console.log("\n=== Verdict vs claimed list ===");
  const livePlans = new Set([...atHa1162.values()].map((r) => r.plan));
  for (const code of Object.keys(CLAIMED).filter((k) => /^\d+$/.test(k))) {
    console.log(
      `${code} (${CLAIMED[code]}): ${livePlans.has(code) ? "YES at HA1162" : "not at HA1162"}`,
    );
  }

  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
