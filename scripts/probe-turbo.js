const zmq = require("zeromq");
const zlib = require("zlib");

function parseCtxDatedPassTimes(text) {
  const lines = String(text || "").split(/\r?\n/);
  let columns = null;
  const rows = [];

  for (const line of lines) {
    if (line.startsWith("\\L")) {
      columns = line.slice(2).split("|");
      continue;
    }

    if (!columns || !line.trim() || line.startsWith("\\")) {
      continue;
    }

    const values = line.split("|");
    const row = {};
    for (let index = 0; index < columns.length; index += 1) {
      const value = values[index];
      row[columns[index]] = value === "\\0" ? "" : value ?? "";
    }
    rows.push(row);
  }

  return rows;
}

function decode(buf) {
  if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) {
    return zlib.gunzipSync(buf).toString("utf8");
  }
  return buf.toString("utf8");
}

(async () => {
  const sub = new zmq.Subscriber();
  sub.subscribe("/GOVI/KV8passtimes/RET");
  sub.connect("tcp://pubsub.besteffort.ndovloket.nl:7817");

  for await (const msg of sub) {
    const buf = msg.length > 1 ? Buffer.concat(msg.slice(1)) : Buffer.alloc(0);
    const text = decode(buf);
    if (!text.includes("HA1162")) {
      continue;
    }

    const rows = parseCtxDatedPassTimes(text).filter(
      (row) => String(row.UserStopCode || "").toUpperCase() === "HA1162",
    );

    console.log("parsed rows for HA1162:", rows.length);
    if (rows[0]) {
      console.log(JSON.stringify(rows[0], null, 2));
    }
    process.exit(0);
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
