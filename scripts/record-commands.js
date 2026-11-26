/**
 * Record what a running server sends to display clients during a replay.
 *
 *   node scripts/record-commands.js <baseWsUrl> <seconds> <out.jsonl>
 *   e.g. node scripts/record-commands.js ws://localhost:8081 1000 captures/main.jsonl
 *
 * Connects to /content (what the display sees), retrying until the server is up,
 * and writes every message with the real arrival time.
 */
const fs = require("fs");
const WebSocket = require("ws");

const [baseUrl, secondsArg, outFile] = process.argv.slice(2);
if (!baseUrl || !secondsArg || !outFile) {
  console.error("usage: record-commands.js <baseWsUrl> <seconds> <out.jsonl>");
  process.exit(1);
}

const out = fs.createWriteStream(outFile);
let received = 0;

function connect() {
  const socket = new WebSocket(`${baseUrl}/content`);
  socket.on("open", () => console.log(`[record] connected to ${baseUrl}/content`));
  socket.on("message", (data) => {
    received += 1;
    out.write(JSON.stringify({ at: Date.now(), message: JSON.parse(String(data)) }) + "\n");
  });
  socket.on("close", () => setTimeout(connect, 1000));
  socket.on("error", () => {});
}

connect();

setTimeout(() => {
  out.end(() => {
    console.log(`[record] ${received} messages written to ${outFile}`);
    process.exit(0);
  });
}, Number(secondsArg) * 1000);
