/**
 * Compare the display commands two servers sent for the same replay.
 *
 *   node scripts/compare-replays.js <a.jsonl> <b.jsonl> [toleranceSeconds]
 *
 * Prints both RET command timelines (time relative to each recording's first
 * command) and exits 1 if the command order differs or a matching command is
 * more than toleranceSeconds apart.
 */
const fs = require("fs");

const [fileA, fileB, toleranceArg = "2"] = process.argv.slice(2);
if (!fileA || !fileB) {
  console.error("usage: compare-replays.js <a.jsonl> <b.jsonl> [toleranceSeconds]");
  process.exit(1);
}
const tolerance = Number(toleranceArg);

function commands(file) {
  const rows = fs
    .readFileSync(file, "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .filter((row) => row.message?.type === "transit-command");
  const first = rows[0]?.at ?? 0;
  return rows.map((row) => ({
    sec: (row.at - first) / 1000,
    command: row.message.command,
    vehicle: row.message.vehiclenumber || "",
  }));
}

const a = commands(fileA);
const b = commands(fileB);
const pad = (value, width) => String(value).padEnd(width);
const fmt = (row) => (row ? `${row.sec.toFixed(1).padStart(7)}s ${pad(row.command, 24)} ${pad(row.vehicle, 6)}` : "");

console.log(`${pad(fileA, 42)} | ${fileB}`);
let problems = 0;
for (let i = 0; i < Math.max(a.length, b.length); i += 1) {
  const left = a[i];
  const right = b[i];
  let note = "";
  if (!left || !right || left.command !== right.command || left.vehicle !== right.vehicle) {
    note = "  <-- differs";
    problems += 1;
  } else if (Math.abs(left.sec - right.sec) > tolerance) {
    note = `  <-- ${Math.abs(left.sec - right.sec).toFixed(1)}s apart`;
    problems += 1;
  }
  console.log(`${pad(fmt(left), 42)} | ${fmt(right)}${note}`);
}

console.log(
  problems === 0
    ? `\nIdentical: ${a.length} commands, timing within ${tolerance}s.`
    : `\n${problems} difference(s) between ${a.length} and ${b.length} commands.`,
);
process.exit(problems === 0 ? 0 : 1);
