/**
 * Compare the display commands two servers sent for the same replay.
 *
 *   node scripts/compare-replays.js <a.jsonl> <b.jsonl> [toleranceSeconds]
 *
 * Aligns both RET command timelines (command + vehicle, longest common
 * subsequence) so an extra or missing tram shows up as one block instead of
 * shifting every later row. Times are relative to each recording's first command.
 * Exits 1 if a command appears on one side only, or if matched commands are more
 * than toleranceSeconds apart.
 */
const fs = require("fs");
const path = require("path");

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

const key = (row) => `${row.command}|${row.vehicle}`;

function align(a, b) {
  // lcs[i][j] = length of the longest common subsequence of a[i..] and b[j..]
  const lcs = Array.from({ length: a.length + 1 }, () => new Array(b.length + 1).fill(0));
  for (let i = a.length - 1; i >= 0; i -= 1) {
    for (let j = b.length - 1; j >= 0; j -= 1) {
      lcs[i][j] =
        key(a[i]) === key(b[j]) ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }
  const pairs = [];
  let i = 0;
  let j = 0;
  while (i < a.length || j < b.length) {
    if (i < a.length && j < b.length && key(a[i]) === key(b[j])) {
      pairs.push([a[i++], b[j++]]);
    } else if (j >= b.length || (i < a.length && lcs[i + 1][j] >= lcs[i][j + 1])) {
      pairs.push([a[i++], null]);
    } else {
      pairs.push([null, b[j++]]);
    }
  }
  return pairs;
}

const a = commands(fileA);
const b = commands(fileB);
const pad = (value, width) => String(value).padEnd(width);
const fmt = (row) =>
  row ? `${row.sec.toFixed(1).padStart(7)}s ${pad(row.command, 24)} ${pad(row.vehicle, 6)}` : "";

console.log(`${pad(path.basename(fileA), 42)} | ${path.basename(fileB)}`);
let onlyA = 0;
let onlyB = 0;
let late = 0;
for (const [left, right] of align(a, b)) {
  let note = "";
  if (!right) {
    note = "  <-- only in A";
    onlyA += 1;
  } else if (!left) {
    note = "  <-- only in B";
    onlyB += 1;
  } else if (Math.abs(left.sec - right.sec) > tolerance) {
    note = `  <-- ${(right.sec - left.sec).toFixed(1)}s`;
    late += 1;
  }
  console.log(`${pad(fmt(left), 42)} | ${fmt(right)}${note}`);
}

const problems = onlyA + onlyB + late;
console.log(
  problems === 0
    ? `\nIdentical: ${a.length} commands, timing within ${tolerance}s.`
    : `\n${a.length} vs ${b.length} commands: ${onlyA} only in A, ${onlyB} only in B, ` +
        `${late} matched but more than ${tolerance}s apart.`,
);
process.exit(problems === 0 ? 0 : 1);
