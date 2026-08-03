const WebSocket = require("ws");

const WS_URL = process.env.MONITOR_WS_URL || "ws://localhost:8080/control";
const DURATION_MS = Number(process.env.MONITOR_MS || 12 * 60 * 1000);
const STATUS_URL = process.env.MONITOR_STATUS_URL || "http://localhost:8080/api/status";

const events = [];
let lastArrivingByVehicle = new Map();
let arrivedSeenByVehicle = new Set();
let arrivedWithoutArriving = 0;
let arrivedAfterArriving = 0;
let arrivingCount = 0;
let arrivedCount = 0;

function ts() {
  return new Date().toISOString();
}

function vehicleOf(msg) {
  return String(
    msg?.vehiclenumber ||
      msg?.entity?.vehiclenumber ||
      msg?.payload?.vehiclenumber ||
      msg?.payload?.entity?.vehiclenumber ||
      "unknown",
  );
}

function logEvent(kind, detail) {
  const line = `[${ts()}] ${kind} ${detail}`;
  console.log(line);
  events.push(line);
}

function handleCommand(cmd, source) {
  // Prefer content-broadcast; ignore duplicate control-command copies.
  if (source === "control-command") {
    return;
  }

  const command = cmd?.command;
  const vehicle = vehicleOf(cmd);
  if (!command) return;

  if (command === "RET_TRAIN_ARRIVING_15S") {
    arrivingCount += 1;
    lastArrivingByVehicle.set(vehicle, Date.now());
    arrivedSeenByVehicle.delete(vehicle);
    logEvent("ARRIVING", `vehicle=${vehicle} source=${source}`);
    return;
  }

  if (command === "RET_TRAIN_ARRIVED") {
    // Count first ARRIVED per vehicle/cycle only (server may rebroadcast).
    if (arrivedSeenByVehicle.has(vehicle)) {
      return;
    }
    arrivedSeenByVehicle.add(vehicle);
    arrivedCount += 1;
    const prev = lastArrivingByVehicle.get(vehicle);
    if (prev) {
      arrivedAfterArriving += 1;
      const deltaSec = ((Date.now() - prev) / 1000).toFixed(1);
      logEvent(
        "ARRIVED_OK",
        `vehicle=${vehicle} source=${source} afterArriving=${deltaSec}s`,
      );
      lastArrivingByVehicle.delete(vehicle);
    } else {
      arrivedWithoutArriving += 1;
      logEvent(
        "ARRIVED_WITHOUT_ARRIVING",
        `vehicle=${vehicle} source=${source}`,
      );
    }
    return;
  }

  if (
    command === "RET_TRAIN_DEPARTED" ||
    command === "RET_NO_TRAIN"
  ) {
    if (command === "RET_TRAIN_DEPARTED") {
      arrivedSeenByVehicle.delete(vehicle);
    }
    if (command === "RET_NO_TRAIN") {
      arrivedSeenByVehicle.clear();
      lastArrivingByVehicle.clear();
    }
    logEvent(command.replace("RET_", ""), `vehicle=${vehicle} source=${source}`);
  }
}

async function pollTurboStatus() {
  try {
    const res = await fetch(STATUS_URL, { cache: "no-store" });
    if (!res.ok) return;
    const data = await res.json();
    const turbo = data?.zmq?.turbo || {};
    logEvent(
      "TURBO",
      `state=${turbo.state} subscribed=${turbo.subscribed} in=${turbo.bytesReceived || 0} kept=${turbo.bytesRelevant || 0}`,
    );
  } catch {
    logEvent("TURBO", "status-fetch-failed");
  }
}

function printSummary() {
  console.log("\n=== MONITOR SUMMARY ===");
  console.log(`arriving events: ${arrivingCount}`);
  console.log(`arrived events:  ${arrivedCount}`);
  console.log(`arrived after arriving:     ${arrivedAfterArriving}`);
  console.log(`arrived WITHOUT arriving:   ${arrivedWithoutArriving}`);
  if (arrivedCount === 0) {
    console.log("result: NO_ARRIVAL_OBSERVED (need more time / no train yet)");
  } else if (arrivedWithoutArriving === 0) {
    console.log("result: PASS (every ARRIVED had a prior ARRIVING)");
  } else {
    console.log("result: FAIL (at least one ARRIVED without prior ARRIVING)");
  }
}

console.log(`[monitor] listening on ${WS_URL} for ${DURATION_MS / 1000}s`);
const ws = new WebSocket(WS_URL);

ws.on("open", () => {
  logEvent("WS", "control connected");
  pollTurboStatus();
});

ws.on("message", (raw) => {
  let msg;
  try {
    msg = JSON.parse(String(raw));
  } catch {
    return;
  }

  if (msg.type === "transit-command") {
    handleCommand(msg, "control-command");
    return;
  }

  if (
    msg.type === "ws-event" &&
    msg.event === "broadcast" &&
    msg.payload?.type === "transit-command"
  ) {
    handleCommand(msg.payload, "content-broadcast");
  }
});

ws.on("close", () => logEvent("WS", "control disconnected"));
ws.on("error", (err) => logEvent("WS", `error: ${err.message}`));

const statusTimer = setInterval(pollTurboStatus, 30000);

setTimeout(() => {
  clearInterval(statusTimer);
  try {
    ws.close();
  } catch {}
  printSummary();
  process.exit(arrivedWithoutArriving > 0 ? 2 : 0);
}, DURATION_MS);
