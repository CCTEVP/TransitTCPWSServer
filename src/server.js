const path = require("path");
const http = require("http");
const crypto = require("crypto");
const zlib = require("zlib");
const express = require("express");
const WebSocket = require("ws");
const zmq = require("zeromq");
const { XMLParser } = require("fast-xml-parser");
require("dotenv").config();

function envList(name, fallback = "") {
  return String(process.env[name] || fallback)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function envBool(name, fallback = true) {
  const value = process.env[name];
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  return !["0", "false", "no", "off"].includes(String(value).toLowerCase());
}

const PORT = Number(process.env.PORT || 8080);
const ZMQ_ENDPOINT =
  process.env.ZMQ_ENDPOINT || "tcp://pubsub.besteffort.ndovloket.nl:7658";
const ZMQ_TOPICS = envList(
  "ZMQ_TOPICS",
  "/RIG/KV17cvlinfo,/RIG/KV6posinfo",
);
const ZMQ_TURBO_ENDPOINT =
  process.env.ZMQ_TURBO_ENDPOINT ||
  "tcp://pubsub.besteffort.ndovloket.nl:7817";
const ZMQ_TURBO_TOPICS = envList(
  "ZMQ_TURBO_TOPICS",
  "/GOVI/KV8passtimes/RET",
);
const ZMQ_TURBO_ENABLED = envBool("ZMQ_TURBO_ENABLED", true);
const ZMQ_TURBO_ON_DEMAND = envBool("ZMQ_TURBO_ON_DEMAND", true);
const ZMQ_TURBO_IDLE_UNSUBSCRIBE_MS = Number(
  process.env.ZMQ_TURBO_IDLE_UNSUBSCRIBE_MS || 60000,
);
// Emit RET_TRAIN_ARRIVING_15S only when forecast ETA is at/below this (seconds).
const ARRIVING_ETA_SEC = Number(process.env.ARRIVING_ETA_SEC || 15);
const UPCOMING_STALE_MS = Number(process.env.UPCOMING_STALE_MS || 120000);
const UPCOMING_DISPLAY_LIMIT = Number(process.env.UPCOMING_DISPLAY_LIMIT || 5);
// Keep cached forecasts this long after expected arrival while GOVI is off.
const UPCOMING_OVERDUE_KEEP_SEC = Number(
  process.env.UPCOMING_OVERDUE_KEEP_SEC || 90,
);
const MAX_PAYLOAD_BYTES = Number(process.env.MAX_PAYLOAD_BYTES || 200000);
const WS_CHANNEL_CONTROL = "control";
const WS_CHANNEL_CONTENT = "content";
const ROTTERDAM_TOPIC_PREFIX = process.env.ROTTERDAM_TOPIC_PREFIX || "/RIG/";
const USERSTOPCODES = envList("USERSTOPCODES").map((code) => code.toLowerCase());
// GOVI/Turbo stop filter. Empty env → reuse USERSTOPCODES. Set explicitly for quay split.
const GOVI_USERSTOPCODES = (
  process.env.GOVI_USERSTOPCODES === undefined
    ? USERSTOPCODES
    : envList("GOVI_USERSTOPCODES").map((code) => code.toLowerCase())
);
// Empty = all lines. Exact codes and wildcards (# = one digit, * = any run).
// Example: 1### matches 1682/1702/… ; combine with literals: 1###,43,44
const LINEPLANNINGNUMBERS = envList("LINEPLANNINGNUMBERS").map((code) =>
  String(code).toLowerCase(),
);
const LINEPLANNING_PATTERNS = LINEPLANNINGNUMBERS.map((pattern) => {
  if (!/[!*#?]/.test(pattern)) {
    return { type: "exact", value: pattern };
  }
  // # = one digit; * = any chars; ? = one char; escape other regex meta
  const source = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".")
    .replace(/#/g, "\\d");
  return { type: "regex", value: new RegExp(`^${source}$`, "i") };
});
// LinePlanningNumber → LinePublicNumber fallback when feed omits public number.
const RET_BEURS_TRAM_PUBLIC = {
  1702: "1",
  1682: "3",
  43: "4",
  1683: "5",
  1703: "6",
  44: "7",
  1704: "11",
};
// GOVI/BISON wall-clock times are Dutch local time, not the server TZ (UTC on Cloud Run).
const TRANSIT_TIMEZONE =
  process.env.TRANSIT_TIMEZONE || "Europe/Amsterdam";
const NO_TRAIN_INITIAL_DELAY_MS = Number(
  process.env.NO_TRAIN_INITIAL_DELAY_MS || 30000,
);
const NO_TRAIN_AFTER_DEPARTURE_MS = Number(
  process.env.NO_TRAIN_AFTER_DEPARTURE_MS || 30000,
);
const WS_PING_INTERVAL_MS = Number(process.env.WS_PING_INTERVAL_MS || 25000);
const ALLOWED_RET_COMMANDS = new Set([
  "RET_NO_TRAIN",
  "RET_TRAIN_ARRIVING_15S",
  "RET_TRAIN_ARRIVED",
  "RET_TRAIN_DEPARTED",
]);
let initialNoTrainSent = false;
let departureNoTrainTimer = null;
let bisonSubscribed = false;
let turboSubscribed = false;
let feedPhase = "bootstrap"; // bootstrap | rig | govi | arriving
let feedSwitchingActive = false;
const arrivingTimers = new Map();
const arrivedCountdownTimers = new Map();
// stopCode -> Map(journeyKey -> upcoming forecast entry)
const upcomingByStop = new Map();

const app = express();
const publicDir = path.join(__dirname, "..", "public");
const contentDir = path.join(publicDir, "content");

app.get("/", (req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});
app.get(["/status", "/dashboard/status"], (req, res) => {
  res.redirect(301, "/");
});
app.get(["/content", "/content/"], (req, res) => {
  res.sendFile(path.join(contentDir, "transit", "v3", "index.html"));
});
app.use("/content", express.static(path.join(contentDir, "transit", "v3")));
app.use("/content", express.static(contentDir));
app.use(express.static(publicDir));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  trimValues: true,
  parseTagValue: false,
});

const subscriber = new zmq.Subscriber();
const turboSubscriber = new zmq.Subscriber();
const wsClientMeta = new Map();
const lastBroadcastKeyByChannel = {
  [WS_CHANNEL_CONTENT]: null,
  [WS_CHANNEL_CONTROL]: null,
};
// Last RET command sent to /content clients; replayed to new connections.
let lastContentCommand = null;

const bridgeStatus = {
  startedAt: new Date().toISOString(),
  zmq: {
    endpoint: ZMQ_ENDPOINT,
    topics: ZMQ_TOPICS,
    state: "initializing",
    subscribed: false,
    connectedAt: null,
    lastSubscribeAt: null,
    lastUnsubscribeAt: null,
    feedPhase: "bootstrap",
    feedSwitchingActive: false,
    turbo: {
      enabled: ZMQ_TURBO_ENABLED,
      onDemand: ZMQ_TURBO_ON_DEMAND,
      idleUnsubscribeMs: ZMQ_TURBO_IDLE_UNSUBSCRIBE_MS,
      endpoint: ZMQ_TURBO_ENDPOINT,
      topics: ZMQ_TURBO_TOPICS,
      state: ZMQ_TURBO_ENABLED ? "initializing" : "disabled",
      subscribed: false,
      connectedAt: null,
      lastMessageAt: null,
      lastSubscribeAt: null,
      lastUnsubscribeAt: null,
      totalMessages: 0,
      droppedMessages: 0,
      ignoredMessages: 0,
      relevantMessages: 0,
      bytesReceived: 0,
      bytesRelevant: 0,
      lastError: null,
    },
    lastMessageAt: null,
    totalMessages: 0,
    droppedMessages: 0,
    ignoredMessages: 0,
    relevantMessages: 0,
    bytesReceived: 0,
    bytesRelevant: 0,
    lastError: null,
    userstopcodes: USERSTOPCODES,
    goviUserstopcodes: GOVI_USERSTOPCODES,
    lineplanningnumbers: LINEPLANNINGNUMBERS,
  },
  websocket: {
    contentBytesSent: 0,
    controlBytesSent: 0,
  },
  config: {
    rotterdamTopicPrefix: ROTTERDAM_TOPIC_PREFIX,
    noTrainInitialDelayMs: NO_TRAIN_INITIAL_DELAY_MS,
    noTrainAfterDepartureMs: NO_TRAIN_AFTER_DEPARTURE_MS,
    maxPayloadBytes: MAX_PAYLOAD_BYTES,
    wsPingIntervalMs: WS_PING_INTERVAL_MS,
    turboEnabled: ZMQ_TURBO_ENABLED,
    turboOnDemand: ZMQ_TURBO_ON_DEMAND,
    turboIdleUnsubscribeMs: ZMQ_TURBO_IDLE_UNSUBSCRIBE_MS,
  },
};

function secondsSince(iso) {
  if (!iso) {
    return null;
  }

  return Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
}

function normalizeWsChannel(urlPath) {
  const rawPath = String(urlPath || "/")
    .split("?")[0]
    .toLowerCase();
  const pathOnly =
    rawPath.endsWith("/") && rawPath.length > 1
      ? rawPath.slice(0, -1)
      : rawPath;

  if (pathOnly === "/control") {
    return WS_CHANNEL_CONTROL;
  }

  // Keep backward compatibility for clients still connecting to root.
  if (pathOnly === "/content" || pathOnly === "/") {
    return WS_CHANNEL_CONTENT;
  }

  return "unknown";
}

function getVehicleNumberFromMessage(obj) {
  if (!obj || typeof obj !== "object") {
    return "";
  }

  return String(
    obj.vehiclenumber ||
      obj.entity?.vehiclenumber ||
      obj.data?.vehiclenumber ||
      obj.payload?.vehiclenumber ||
      obj.payload?.entity?.vehiclenumber ||
      obj.payload?.data?.vehiclenumber ||
      "",
  );
}

function toClientCommandMessage(commandMessage) {
  const vehiclenumber = getVehicleNumberFromMessage(commandMessage);

  const message = {
    type: "transit-command",
    protocol: commandMessage.protocol || "RET",
    command: commandMessage.command,
    receivedAt: commandMessage.receivedAt || new Date().toISOString(),
  };

  if (vehiclenumber) {
    message.vehiclenumber = vehiclenumber;
  }

  return message;
}

function getBroadcastDedupeKey(obj) {
  if (!obj || typeof obj !== "object") {
    return null;
  }

  const type = String(obj.type || "");

  if (type === "transit-command") {
    return `cmd|${obj.command || ""}|${getVehicleNumberFromMessage(obj)}`;
  }

  if (type === "ws-event" && obj.event === "broadcast" && obj.payload) {
    return `broadcast|${obj.payload.command || ""}|${getVehicleNumberFromMessage(obj.payload)}`;
  }

  if (type === "ws-event") {
    return `ws-event|${obj.event || ""}|${obj.channel || ""}|${obj.clientId || ""}`;
  }

  if (type === "transit-update") {
    const rows = Array.isArray(obj.matchingRows) ? obj.matchingRows : [];
    const vehicles = rows
      .map(
        (row) =>
          row?.entity?.vehiclenumber ||
          row?.data?.vehiclenumber ||
          "",
      )
      .filter(Boolean)
      .sort()
      .join(",");
    const commands = rows
      .map((row) => row?.sourceCommand || "")
      .filter(Boolean)
      .sort()
      .join(",");
    return `update|${obj.source || ""}|${obj.topic || ""}|${obj.matchingRowCount || 0}|${commands}|${vehicles}`;
  }

  if (type === "bridge-status") {
    return `bridge-status|${obj.status || ""}|${obj.channel || ""}|${obj.clientId || ""}`;
  }

  return `${type}|${JSON.stringify(obj)}`;
}

function shouldSkipDuplicateBroadcast(channel, obj) {
  const key = getBroadcastDedupeKey(obj);
  if (!key) {
    return false;
  }

  if (lastBroadcastKeyByChannel[channel] === key) {
    return true;
  }

  lastBroadcastKeyByChannel[channel] = key;
  return false;
}

function sendWsMessage(client, message) {
  if (client.readyState !== WebSocket.OPEN) {
    return false;
  }

  client.send(message);

  const bytes = Buffer.byteLength(message, "utf8");
  const meta = wsClientMeta.get(client);
  if (meta) {
    meta.lastSentAt = new Date().toISOString();
    meta.sentCount = (meta.sentCount || 0) + 1;
    meta.sentBytes = (meta.sentBytes || 0) + bytes;

    if (meta.channel === WS_CHANNEL_CONTENT) {
      bridgeStatus.websocket.contentBytesSent += bytes;
    } else if (meta.channel === WS_CHANNEL_CONTROL) {
      bridgeStatus.websocket.controlBytesSent += bytes;
    }
  }

  return true;
}

function broadcastChannelMessage(channel, message) {
  let sent = 0;

  for (const client of wss.clients) {
    const meta = wsClientMeta.get(client);
    if (!meta || meta.channel !== channel) {
      continue;
    }

    if (sendWsMessage(client, message)) {
      sent += 1;
    }
  }

  return sent;
}

function emitWsEvent(event, details) {
  const payload = {
    type: "ws-event",
    event,
    at: new Date().toISOString(),
    ...details,
  };

  if (shouldSkipDuplicateBroadcast(WS_CHANNEL_CONTROL, payload)) {
    return 0;
  }

  const message = JSON.stringify(payload);
  return broadcastChannelMessage(WS_CHANNEL_CONTROL, message);
}

function summarizeWsClients() {
  const clients = [];
  const contentClients = [];
  const controlClients = [];
  const unknownClients = [];

  for (const meta of wsClientMeta.values()) {
    const summary = {
      id: meta.id,
      channel: meta.channel,
      connectedAt: meta.connectedAt,
      secondsConnected: secondsSince(meta.connectedAt),
      lastSentAt: meta.lastSentAt,
      secondsSinceLastSent: secondsSince(meta.lastSentAt),
      sentCount: meta.sentCount || 0,
      sentBytes: meta.sentBytes || 0,
      remoteAddress: meta.remoteAddress,
    };

    clients.push(summary);

    if (meta.channel === WS_CHANNEL_CONTENT) {
      contentClients.push(summary);
      continue;
    }

    if (meta.channel === WS_CHANNEL_CONTROL) {
      controlClients.push(summary);
      continue;
    }

    unknownClients.push(summary);
  }

  const contentMessagesSent = contentClients.reduce(
    (sum, client) => sum + (client.sentCount || 0),
    0,
  );
  const controlMessagesSent = controlClients.reduce(
    (sum, client) => sum + (client.sentCount || 0),
    0,
  );

  return {
    totalConnected: wss.clients.size,
    contentConnected: contentClients.length,
    controlConnected: controlClients.length,
    unknownConnected: unknownClients.length,
    contentMessagesSent,
    controlMessagesSent,
    contentBytesSent: bridgeStatus.websocket.contentBytesSent,
    controlBytesSent: bridgeStatus.websocket.controlBytesSent,
    bytesSent:
      bridgeStatus.websocket.contentBytesSent +
      bridgeStatus.websocket.controlBytesSent,
    clients,
    contentClients,
    controlClients,
    unknownClients,
  };
}

function getStatusPayload() {
  const bisonBytes = bridgeStatus.zmq.bytesReceived || 0;
  const turboBytes = bridgeStatus.zmq.turbo.bytesReceived || 0;
  const bisonRelevantBytes = bridgeStatus.zmq.bytesRelevant || 0;
  const turboRelevantBytes = bridgeStatus.zmq.turbo.bytesRelevant || 0;

  return {
    service: "transit-tcp-ws-server",
    now: new Date().toISOString(),
    uptimeSeconds: Math.floor(process.uptime()),
    startedAt: bridgeStatus.startedAt,
    config: bridgeStatus.config,
    zmq: {
      ...bridgeStatus.zmq,
      bytesReceived: bisonBytes,
      bytesRelevant: bisonRelevantBytes,
      inboundBytes: bisonBytes + turboBytes,
      relevantBytes: bisonRelevantBytes + turboRelevantBytes,
      secondsSinceLastMessage: secondsSince(bridgeStatus.zmq.lastMessageAt),
      secondsSinceConnected: secondsSince(bridgeStatus.zmq.connectedAt),
      turbo: {
        ...bridgeStatus.zmq.turbo,
        bytesReceived: turboBytes,
        bytesRelevant: turboRelevantBytes,
        secondsSinceConnected: secondsSince(
          bridgeStatus.zmq.turbo.connectedAt,
        ),
        secondsSinceLastMessage: secondsSince(
          bridgeStatus.zmq.turbo.lastMessageAt,
        ),
      },
    },
    websocket: {
      ...summarizeWsClients(),
      startedAt: bridgeStatus.startedAt,
      uptimeSeconds: Math.floor(process.uptime()),
    },
    upcomingVehicles: getUpcomingVehiclesSnapshot(),
  };
}

app.get("/api/status", (req, res) => {
  res.json(getStatusPayload());
});

function broadcastJson(obj) {
  const outbound =
    obj?.type === "transit-command" ? toClientCommandMessage(obj) : obj;

  if (outbound?.type === "transit-command" && outbound.command) {
    lastContentCommand = outbound;
  }

  if (shouldSkipDuplicateBroadcast(WS_CHANNEL_CONTENT, outbound)) {
    return 0;
  }

  const message = JSON.stringify(outbound);
  const contentRecipients = broadcastChannelMessage(
    WS_CHANNEL_CONTENT,
    message,
  );

  if (outbound?.type !== "ws-event") {
    emitWsEvent("broadcast", {
      channel: WS_CHANNEL_CONTENT,
      messageType: outbound?.type || "unknown",
      recipients: contentRecipients,
      totalRecipients: contentRecipients,
      payload: outbound,
    });
  }

  return contentRecipients;
}

function broadcastControlJson(obj) {
  // Control/TCP dashboard keeps full payloads (entity + data).
  if (shouldSkipDuplicateBroadcast(WS_CHANNEL_CONTROL, obj)) {
    return 0;
  }

  const message = JSON.stringify(obj);
  return broadcastChannelMessage(WS_CHANNEL_CONTROL, message);
}

function isContentChannel(channel) {
  return channel === WS_CHANNEL_CONTENT;
}

wss.on("connection", (socket, request) => {
  const clientId = crypto.randomUUID();
  const channel = normalizeWsChannel(request?.url);
  const remoteAddress = request?.socket?.remoteAddress || null;

  wsClientMeta.set(socket, {
    id: clientId,
    channel,
    connectedAt: new Date().toISOString(),
    lastSentAt: null,
    sentCount: 0,
    sentBytes: 0,
    remoteAddress,
  });

  console.log(`[WS] Client connected (${channel})`);

  const welcome = {
    type: "bridge-status",
    status: "connected",
    channel,
    endpoint: ZMQ_ENDPOINT,
    topics: ZMQ_TOPICS,
    clientId,
    serverTime: new Date().toISOString(),
  };
  socket.send(JSON.stringify(welcome));
  // Welcome is per-client; do not apply channel-wide broadcast dedupe.

  if (isContentChannel(channel)) {
    if (lastContentCommand) {
      // Replay current display state to late joiners / refreshes.
      const snapshot = JSON.stringify(lastContentCommand);
      socket.send(snapshot);
      const meta = wsClientMeta.get(socket);
      if (meta) {
        meta.lastSentAt = new Date().toISOString();
        meta.sentCount += 1;
        meta.sentBytes += Buffer.byteLength(snapshot);
        bridgeStatus.websocket.contentBytesSent += Buffer.byteLength(snapshot);
      }
    }

    emitWsEvent("client-connected", {
      channel,
      clientId,
      remoteAddress,
    });
  }

  socket.isAlive = true;
  socket.on("pong", () => {
    socket.isAlive = true;
  });

  socket.on("close", () => {
    wsClientMeta.delete(socket);
    console.log(`[WS] Client disconnected (${channel})`);

    if (isContentChannel(channel)) {
      emitWsEvent("client-disconnected", {
        channel,
        clientId,
        remoteAddress,
      });
    }
  });
});

setInterval(() => {
  const heartbeat = JSON.stringify({
    type: "heartbeat",
    serverTime: new Date().toISOString(),
  });

  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (!client.isAlive) {
      client.terminate();
      continue;
    }

    client.isAlive = false;
    // Protocol ping (browser-handled) + JSON heartbeat (visible to client JS).
    try {
      client.ping();
    } catch {
      // ignore
    }
    try {
      client.send(heartbeat);
    } catch {
      // ignore
    }
  }
}, WS_PING_INTERVAL_MS);

function safeUtf8(buffer) {
  try {
    return buffer.toString("utf8");
  } catch {
    return null;
  }
}

function toArray(value) {
  if (value === undefined || value === null) {
    return [];
  }

  return Array.isArray(value) ? value : [value];
}

function detectAndDecodePayload(payloadBuffer) {
  // GOVI KV6/KV15/KV17 messages are commonly gzip-compressed XML.
  const isGzip =
    payloadBuffer.length > 2 &&
    payloadBuffer[0] === 0x1f &&
    payloadBuffer[1] === 0x8b;

  if (isGzip) {
    const decompressed = zlib.gunzipSync(payloadBuffer);
    return {
      encoding: "gzip+xml",
      buffer: decompressed,
      text: safeUtf8(decompressed),
    };
  }

  return {
    encoding: "raw",
    buffer: payloadBuffer,
    text: safeUtf8(payloadBuffer),
  };
}

function extractEntity(node) {
  if (!node || typeof node !== "object") {
    return {};
  }

  return {
    dataownercode: getRowValue(node, "dataownercode"),
    lineplanningnumber: getRowValue(node, "lineplanningnumber"),
    linepublicnumber: getRowValue(node, [
      "linepublicnumber",
      "linepublicnummer",
    ]),
    operatingday: getRowValue(node, [
      "operatingday",
      "operatingdate",
      "operationdate",
    ]),
    journeynumber: getRowValue(node, "journeynumber"),
    reinforcementnumber: getRowValue(node, "reinforcementnumber"),
    userstopcode: getRowValue(node, "userstopcode"),
    passagesequencenumber: getRowValue(node, "passagesequencenumber"),
    vehiclenumber: getRowValue(node, "vehiclenumber"),
    timestamp: getRowValue(node, "timestamp"),
  };
}

function getRowValue(row, fieldNames) {
  if (!row || typeof row !== "object") {
    return null;
  }

  const names = Array.isArray(fieldNames) ? fieldNames : [fieldNames];

  for (const name of names) {
    if (row[name] !== undefined && row[name] !== null && row[name] !== "") {
      return row[name];
    }

    const key = Object.keys(row).find(
      (candidate) => candidate.toLowerCase() === String(name).toLowerCase(),
    );
    if (
      key &&
      row[key] !== undefined &&
      row[key] !== null &&
      row[key] !== ""
    ) {
      return row[key];
    }
  }

  return null;
}

function asUpperString(value) {
  return String(value || "").toUpperCase();
}

function parseDayParts(dayValue, fallbackIso) {
  const raw = String(dayValue || fallbackIso || "");
  const dayMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (dayMatch) {
    return {
      year: Number(dayMatch[1]),
      month: Number(dayMatch[2]),
      day: Number(dayMatch[3]),
    };
  }

  const fallback = new Date(fallbackIso || Date.now());
  if (Number.isNaN(fallback.getTime())) {
    return null;
  }

  // Use transit timezone calendar day, not the server's local day.
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-CA", {
      timeZone: TRANSIT_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    })
      .formatToParts(fallback)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );

  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
  };
}

function zonedWallTimeToUtcDate(year, month, day, hour, minute, second, timeZone) {
  // Convert a wall-clock time in `timeZone` to a UTC Date without depending on
  // process TZ (Cloud Run is UTC; GOVI times are Europe/Amsterdam).
  const utcGuess = Date.UTC(year, month - 1, day, hour, minute, second);
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });

  const parts = Object.fromEntries(
    dtf
      .formatToParts(new Date(utcGuess))
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );

  const asUtcFromParts = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    Number(parts.second),
  );

  return new Date(utcGuess - (asUtcFromParts - utcGuess));
}

function toDateOnDay(timeValue, dayValue, fallbackIso) {
  if (!timeValue || typeof timeValue !== "string") {
    return null;
  }

  const trimmed = timeValue.trim();

  // Fully qualified ISO with explicit zone/offset — trust native parsing.
  if (/[zZ]|[+-]\d{2}:\d{2}$/.test(trimmed)) {
    const direct = new Date(trimmed);
    if (!Number.isNaN(direct.getTime())) {
      return direct;
    }
  }

  // Date-time without zone: treat wall time as TRANSIT_TIMEZONE.
  const dateTimeMatch = trimmed.match(
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/,
  );
  if (dateTimeMatch) {
    return zonedWallTimeToUtcDate(
      Number(dateTimeMatch[1]),
      Number(dateTimeMatch[2]),
      Number(dateTimeMatch[3]),
      Number(dateTimeMatch[4]),
      Number(dateTimeMatch[5]),
      Number(dateTimeMatch[6] || 0),
      TRANSIT_TIMEZONE,
    );
  }

  // Accept HH:mm[:ss] values by anchoring them to operation/reception date
  // in the transit timezone (not the server local timezone).
  const match = trimmed.match(/^(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) {
    return null;
  }

  const dayParts = parseDayParts(dayValue, fallbackIso);
  if (!dayParts) {
    return null;
  }

  return zonedWallTimeToUtcDate(
    dayParts.year,
    dayParts.month,
    dayParts.day,
    Number(match[1]),
    Number(match[2]),
    Number(match[3] || 0),
    TRANSIT_TIMEZONE,
  );
}

function extractExpectedArrivalMs(row, receivedAt) {
  if (!row || typeof row !== "object") {
    return null;
  }

  const operatingDay =
    getRowValue(row, [
      "operatingday",
      "operatingdate",
      "operationdate",
      "timestamp",
    ]) || receivedAt;

  const expectedArrival = getRowValue(row, "expectedarrivaltime");
  if (expectedArrival) {
    const arrivalDate = toDateOnDay(expectedArrival, operatingDay, receivedAt);
    if (arrivalDate) {
      return arrivalDate.getTime();
    }
  }

  const etaSeconds = extractEtaSeconds(row, receivedAt);
  if (!Number.isFinite(etaSeconds)) {
    return null;
  }

  return new Date(receivedAt).getTime() + etaSeconds * 1000;
}

function extractEtaSeconds(row, receivedAt) {
  if (!row || typeof row !== "object") {
    return null;
  }

  const operatingDay =
    getRowValue(row, [
      "operatingday",
      "operatingdate",
      "operationdate",
      "timestamp",
    ]) || receivedAt;

  const expectedArrival = getRowValue(row, "expectedarrivaltime");
  if (expectedArrival) {
    const arrivalDate = toDateOnDay(expectedArrival, operatingDay, receivedAt);
    if (arrivalDate) {
      return Math.round(
        (arrivalDate.getTime() - new Date(receivedAt).getTime()) / 1000,
      );
    }
  }

  const numericCandidates = ["secondsleft", "remainingseconds", "timetostop"];

  for (const field of numericCandidates) {
    const value = getRowValue(row, field);
    if (value === undefined || value === null || value === "") {
      continue;
    }

    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      return Math.round(numeric);
    }
  }

  const targetArrival = getRowValue(row, "targetarrivaltime");
  if (targetArrival) {
    const targetDate = toDateOnDay(targetArrival, operatingDay, receivedAt);
    if (targetDate) {
      const punctuality = Number(getRowValue(row, "punctuality"));
      const punctualityMs = Number.isFinite(punctuality) ? punctuality * 1000 : 0;
      const expectedMs = targetDate.getTime() + punctualityMs;
      return Math.round(
        (expectedMs - new Date(receivedAt).getTime()) / 1000,
      );
    }
  }

  const timeCandidates = [
    "actualarrivaltime",
    "arrivaltime",
    "targetdeparturetime",
  ];

  for (const field of timeCandidates) {
    const value = getRowValue(row, field);
    const arrivalDate = toDateOnDay(value, operatingDay, receivedAt);
    if (!arrivalDate) {
      continue;
    }

    return Math.round(
      (arrivalDate.getTime() - new Date(receivedAt).getTime()) / 1000,
    );
  }

  return null;
}

function containsNoTrainSignal(row, baseCommand) {
  const commandUpper = asUpperString(baseCommand.command);
  if (commandUpper.includes("CANCEL") || commandUpper.includes("STOPERROR")) {
    return true;
  }

  const textualFields = Object.values(row || {})
    .map((value) => String(value || "").toLowerCase())
    .join(" ");

  return (
    textualFields.includes("geen trein") ||
    textualFields.includes("rit vervallen") ||
    textualFields.includes("uitgevallen") ||
    textualFields.includes("cancel") ||
    textualFields.includes("canceled")
  );
}

function buildRotterdamTrainStateCommands(topic, baseCommands) {
  if (!String(topic || "").startsWith(ROTTERDAM_TOPIC_PREFIX)) {
    return [];
  }

  const stateCommands = [];

  for (const baseCommand of baseCommands) {
    const protocol = asUpperString(baseCommand.protocol);
    const command = asUpperString(baseCommand.command);
    const row = baseCommand.data || {};
    const etaSeconds = extractEtaSeconds(row, baseCommand.receivedAt);
    const stopCode = String(baseCommand.entity?.userstopcode || "").toLowerCase();
    const journeyKey = getJourneyKeyFromCommand(baseCommand);
    const approachKey = stopCode && journeyKey ? `${stopCode}|${journeyKey}` : null;

    if (containsNoTrainSignal(row, baseCommand)) {
      if (approachKey) {
        clearArrivingTimer(approachKey);
        clearArrivedCountdown(approachKey);
      }
      stateCommands.push({
        type: "transit-command",
        protocol: "RET",
        command: "RET_NO_TRAIN",
        topic,
        receivedAt: baseCommand.receivedAt,
        sourceCommand: baseCommand.command,
        entity: baseCommand.entity,
        data: row,
      });
      continue;
    }

    const isApproachUpdate =
      (protocol === "KV6" || protocol === "KV17") && command.includes("ONROUTE");

    if (isApproachUpdate) {
      // RIG only detects approach; GOVI owns ETA / ARRIVING forecasts.
      // Keep RIG while a train is already ARRIVED so we still see DEPARTURE.
      maybeActivateGoviForOnRoute(baseCommand);
      continue;
    }

    const isPureArrival =
      (protocol === "KV6" || protocol === "KV17") && command.endsWith("_ARRIVAL");

    const isArrived =
      command.includes("ONSTOP") ||
      isPureArrival ||
      command.includes("END") ||
      (command.includes("ARRIVAL") &&
        Number.isFinite(etaSeconds) &&
        etaSeconds <= 0);

    if (isArrived) {
      if (approachKey) {
        clearArrivingTimer(approachKey);
        clearArrivedCountdown(approachKey);
      }
      stateCommands.push({
        type: "transit-command",
        protocol: "RET",
        command: "RET_TRAIN_ARRIVED",
        topic,
        receivedAt: baseCommand.receivedAt,
        sourceCommand: baseCommand.command,
        entity: baseCommand.entity,
        data: row,
      });
      continue;
    }

    const isDeparted =
      command.includes("DEPARTURE") ||
      command.includes("DEPART") ||
      command.includes("OFFROUTE");

    if (isDeparted) {
      if (approachKey) {
        clearArrivingTimer(approachKey);
        clearArrivedCountdown(approachKey);
      }
      stateCommands.push({
        type: "transit-command",
        protocol: "RET",
        command: "RET_TRAIN_DEPARTED",
        topic,
        receivedAt: baseCommand.receivedAt,
        sourceCommand: baseCommand.command,
        entity: baseCommand.entity,
        data: row,
      });
    }
  }

  return stateCommands;
}

function isAllowedRetCommand(commandMessage) {
  return (
    commandMessage?.type === "transit-command" &&
    commandMessage?.protocol === "RET" &&
    ALLOWED_RET_COMMANDS.has(commandMessage?.command)
  );
}

function matchesUserStopCode(row, stopList = USERSTOPCODES) {
  if (stopList.length === 0) {
    return true;
  }

  const stopCode = String(getRowValue(row, "userstopcode") || "").toLowerCase();
  return stopList.includes(stopCode);
}

function matchesLinePlanningNumber(row) {
  if (LINEPLANNING_PATTERNS.length === 0) {
    return true;
  }

  const line = String(getRowValue(row, "lineplanningnumber") || "").trim();
  if (!line) {
    return false;
  }

  const lower = line.toLowerCase();
  return LINEPLANNING_PATTERNS.some((pattern) => {
    if (pattern.type === "exact") {
      return lower === pattern.value;
    }
    return pattern.value.test(line);
  });
}

function matchesStopAndLine(row, stopList = USERSTOPCODES) {
  return matchesUserStopCode(row, stopList) && matchesLinePlanningNumber(row);
}

function filterBaseCommandsByStop(baseCommands) {
  if (USERSTOPCODES.length === 0 && LINEPLANNINGNUMBERS.length === 0) {
    return baseCommands;
  }

  return baseCommands.filter((command) => {
    const entityOk =
      matchesStopAndLine(command.entity) ||
      (matchesUserStopCode(command.entity) &&
        matchesLinePlanningNumber(command.data));
    const dataOk =
      matchesStopAndLine(command.data) ||
      (matchesUserStopCode(command.data) &&
        matchesLinePlanningNumber(command.entity));
    return entityOk || dataOk;
  });
}

function xmlContainsTrackedStop(xmlText, stopList = USERSTOPCODES) {
  if (stopList.length === 0) {
    return true;
  }

  const lowerText = String(xmlText || "").toLowerCase();
  return stopList.some((stopCode) => lowerText.includes(stopCode));
}

function getStopCodeFromCommand(commandMessage) {
  return String(
    commandMessage?.entity?.userstopcode ||
      commandMessage?.data?.userstopcode ||
      USERSTOPCODES[0] ||
      "",
  ).toLowerCase();
}

function getJourneyKeyFromCommand(commandMessage) {
  const entity = commandMessage?.entity || {};
  return [
    entity.dataownercode,
    entity.lineplanningnumber,
    entity.journeynumber,
    entity.vehiclenumber,
  ]
    .filter(Boolean)
    .join("|");
}

const stopStates = new Map();

function isTrainAtStation(stopCode) {
  const current = stopStates.get(stopCode);
  return (
    current?.command === "RET_TRAIN_ARRIVED" ||
    current?.command === "RET_TRAIN_ARRIVING_15S"
  );
}

function processStopCommand(commandMessage) {
  const stopCode = getStopCodeFromCommand(commandMessage);
  const journeyKey = getJourneyKeyFromCommand(commandMessage);
  const command = commandMessage.command;
  const current = stopStates.get(stopCode) || {
    command: null,
    journeyKey: null,
  };

  if (command === "RET_NO_TRAIN") {
    if (isTrainAtStation(stopCode)) {
      return {
        broadcast: false,
        scheduleDepartureTimer: false,
        clearDepartureTimer: false,
      };
    }

    stopStates.set(stopCode, { command, journeyKey: null });
    return { broadcast: true, scheduleDepartureTimer: false, clearDepartureTimer: true };
  }

  if (command === "RET_TRAIN_ARRIVING_15S") {
    if (
      current.command === "RET_TRAIN_ARRIVING_15S" &&
      journeyKey === current.journeyKey
    ) {
      return {
        broadcast: false,
        scheduleDepartureTimer: false,
        clearDepartureTimer: false,
      };
    }

    stopStates.set(stopCode, { command, journeyKey });
    return { broadcast: true, scheduleDepartureTimer: false, clearDepartureTimer: true };
  }

  if (command === "RET_TRAIN_ARRIVED") {
    if (
      journeyKey &&
      journeyKey === current.journeyKey &&
      (current.command === "RET_TRAIN_ARRIVED" ||
        current.command === "RET_TRAIN_DEPARTED")
    ) {
      return {
        broadcast: false,
        scheduleDepartureTimer: false,
        clearDepartureTimer: false,
      };
    }

    stopStates.set(stopCode, { command, journeyKey });
    return { broadcast: true, scheduleDepartureTimer: false, clearDepartureTimer: true };
  }

  if (command === "RET_TRAIN_DEPARTED") {
    const enteringDeparted = current.command !== "RET_TRAIN_DEPARTED";
    stopStates.set(stopCode, { command, journeyKey });
    return {
      broadcast: true,
      scheduleDepartureTimer: enteringDeparted,
      clearDepartureTimer: false,
    };
  }

  return { broadcast: true, scheduleDepartureTimer: false, clearDepartureTimer: false };
}

function buildTransitCommands(topic, parsedXml, receivedAt) {
  const keys = Object.keys(parsedXml || {});
  const rootName = keys.find((key) => !key.startsWith("?")) || keys[0];
  if (!rootName) {
    return [];
  }

  const root = parsedXml[rootName];
  const commandMessages = [];

  const protocolMaps = [
    { container: "KV6posinfo", prefix: "KV6" },
    { container: "KV17cvlinfo", prefix: "KV17" },
    { container: "KV15messages", prefix: "KV15" },
  ];

  for (const { container, prefix } of protocolMaps) {
    const protocolNode = root?.[container];
    if (!protocolNode || typeof protocolNode !== "object") {
      continue;
    }

    for (const [messageType, item] of Object.entries(protocolNode)) {
      if (messageType === "delimiter" || messageType.startsWith("@")) {
        continue;
      }

      for (const row of toArray(item)) {
        commandMessages.push({
          type: "transit-command",
          protocol: prefix,
          command: `${prefix}_${String(messageType).toUpperCase()}`,
          topic,
          receivedAt,
          entity: extractEntity(row),
          data: row,
        });
      }
    }
  }

  if (commandMessages.length > 0) {
    return commandMessages;
  }

  return [
    {
      type: "transit-command",
      protocol: rootName,
      command: "RAW_XML_MESSAGE",
      topic,
      receivedAt,
      data: root,
    },
  ];
}

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

function extractTurboPassTimeRows(decoded) {
  const text = decoded?.text || "";
  if (!text) {
    return [];
  }

  if (text.trimStart().startsWith("<")) {
    const parsedXml = xmlParser.parse(text);
    return extractDatedPassTimes(parsedXml);
  }

  if (text.startsWith("\\G") || text.includes("\\TDATEDPASSTIME")) {
    return parseCtxDatedPassTimes(text);
  }

  return [];
}

function extractDatedPassTimes(parsedXml) {
  const rows = [];

  function walk(node) {
    if (!node || typeof node !== "object") {
      return;
    }

    for (const [key, value] of Object.entries(node)) {
      if (key.toLowerCase() === "datedpasstime") {
        rows.push(...toArray(value));
        continue;
      }

      if (value && typeof value === "object") {
        walk(value);
      }
    }
  }

  walk(parsedXml);
  return rows;
}

function handleForecastUpdate(topic, row, receivedAt) {
  const stopCode = String(getRowValue(row, "userstopcode") || "").toLowerCase();
  if (!matchesStopAndLine(row, GOVI_USERSTOPCODES)) {
    return null;
  }

  const entity = extractEntity(row);
  const baseCommand = {
    type: "transit-command",
    protocol: "KV8",
    command: "KV8_FORECAST",
    topic,
    receivedAt,
    entity,
    data: row,
  };
  const journeyKey = getJourneyKeyFromCommand(baseCommand);
  const approachKey = stopCode && journeyKey ? `${stopCode}|${journeyKey}` : null;
  const tripStatus = asUpperString(getRowValue(row, "tripstopstatus"));
  const expectedArrivalMs = extractExpectedArrivalMs(row, receivedAt);
  const etaSeconds = Number.isFinite(expectedArrivalMs)
    ? Math.round((expectedArrivalMs - new Date(receivedAt).getTime()) / 1000)
    : extractEtaSeconds(row, receivedAt);

  const entry = {
    stopCode,
    journeyKey,
    approachKey,
    topic,
    receivedAt,
    updatedAtMs: Date.now(),
    tripStatus,
    etaSeconds: Number.isFinite(etaSeconds) ? etaSeconds : null,
    expectedArrivalMs,
    expectedArrivalTime: getRowValue(row, "expectedarrivaltime") || null,
    vehiclenumber: entity.vehiclenumber || null,
    lineplanningnumber: entity.lineplanningnumber || null,
    linepublicnumber: entity.linepublicnumber || null,
    linedirection: getRowValue(row, "linedirection") || null,
    destinationName:
      getRowValue(row, ["destinationname", "destinationname50"]) || null,
    journeynumber: entity.journeynumber || null,
    dataownercode: entity.dataownercode || null,
    entity,
    data: row,
    baseCommand,
  };

  upsertUpcomingForecast(entry);

  return {
    sourceCommand: "KV8_FORECAST",
    entity,
    data: row,
    tripStatus: tripStatus || null,
    etaSeconds: entry.etaSeconds,
    vehiclenumber: entry.vehiclenumber,
    journeyKey,
  };
}

function upsertUpcomingForecast(entry) {
  if (!entry?.stopCode || !entry?.journeyKey) {
    return;
  }

  let byJourney = upcomingByStop.get(entry.stopCode);
  if (!byJourney) {
    byJourney = new Map();
    upcomingByStop.set(entry.stopCode, byJourney);
  }

  const existing = byJourney.get(entry.journeyKey);

  // GOVI PASSED/ARRIVED must not remove a row we are tracking via RIG
  // (Arriving/Arrived/Departing) — keep it until NO_TRAIN or depart timeout.
  if (entry.tripStatus === "ARRIVED" || entry.tripStatus === "PASSED") {
    if (existing?.rigPinned) {
      existing.tripStatus = entry.tripStatus;
      existing.goviUpdatedAtMs = Date.now();
      byJourney.set(entry.journeyKey, existing);
      return;
    }
    // Still keep the row for the list (negative ETA allowed); don't delete.
  }

  byJourney.set(entry.journeyKey, {
    ...existing,
    ...entry,
    displayStatus: existing?.displayStatus || "Driving",
    rigPinned: Boolean(existing?.rigPinned),
    removeAtMs: existing?.removeAtMs || null,
    goviUpdatedAtMs: Date.now(),
  });
}

function liveEtaSeconds(entry, nowMs = Date.now()) {
  if (Number.isFinite(entry?.expectedArrivalMs)) {
    return Math.round((entry.expectedArrivalMs - nowMs) / 1000);
  }
  if (Number.isFinite(entry?.etaSeconds) && entry?.updatedAtMs) {
    const ageSec = Math.round((nowMs - entry.updatedAtMs) / 1000);
    return entry.etaSeconds - ageSec;
  }
  return null;
}

function isGoviFeedLive() {
  return Boolean(turboSubscribed);
}

function displayStatusForUpcoming(entry) {
  return entry?.displayStatus || "Driving";
}

function formatUpcomingLineLabel(lineplanningnumber, linepublicnumber) {
  const planning = String(lineplanningnumber || "").trim();
  const fromFeed = String(linepublicnumber || "").trim();
  const pub =
    fromFeed ||
    RET_BEURS_TRAM_PUBLIC[planning] ||
    RET_BEURS_TRAM_PUBLIC[planning.toLowerCase()] ||
    null;
  if (pub && planning) {
    return `${pub} (${planning})`;
  }
  return pub || planning || null;
}

function lifecycleSortRank(entry) {
  switch (displayStatusForUpcoming(entry)) {
    case "Departing":
      return 0;
    case "Arrived":
      return 1;
    case "Arriving":
      return 2;
    default:
      return 3;
  }
}

function shouldDropUpcomingEntry(entry, etaSeconds, nowMs = Date.now()) {
  if (entry?.removeAtMs && nowMs >= entry.removeAtMs) {
    return true;
  }

  // Pinned RIG lifecycle rows stay until NO_TRAIN / depart timeout.
  if (entry?.rigPinned) {
    return false;
  }

  if (Number.isFinite(etaSeconds) && etaSeconds < -UPCOMING_OVERDUE_KEEP_SEC) {
    return true;
  }

  const lastGovi = entry.goviUpdatedAtMs || entry.updatedAtMs || 0;
  if (isGoviFeedLive() && nowMs - lastGovi > UPCOMING_STALE_MS) {
    return true;
  }

  return false;
}

function getSortedUpcoming(stopCode, { includeOverdue = true } = {}) {
  const byJourney = upcomingByStop.get(stopCode);
  if (!byJourney) {
    return [];
  }

  const nowMs = Date.now();
  const goviLive = isGoviFeedLive();
  const list = [];

  for (const [journeyKey, entry] of byJourney.entries()) {
    const etaSeconds = liveEtaSeconds(entry, nowMs);
    if (shouldDropUpcomingEntry(entry, etaSeconds, nowMs)) {
      byJourney.delete(journeyKey);
      if (entry.approachKey) {
        clearArrivingTimer(entry.approachKey);
      }
      continue;
    }

    if (!Number.isFinite(etaSeconds) && !entry.rigPinned) {
      continue;
    }
    if (
      !includeOverdue &&
      Number.isFinite(etaSeconds) &&
      etaSeconds <= 0 &&
      !entry.rigPinned
    ) {
      continue;
    }

    list.push({
      ...entry,
      etaSeconds: Number.isFinite(etaSeconds) ? etaSeconds : 0,
      cached: !goviLive || nowMs - (entry.goviUpdatedAtMs || entry.updatedAtMs) > 5000,
      displayStatus: displayStatusForUpcoming(entry),
    });
  }

  list.sort((a, b) => {
    const lifeA = lifecycleSortRank(a);
    const lifeB = lifecycleSortRank(b);
    if (lifeA !== lifeB) {
      return lifeA - lifeB;
    }
    if (a.etaSeconds !== b.etaSeconds) {
      return a.etaSeconds - b.etaSeconds;
    }
    return String(a.vehiclenumber || "").localeCompare(
      String(b.vehiclenumber || ""),
    );
  });

  return list;
}

function getUpcomingVehiclesSnapshot() {
  const stops =
    GOVI_USERSTOPCODES.length > 0
      ? GOVI_USERSTOPCODES
      : USERSTOPCODES.length > 0
        ? USERSTOPCODES
        : [...upcomingByStop.keys()];
  const goviLive = isGoviFeedLive();

  const merged = [];
  for (const stopCode of stops) {
    merged.push(...getSortedUpcoming(stopCode, { includeOverdue: true }));
  }

  // Global order across all stops (same ranking as per-stop list).
  merged.sort((a, b) => {
    const lifeA = lifecycleSortRank(a);
    const lifeB = lifecycleSortRank(b);
    if (lifeA !== lifeB) {
      return lifeA - lifeB;
    }
    if (a.etaSeconds !== b.etaSeconds) {
      return a.etaSeconds - b.etaSeconds;
    }
    return String(a.vehiclenumber || "").localeCompare(
      String(b.vehiclenumber || ""),
    );
  });

  const limited = merged.slice(0, UPCOMING_DISPLAY_LIMIT);
  const nearestIndex = limited.findIndex(
    (entry) =>
      entry.etaSeconds > 0 && displayStatusForUpcoming(entry) === "Driving",
  );

  return limited.map((entry, index) => ({
    rank: index + 1,
    isNearest:
      index === nearestIndex ||
      (nearestIndex < 0 && index === 0 && entry.displayStatus === "Arriving"),
    cached: Boolean(entry.cached),
    goviLive,
    stopCode: entry.stopCode,
    vehiclenumber: entry.vehiclenumber,
    lineplanningnumber: entry.lineplanningnumber,
    linepublicnumber: entry.linepublicnumber || null,
    lineLabel: formatUpcomingLineLabel(
      entry.lineplanningnumber,
      entry.linepublicnumber,
    ),
    linedirection: entry.linedirection || null,
    destinationName: entry.destinationName || null,
    journeynumber: entry.journeynumber,
    journeyKey: entry.journeyKey,
    tripStatus: entry.tripStatus || null,
    status: entry.displayStatus || "Driving",
    etaSeconds: entry.etaSeconds,
    expectedArrivalTime: entry.expectedArrivalTime,
    updatedAt: new Date(entry.updatedAtMs).toISOString(),
  }));
}

function findUpcomingEntry(stopCode, journeyKey, vehiclenumber) {
  const byJourney = upcomingByStop.get(stopCode);
  if (!byJourney) {
    return null;
  }
  if (journeyKey && byJourney.has(journeyKey)) {
    return { key: journeyKey, entry: byJourney.get(journeyKey), byJourney };
  }
  if (vehiclenumber) {
    for (const [key, entry] of byJourney.entries()) {
      if (String(entry.vehiclenumber || "") === String(vehiclenumber)) {
        return { key, entry, byJourney };
      }
    }
  }
  return null;
}

function clearUpcomingForStop(stopCode) {
  const byJourney = upcomingByStop.get(stopCode);
  if (!byJourney) {
    return;
  }
  for (const entry of byJourney.values()) {
    if (entry.approachKey) {
      clearArrivingTimer(entry.approachKey);
      clearArrivedCountdown(entry.approachKey);
    }
  }
  upcomingByStop.delete(stopCode);
}

function updateUpcomingFromRetCommand(commandMessage) {
  const command = commandMessage?.command;
  const stopCode = getStopCodeFromCommand(commandMessage);
  if (!stopCode || !command) {
    return;
  }

  if (command === "RET_NO_TRAIN") {
    clearUpcomingForStop(stopCode);
    return;
  }

  let displayStatus = null;
  if (command === "RET_TRAIN_ARRIVING_15S") {
    displayStatus = "Arriving"; // Entering
  } else if (command === "RET_TRAIN_ARRIVED") {
    displayStatus = "Arrived"; // Parked
  } else if (command === "RET_TRAIN_DEPARTED") {
    displayStatus = "Departing"; // Leaving
  } else {
    return;
  }

  const journeyKey = getJourneyKeyFromCommand(commandMessage);
  const vehiclenumber =
    commandMessage?.entity?.vehiclenumber ||
    commandMessage?.data?.vehiclenumber ||
    null;

  let byJourney = upcomingByStop.get(stopCode);
  if (!byJourney) {
    byJourney = new Map();
    upcomingByStop.set(stopCode, byJourney);
  }

  const found = findUpcomingEntry(stopCode, journeyKey, vehiclenumber);
  const key =
    found?.key ||
    journeyKey ||
    `${stopCode}|${vehiclenumber || "unknown"}|${Date.now()}`;
  const existing = found?.entry || {};

  const next = {
    ...existing,
    stopCode,
    journeyKey: journeyKey || existing.journeyKey || key,
    approachKey: existing.approachKey || `${stopCode}|${key}`,
    topic: commandMessage.topic || existing.topic,
    receivedAt: commandMessage.receivedAt || existing.receivedAt,
    updatedAtMs: Date.now(),
    tripStatus: existing.tripStatus || null,
    etaSeconds: existing.etaSeconds ?? null,
    expectedArrivalMs: existing.expectedArrivalMs ?? null,
    expectedArrivalTime: existing.expectedArrivalTime || null,
    vehiclenumber: vehiclenumber || existing.vehiclenumber || null,
    lineplanningnumber:
      commandMessage?.entity?.lineplanningnumber ||
      existing.lineplanningnumber ||
      null,
    linepublicnumber:
      commandMessage?.entity?.linepublicnumber ||
      existing.linepublicnumber ||
      null,
    journeynumber:
      commandMessage?.entity?.journeynumber || existing.journeynumber || null,
    dataownercode:
      commandMessage?.entity?.dataownercode || existing.dataownercode || null,
    entity: commandMessage.entity || existing.entity || {},
    data: commandMessage.data || existing.data || {},
    baseCommand: existing.baseCommand || null,
    displayStatus,
    rigPinned: true,
    removeAtMs:
      displayStatus === "Departing"
        ? Date.now() + NO_TRAIN_AFTER_DEPARTURE_MS
        : null,
  };

  byJourney.set(key, next);
}

function stopIsLockedToTrain(stopCode) {
  const current = stopStates.get(stopCode);
  return (
    current?.command === "RET_TRAIN_ARRIVING_15S" ||
    current?.command === "RET_TRAIN_ARRIVED"
  );
}

function reconcileUpcomingArrivals(stopCode) {
  // Only Driving forecasts compete for the next ARRIVING trigger.
  const hunting = getSortedUpcoming(stopCode, { includeOverdue: false }).filter(
    (entry) => displayStatusForUpcoming(entry) === "Driving",
  );
  const nearest = hunting[0] || null;
  const byJourney = upcomingByStop.get(stopCode);

  if (byJourney) {
    for (const entry of byJourney.values()) {
      if (!nearest || entry.journeyKey !== nearest.journeyKey) {
        if (entry.approachKey && displayStatusForUpcoming(entry) === "Driving") {
          clearArrivingTimer(entry.approachKey);
        }
      }
    }
  }

  // Already committed to a train at/near the stop — keep ranking for UI only.
  if (stopIsLockedToTrain(stopCode)) {
    return hunting;
  }

  if (!nearest) {
    return hunting;
  }

  if (nearest.etaSeconds > ARRIVING_ETA_SEC) {
    scheduleArrivingFromEta(
      nearest.topic,
      nearest.baseCommand,
      nearest.etaSeconds,
    );
  } else if (nearest.etaSeconds > 0 && nearest.etaSeconds <= ARRIVING_ETA_SEC) {
    if (nearest.approachKey) {
      clearArrivingTimer(nearest.approachKey);
    }
    emitDerivedCommand({
      type: "transit-command",
      protocol: "RET",
      command: "RET_TRAIN_ARRIVING_15S",
      topic: nearest.topic,
      receivedAt: new Date().toISOString(),
      sourceCommand: "KV8_FORECAST_NEAREST",
      etaSeconds: nearest.etaSeconds,
      entity: nearest.entity,
      data: nearest.data,
    });
  }

  return hunting;
}

function clearArrivingTimer(approachKey) {
  const timer = arrivingTimers.get(approachKey);
  if (timer) {
    clearTimeout(timer);
    arrivingTimers.delete(approachKey);
  }
}

function clearArrivedCountdown(approachKey) {
  const timer = arrivedCountdownTimers.get(approachKey);
  if (timer) {
    clearTimeout(timer);
    arrivedCountdownTimers.delete(approachKey);
  }
}

function scheduleArrivingFromEta(topic, baseCommand, etaSeconds) {
  const stopCode = String(baseCommand.entity?.userstopcode || "").toLowerCase();
  const journeyKey = getJourneyKeyFromCommand(baseCommand);
  if (!stopCode || !journeyKey) {
    return;
  }

  const approachKey = `${stopCode}|${journeyKey}`;
  clearArrivingTimer(approachKey);

  // Fire when this journey's forecast ETA reaches the arriving threshold.
  const delayMs = Math.max(0, (etaSeconds - ARRIVING_ETA_SEC) * 1000);
  const timer = setTimeout(() => {
    arrivingTimers.delete(approachKey);

    // Only the current nearest Driving vehicle may transition to ARRIVING.
    const nearest = getSortedUpcoming(stopCode, { includeOverdue: false }).find(
      (entry) => displayStatusForUpcoming(entry) === "Driving",
    );
    if (!nearest || nearest.journeyKey !== journeyKey) {
      return;
    }
    if (stopIsLockedToTrain(stopCode)) {
      return;
    }

    const current = stopStates.get(stopCode);
    if (
      current?.journeyKey === journeyKey &&
      (current.command === "RET_TRAIN_ARRIVED" ||
        current.command === "RET_TRAIN_DEPARTED")
    ) {
      return;
    }

    emitDerivedCommand({
      type: "transit-command",
      protocol: "RET",
      command: "RET_TRAIN_ARRIVING_15S",
      topic,
      receivedAt: new Date().toISOString(),
      sourceCommand: "SCHEDULED_ETA_NEAREST",
      etaSeconds: ARRIVING_ETA_SEC,
      entity: baseCommand.entity,
      data: baseCommand.data,
    });
  }, delayMs);

  arrivingTimers.set(approachKey, timer);
}

function scheduleArrivedFromEta(arrivingCommand) {
  const stopCode = getStopCodeFromCommand(arrivingCommand);
  const journeyKey = getJourneyKeyFromCommand(arrivingCommand);
  if (!stopCode || !journeyKey) {
    return;
  }

  const approachKey = `${stopCode}|${journeyKey}`;
  clearArrivedCountdown(approachKey);

  const etaSeconds = Number(arrivingCommand.etaSeconds);
  const delayMs = Math.max(
    0,
    (Number.isFinite(etaSeconds) ? etaSeconds : ARRIVING_ETA_SEC) * 1000,
  );

  const timer = setTimeout(() => {
    arrivedCountdownTimers.delete(approachKey);
    const current = stopStates.get(stopCode);
    if (
      current?.journeyKey === journeyKey &&
      (current.command === "RET_TRAIN_ARRIVED" ||
        current.command === "RET_TRAIN_DEPARTED")
    ) {
      return;
    }

    emitDerivedCommand({
      type: "transit-command",
      protocol: "RET",
      command: "RET_TRAIN_ARRIVED",
      topic: arrivingCommand.topic,
      receivedAt: new Date().toISOString(),
      sourceCommand: "ETA_ARRIVAL_COUNTDOWN",
      etaSeconds: 0,
      entity: arrivingCommand.entity,
      data: arrivingCommand.data || {},
    });
  }, delayMs);

  arrivedCountdownTimers.set(approachKey, timer);
  console.log(
    `[RET] ARRIVING → countdown ${Math.round(delayMs / 1000)}s to ARRIVED (${journeyKey || stopCode})`,
  );
}

function emitDerivedCommand(commandMessage) {
  initialNoTrainSent = true;

  const { broadcast, scheduleDepartureTimer, clearDepartureTimer } =
    processStopCommand(commandMessage);

  if (!broadcast) {
    return;
  }

  if (clearDepartureTimer) {
    clearDepartureNoTrainTimer();
  }

  if (scheduleDepartureTimer) {
    scheduleDepartureNoTrain(commandMessage);
  }

  syncFeedsToCommand(commandMessage.command, `command:${commandMessage.command}`);

  if (commandMessage.command === "RET_TRAIN_ARRIVING_15S") {
    scheduleArrivedFromEta(commandMessage);
  }

  updateUpcomingFromRetCommand(commandMessage);

  broadcastControlJson(commandMessage);
  broadcastJson(commandMessage);
}

function clearDepartureNoTrainTimer() {
  if (departureNoTrainTimer) {
    clearTimeout(departureNoTrainTimer);
    departureNoTrainTimer = null;
  }
}

function scheduleDepartureNoTrain(departureCommand) {
  if (departureNoTrainTimer) {
    return;
  }

  departureNoTrainTimer = setTimeout(() => {
    departureNoTrainTimer = null;
    const stopCode = getStopCodeFromCommand(departureCommand);
    const current = stopStates.get(stopCode);
    if (current?.command !== "RET_TRAIN_DEPARTED") {
      return;
    }

    const noTrainMessage = {
      type: "transit-command",
      protocol: "RET",
      command: "RET_NO_TRAIN",
      topic: departureCommand.topic,
      receivedAt: new Date().toISOString(),
      sourceCommand: "DEPARTURE_TIMEOUT",
      entity: departureCommand.entity,
      data: {},
    };

    emitDerivedCommand(noTrainMessage);
    console.log(
      `[RET] No new train after departure — sent RET_NO_TRAIN (${NO_TRAIN_AFTER_DEPARTURE_MS}ms timeout)`,
    );
  }, NO_TRAIN_AFTER_DEPARTURE_MS);
}

function processBisonMessage(topic, decoded, receivedAt) {
  let commandMessages = [];
  let matchingRows = [];
  let parseError = null;

  if (decoded.text && decoded.text.trimStart().startsWith("<")) {
    try {
      const parsedXml = xmlParser.parse(decoded.text);
      const baseCommands = filterBaseCommandsByStop(
        buildTransitCommands(topic, parsedXml, receivedAt),
      );
      matchingRows = baseCommands.map((command) => ({
        sourceCommand: command.command,
        entity: command.entity,
        data: command.data,
      }));
      commandMessages = buildRotterdamTrainStateCommands(
        topic,
        baseCommands,
      ).filter(isAllowedRetCommand);
    } catch (error) {
      parseError = error?.message || String(error);
      commandMessages = [];
      matchingRows = [];
    }
  }

  return { commandMessages, matchingRows, parseError };
}

function processTurboMessage(topic, decoded, receivedAt) {
  let matchingRows = [];
  let parseError = null;

  try {
    matchingRows = extractTurboPassTimeRows(decoded)
      .map((row) => handleForecastUpdate(topic, row, receivedAt))
      .filter(Boolean);

    const touchedStops = new Set(
      matchingRows
        .map((row) =>
          String(row?.entity?.userstopcode || "").toLowerCase(),
        )
        .filter(Boolean),
    );

    for (const stopCode of touchedStops) {
      reconcileUpcomingArrivals(stopCode);
    }

    // First relevant GOVI forecast ends bootstrap dual-subscribe → GOVI hunting.
    if (!feedSwitchingActive && matchingRows.length > 0) {
      setFeedPhase("govi", "govi-forecast");
    }

    const nearestJourneyKeys = new Set();
    for (const stopCode of touchedStops) {
      const nearest = getSortedUpcoming(stopCode)[0];
      if (nearest?.journeyKey) {
        nearestJourneyKeys.add(nearest.journeyKey);
      }
    }

    matchingRows = matchingRows
      .map((row) => ({
        ...row,
        isNearest: nearestJourneyKeys.has(row.journeyKey),
      }))
      .sort((a, b) => {
        const etaA = Number.isFinite(a.etaSeconds) ? a.etaSeconds : Number.POSITIVE_INFINITY;
        const etaB = Number.isFinite(b.etaSeconds) ? b.etaSeconds : Number.POSITIVE_INFINITY;
        if (etaA !== etaB) {
          return etaA - etaB;
        }
        return String(a.entity?.vehiclenumber || "").localeCompare(
          String(b.entity?.vehiclenumber || ""),
        );
      });
  } catch (error) {
    parseError = error?.message || String(error);
    matchingRows = [];
  }

  return { matchingRows, parseError };
}

function recordInboundMessage(
  decoded,
  matchingRowCount,
  parseError,
  feed,
  payloadBytes = 0,
) {
  const stats =
    feed === "turbo" ? bridgeStatus.zmq.turbo : bridgeStatus.zmq;

  stats.totalMessages += 1;
  stats.lastMessageAt = new Date().toISOString();

  if (
    USERSTOPCODES.length > 0 &&
    (!decoded.text || !xmlContainsTrackedStop(decoded.text))
  ) {
    stats.ignoredMessages += 1;
    return false;
  }

  if (USERSTOPCODES.length > 0 && matchingRowCount === 0 && !parseError) {
    stats.ignoredMessages += 1;
    return false;
  }

  stats.relevantMessages += 1;
  stats.bytesRelevant += Number(payloadBytes) || 0;
  return true;
}

function subscribeBisonTopics(reason = "manual") {
  if (bisonSubscribed) {
    return false;
  }

  for (const topic of ZMQ_TOPICS) {
    subscriber.subscribe(topic);
  }

  bisonSubscribed = true;
  bridgeStatus.zmq.subscribed = true;
  bridgeStatus.zmq.state = "subscribed";
  bridgeStatus.zmq.lastSubscribeAt = new Date().toISOString();
  console.log(
    `[ZMQ/RIG] Subscribed topics: ${ZMQ_TOPICS.join(", ")} (${reason})`,
  );
  return true;
}

function unsubscribeBisonTopics(reason = "manual") {
  if (!bisonSubscribed) {
    return false;
  }

  for (const topic of ZMQ_TOPICS) {
    subscriber.unsubscribe(topic);
  }

  bisonSubscribed = false;
  bridgeStatus.zmq.subscribed = false;
  bridgeStatus.zmq.state = "standby";
  bridgeStatus.zmq.lastUnsubscribeAt = new Date().toISOString();
  console.log(`[ZMQ/RIG] Unsubscribed (${reason})`);
  return true;
}

function subscribeTurboTopics(reason = "manual") {
  if (!ZMQ_TURBO_ENABLED || ZMQ_TURBO_TOPICS.length === 0) {
    return false;
  }

  if (turboSubscribed) {
    return false;
  }

  for (const topic of ZMQ_TURBO_TOPICS) {
    turboSubscriber.subscribe(topic);
  }

  turboSubscribed = true;
  bridgeStatus.zmq.turbo.subscribed = true;
  bridgeStatus.zmq.turbo.state = "subscribed";
  bridgeStatus.zmq.turbo.lastSubscribeAt = new Date().toISOString();
  console.log(
    `[ZMQ/GOVI] Subscribed topics: ${ZMQ_TURBO_TOPICS.join(", ")} (${reason})`,
  );
  return true;
}

function unsubscribeTurboTopics(reason = "manual") {
  if (!turboSubscribed) {
    return false;
  }

  for (const topic of ZMQ_TURBO_TOPICS) {
    turboSubscriber.unsubscribe(topic);
  }

  turboSubscribed = false;
  bridgeStatus.zmq.turbo.subscribed = false;
  bridgeStatus.zmq.turbo.state = "standby";
  bridgeStatus.zmq.turbo.lastUnsubscribeAt = new Date().toISOString();
  console.log(`[ZMQ/GOVI] Unsubscribed (${reason})`);
  return true;
}

function enterBootstrapFeeds(reason = "startup") {
  feedSwitchingActive = false;
  feedPhase = "bootstrap";
  bridgeStatus.zmq.feedPhase = "bootstrap";
  bridgeStatus.zmq.feedSwitchingActive = false;
  subscribeBisonTopics(reason);
  if (ZMQ_TURBO_ENABLED) {
    subscribeTurboTopics(reason);
  }
  console.log(
    `[ZMQ] Bootstrap dual-subscribe (RIG + GOVI); phase switching starts on first train state (${reason})`,
  );
}

function setFeedPhase(phase, reason = "manual") {
  if (!["rig", "govi", "arriving"].includes(phase)) {
    return feedPhase;
  }

  // When phase switching is disabled, keep both feeds subscribed.
  if (!ZMQ_TURBO_ON_DEMAND || !ZMQ_TURBO_ENABLED) {
    subscribeBisonTopics(reason);
    if (ZMQ_TURBO_ENABLED) {
      subscribeTurboTopics(reason);
    }
    feedPhase = phase;
    bridgeStatus.zmq.feedPhase = phase;
    return feedPhase;
  }

  // Startup / initial NO_TRAIN: keep listening to both until a real train state.
  if (
    !feedSwitchingActive &&
    (reason === "startup" || reason === "initial-no-train")
  ) {
    enterBootstrapFeeds(reason);
    return feedPhase;
  }

  if (!feedSwitchingActive) {
    feedSwitchingActive = true;
    bridgeStatus.zmq.feedSwitchingActive = true;
    console.log(`[ZMQ] Leaving bootstrap — phase switching active (${reason})`);
  }

  if (phase === feedPhase) {
    // Still enforce the expected subscription pair in case of drift.
  } else {
    console.log(`[ZMQ] Feed phase ${feedPhase} → ${phase} (${reason})`);
  }

  feedPhase = phase;
  bridgeStatus.zmq.feedPhase = phase;

  if (phase === "rig") {
    subscribeBisonTopics(reason);
    unsubscribeTurboTopics(reason);
  } else if (phase === "govi") {
    unsubscribeBisonTopics(reason);
    subscribeTurboTopics(reason);
  } else if (phase === "arriving") {
    unsubscribeBisonTopics(reason);
    unsubscribeTurboTopics(reason);
  }

  return feedPhase;
}

function syncFeedsToCommand(command, reason = "command") {
  // Initial NO_TRAIN must not end bootstrap dual-subscribe.
  if (
    command === "RET_NO_TRAIN" &&
    !feedSwitchingActive &&
    String(reason).includes("initial-no-train")
  ) {
    enterBootstrapFeeds("initial-no-train");
    return;
  }

  if (command === "RET_NO_TRAIN" || command === "RET_TRAIN_ARRIVED") {
    setFeedPhase("rig", reason);
    return;
  }

  if (command === "RET_TRAIN_DEPARTED") {
    setFeedPhase("govi", reason);
    return;
  }

  if (command === "RET_TRAIN_ARRIVING_15S") {
    setFeedPhase("arriving", reason);
  }
}

function maybeActivateGoviForOnRoute(baseCommand) {
  if (!ZMQ_TURBO_ENABLED || !ZMQ_TURBO_ON_DEMAND) {
    return;
  }

  // Do not leave RIG while a train is at the stop (need DEPARTURE) or while
  // ARRIVING countdown owns the next ARRIVED transition.
  for (const state of stopStates.values()) {
    if (
      state?.command === "RET_TRAIN_ARRIVED" ||
      state?.command === "RET_TRAIN_ARRIVING_15S"
    ) {
      return;
    }
  }

  const vehicle =
    baseCommand?.entity?.vehiclenumber ||
    baseCommand?.data?.vehiclenumber ||
    "?";
  setFeedPhase("govi", `onroute:${vehicle}`);
}

async function startZmqBridge() {
  subscriber.connect(ZMQ_ENDPOINT);
  bridgeStatus.zmq.connectedAt = new Date().toISOString();
  console.log(`[ZMQ/RIG] Connected to ${ZMQ_ENDPOINT}`);

  // Beginning only: listen to both; switch after first real train state.
  enterBootstrapFeeds("startup");

  if (USERSTOPCODES.length > 0) {
    console.log(
      `[ZMQ] Filtering RIG activity to userstopcodes: ${USERSTOPCODES.join(", ")}`,
    );
  }
  if (GOVI_USERSTOPCODES.length > 0) {
    console.log(
      `[ZMQ] Filtering GOVI activity to userstopcodes: ${GOVI_USERSTOPCODES.join(", ")}`,
    );
  }
  if (LINEPLANNINGNUMBERS.length > 0) {
    console.log(
      `[ZMQ] Filtering activity to lineplanningnumbers: ${LINEPLANNINGNUMBERS.join(", ")}`,
    );
  } else {
    console.log(`[ZMQ] Line filter off (all LinePlanningNumbers at stop filter)`);
  }

  for await (const msg of subscriber) {
    if (!Array.isArray(msg) || msg.length === 0) {
      continue;
    }

    const [topicFrame, ...payloadFrames] = msg;
    const topic = topicFrame.toString("utf8");

    const payloadBuffer = payloadFrames.length
      ? Buffer.concat(payloadFrames)
      : Buffer.alloc(0);

    bridgeStatus.zmq.bytesReceived += payloadBuffer.length;

    if (payloadBuffer.length > MAX_PAYLOAD_BYTES) {
      const droppedAt = new Date().toISOString();
      bridgeStatus.zmq.droppedMessages += 1;
      bridgeStatus.zmq.totalMessages += 1;
      bridgeStatus.zmq.lastMessageAt = droppedAt;
      continue;
    }

    const receivedAt = new Date().toISOString();
    let decoded;
    try {
      decoded = detectAndDecodePayload(payloadBuffer);
    } catch {
      bridgeStatus.zmq.droppedMessages += 1;
      bridgeStatus.zmq.totalMessages += 1;
      bridgeStatus.zmq.lastMessageAt = receivedAt;
      continue;
    }

    // Early reject: skip XML parse when stop filter is not present.
    if (
      USERSTOPCODES.length > 0 &&
      (!decoded.text || !xmlContainsTrackedStop(decoded.text))
    ) {
      bridgeStatus.zmq.totalMessages += 1;
      bridgeStatus.zmq.ignoredMessages += 1;
      bridgeStatus.zmq.lastMessageAt = receivedAt;
      continue;
    }

    const { commandMessages, matchingRows, parseError } = processBisonMessage(
      topic,
      decoded,
      receivedAt,
    );

    if (
      !recordInboundMessage(
        decoded,
        matchingRows.length,
        parseError,
        "bison",
        payloadBuffer.length,
      )
    ) {
      continue;
    }

    broadcastControlJson({
      type: "transit-update",
      source: "bison",
      topic,
      encoding: decoded.encoding,
      commandCount: commandMessages.length,
      matchingRowCount: matchingRows.length,
      userstopcodes: USERSTOPCODES,
      payloadBytes: payloadBuffer.length,
      matchingRows,
      payloadPreview:
        matchingRows.length > 0
          ? JSON.stringify(matchingRows, null, 2).slice(0, 2000)
          : decoded.text
            ? decoded.text.slice(0, 1000)
            : null,
      parseError,
      receivedAt,
    });

    for (const commandMessage of commandMessages) {
      emitDerivedCommand(commandMessage);
    }
  }
}

async function startTurboBridge() {
  if (!ZMQ_TURBO_ENABLED || ZMQ_TURBO_TOPICS.length === 0) {
    return;
  }

  turboSubscriber.connect(ZMQ_TURBO_ENDPOINT);
  bridgeStatus.zmq.turbo.connectedAt = new Date().toISOString();
  console.log(`[ZMQ/GOVI] Connected to ${ZMQ_TURBO_ENDPOINT}`);

  if (ZMQ_TURBO_ON_DEMAND) {
    // Turbo topics are subscribed during enterBootstrapFeeds() from RIG startup.
    console.log(
      `[ZMQ/GOVI] Bootstrap + phase switching (both at start; then rig|govi|arriving)`,
    );
  } else {
    subscribeTurboTopics("startup");
  }

  for await (const msg of turboSubscriber) {
    if (!Array.isArray(msg) || msg.length === 0) {
      continue;
    }

    const [topicFrame, ...payloadFrames] = msg;
    const topic = topicFrame.toString("utf8");
    const payloadBuffer = payloadFrames.length
      ? Buffer.concat(payloadFrames)
      : Buffer.alloc(0);

    bridgeStatus.zmq.turbo.bytesReceived += payloadBuffer.length;

    if (payloadBuffer.length > MAX_PAYLOAD_BYTES) {
      bridgeStatus.zmq.turbo.droppedMessages += 1;
      bridgeStatus.zmq.turbo.totalMessages += 1;
      bridgeStatus.zmq.turbo.lastMessageAt = new Date().toISOString();
      continue;
    }

    const receivedAt = new Date().toISOString();
    let decoded;
    try {
      decoded = detectAndDecodePayload(payloadBuffer);
    } catch {
      bridgeStatus.zmq.turbo.droppedMessages += 1;
      bridgeStatus.zmq.turbo.totalMessages += 1;
      bridgeStatus.zmq.turbo.lastMessageAt = receivedAt;
      continue;
    }

    // Early reject: skip CTX/XML parse when GOVI stop filter is not present.
    if (
      GOVI_USERSTOPCODES.length > 0 &&
      (!decoded.text || !xmlContainsTrackedStop(decoded.text, GOVI_USERSTOPCODES))
    ) {
      bridgeStatus.zmq.turbo.totalMessages += 1;
      bridgeStatus.zmq.turbo.ignoredMessages += 1;
      bridgeStatus.zmq.turbo.lastMessageAt = receivedAt;
      continue;
    }

    const { matchingRows, parseError } = processTurboMessage(
      topic,
      decoded,
      receivedAt,
    );

    if (
      !recordInboundMessage(
        decoded,
        matchingRows.length,
        parseError,
        "turbo",
        payloadBuffer.length,
      )
    ) {
      continue;
    }

    broadcastControlJson({
      type: "transit-update",
      source: "kv78turbo",
      topic,
      encoding: decoded.encoding,
      format: decoded.text?.trimStart().startsWith("<") ? "xml" : "ctx",
      matchingRowCount: matchingRows.length,
      userstopcodes: GOVI_USERSTOPCODES,
      payloadBytes: payloadBuffer.length,
      matchingRows,
      payloadPreview: JSON.stringify(matchingRows, null, 2).slice(0, 2000),
      parseError,
      receivedAt,
    });
  }
}

function sendInitialNoTrain() {
  if (initialNoTrainSent || USERSTOPCODES.length === 0) {
    return;
  }

  if (USERSTOPCODES.some((stopCode) => isTrainAtStation(stopCode))) {
    initialNoTrainSent = true;
    return;
  }

  initialNoTrainSent = true;
  for (const stopCode of USERSTOPCODES) {
    stopStates.set(stopCode, { command: "RET_NO_TRAIN", journeyKey: null });
  }

  const noTrainMessage = {
    type: "transit-command",
    protocol: "RET",
    command: "RET_NO_TRAIN",
    topic: ROTTERDAM_TOPIC_PREFIX,
    receivedAt: new Date().toISOString(),
    sourceCommand: "INITIAL_TIMEOUT",
    entity: { userstopcode: USERSTOPCODES[0] },
    data: {},
  };

  broadcastControlJson(noTrainMessage);
  broadcastJson(noTrainMessage);
  for (const stopCode of USERSTOPCODES) {
    clearUpcomingForStop(stopCode);
  }
  // Keep both feeds until the first real train state (ARRIVED / DEPARTED / ARRIVING / ONROUTE).
  enterBootstrapFeeds("initial-no-train");
  console.log(
    `[RET] No train info received for ${USERSTOPCODES.join(",")} within ${NO_TRAIN_INITIAL_DELAY_MS}ms — sent RET_NO_TRAIN`,
  );
}

server.listen(PORT, () => {
  console.log(`[HTTP/WS] Server listening on http://localhost:${PORT}`);
  setTimeout(sendInitialNoTrain, NO_TRAIN_INITIAL_DELAY_MS);

  // Keep ETA countdowns / nearest ARRIVING working from cache while GOVI is standby.
  setInterval(() => {
    const stops =
      GOVI_USERSTOPCODES.length > 0
        ? GOVI_USERSTOPCODES
        : USERSTOPCODES.length > 0
          ? USERSTOPCODES
          : [...upcomingByStop.keys()];
    for (const stopCode of stops) {
      if (!upcomingByStop.has(stopCode)) {
        continue;
      }
      reconcileUpcomingArrivals(stopCode);
    }
  }, 1000);
});

startZmqBridge().catch((error) => {
  bridgeStatus.zmq.state = "error";
  bridgeStatus.zmq.lastError = {
    message: error?.message || String(error),
    at: new Date().toISOString(),
  };
  console.error("[ZMQ] Bridge failed:", error);
  process.exitCode = 1;
});

startTurboBridge().catch((error) => {
  bridgeStatus.zmq.turbo.state = "error";
  bridgeStatus.zmq.turbo.lastError = {
    message: error?.message || String(error),
    at: new Date().toISOString(),
  };
  console.error("[ZMQ/TURBO] Bridge failed:", error);
});
