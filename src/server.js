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
const MAX_PAYLOAD_BYTES = Number(process.env.MAX_PAYLOAD_BYTES || 200000);
const WS_CHANNEL_CONTROL = "control";
const WS_CHANNEL_CONTENT = "content";
const ROTTERDAM_TOPIC_PREFIX = process.env.ROTTERDAM_TOPIC_PREFIX || "/RIG/";
const USERSTOPCODES = envList("USERSTOPCODES").map((code) => code.toLowerCase());
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
const arrivingTimers = new Map();

const app = express();
app.use(express.static(path.join(__dirname, "..", "public")));
app.get("/status", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "status.html"));
});
app.get("/dashboard/status", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "status.html"));
});

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

const bridgeStatus = {
  startedAt: new Date().toISOString(),
  zmq: {
    endpoint: ZMQ_ENDPOINT,
    topics: ZMQ_TOPICS,
    state: "initializing",
    connectedAt: null,
    turbo: {
      enabled: ZMQ_TURBO_ENABLED,
      endpoint: ZMQ_TURBO_ENDPOINT,
      topics: ZMQ_TURBO_TOPICS,
      state: ZMQ_TURBO_ENABLED ? "initializing" : "disabled",
      connectedAt: null,
      lastMessageAt: null,
      totalMessages: 0,
      droppedMessages: 0,
      ignoredMessages: 0,
      relevantMessages: 0,
      lastError: null,
    },
    lastMessageAt: null,
    totalMessages: 0,
    droppedMessages: 0,
    ignoredMessages: 0,
    relevantMessages: 0,
    lastError: null,
    userstopcodes: USERSTOPCODES,
  },
  config: {
    rotterdamTopicPrefix: ROTTERDAM_TOPIC_PREFIX,
    noTrainInitialDelayMs: NO_TRAIN_INITIAL_DELAY_MS,
    noTrainAfterDepartureMs: NO_TRAIN_AFTER_DEPARTURE_MS,
    maxPayloadBytes: MAX_PAYLOAD_BYTES,
    wsPingIntervalMs: WS_PING_INTERVAL_MS,
    turboEnabled: ZMQ_TURBO_ENABLED,
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

function sendWsMessage(client, message) {
  if (client.readyState !== WebSocket.OPEN) {
    return false;
  }

  client.send(message);

  const meta = wsClientMeta.get(client);
  if (meta) {
    meta.lastSentAt = new Date().toISOString();
    meta.sentCount = (meta.sentCount || 0) + 1;
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
    clients,
    contentClients,
    controlClients,
    unknownClients,
  };
}

function getStatusPayload() {
  return {
    service: "transit-tcp-ws-server",
    now: new Date().toISOString(),
    uptimeSeconds: Math.floor(process.uptime()),
    startedAt: bridgeStatus.startedAt,
    config: bridgeStatus.config,
    zmq: {
      ...bridgeStatus.zmq,
      secondsSinceLastMessage: secondsSince(bridgeStatus.zmq.lastMessageAt),
      secondsSinceConnected: secondsSince(bridgeStatus.zmq.connectedAt),
      turbo: {
        ...bridgeStatus.zmq.turbo,
        secondsSinceConnected: secondsSince(
          bridgeStatus.zmq.turbo.connectedAt,
        ),
        secondsSinceLastMessage: secondsSince(
          bridgeStatus.zmq.turbo.lastMessageAt,
        ),
      },
    },
    websocket: summarizeWsClients(),
  };
}

app.get("/api/status", (req, res) => {
  res.json(getStatusPayload());
});

function broadcastJson(obj) {
  const message = JSON.stringify(obj);
  const contentRecipients = broadcastChannelMessage(
    WS_CHANNEL_CONTENT,
    message,
  );

  if (obj?.type !== "ws-event") {
    emitWsEvent("broadcast", {
      channel: WS_CHANNEL_CONTENT,
      messageType: obj?.type || "unknown",
      recipients: contentRecipients,
      totalRecipients: contentRecipients,
      payload: obj,
    });
  }
}

function broadcastControlJson(obj) {
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
    remoteAddress,
  });

  console.log(`[WS] Client connected (${channel})`);

  socket.send(
    JSON.stringify({
      type: "bridge-status",
      status: "connected",
      channel,
      endpoint: ZMQ_ENDPOINT,
      topics: ZMQ_TOPICS,
      clientId,
      serverTime: new Date().toISOString(),
    }),
  );

  if (isContentChannel(channel)) {
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
  for (const client of wss.clients) {
    if (!client.isAlive) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    client.ping();
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

function toDateOnDay(timeValue, dayValue, fallbackIso) {
  if (!timeValue || typeof timeValue !== "string") {
    return null;
  }

  // Handle fully qualified date-times first.
  const direct = new Date(timeValue);
  if (!Number.isNaN(direct.getTime())) {
    return direct;
  }

  // Accept HH:mm[:ss] values by anchoring them to operation/reception date.
  const match = timeValue.match(/^(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) {
    return null;
  }

  const base = new Date(dayValue || fallbackIso);
  if (Number.isNaN(base.getTime())) {
    return null;
  }

  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3] || 0);
  base.setHours(hours, minutes, seconds, 0);
  return base;
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

    if (
      isApproachUpdate &&
      Number.isFinite(etaSeconds) &&
      etaSeconds > 15
    ) {
      scheduleArriving15s(topic, baseCommand, etaSeconds);
      continue;
    }

    if (
      isApproachUpdate &&
      Number.isFinite(etaSeconds) &&
      etaSeconds > 0 &&
      etaSeconds <= 15
    ) {
      if (approachKey) {
        clearArrivingTimer(approachKey);
      }
      stateCommands.push({
        type: "transit-command",
        protocol: "RET",
        command: "RET_TRAIN_ARRIVING_15S",
        topic,
        receivedAt: baseCommand.receivedAt,
        sourceCommand: baseCommand.command,
        etaSeconds,
        entity: baseCommand.entity,
        data: row,
      });
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

function matchesUserStopCode(row) {
  if (USERSTOPCODES.length === 0) {
    return true;
  }

  const stopCode = String(getRowValue(row, "userstopcode") || "").toLowerCase();
  return USERSTOPCODES.includes(stopCode);
}

function filterBaseCommandsByStop(baseCommands) {
  if (USERSTOPCODES.length === 0) {
    return baseCommands;
  }

  return baseCommands.filter((command) => {
    return (
      matchesUserStopCode(command.entity) || matchesUserStopCode(command.data)
    );
  });
}

function xmlContainsTrackedStop(xmlText) {
  if (USERSTOPCODES.length === 0) {
    return true;
  }

  const lowerText = String(xmlText || "").toLowerCase();
  return USERSTOPCODES.some((stopCode) => lowerText.includes(stopCode));
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
      current.command === "RET_TRAIN_DEPARTED" &&
      journeyKey === current.journeyKey
    ) {
      return { broadcast: false, scheduleDepartureTimer: false, clearDepartureTimer: false };
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
  if (!matchesUserStopCode({ userstopcode: stopCode })) {
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

  if (tripStatus === "ARRIVED" || tripStatus === "PASSED") {
    if (approachKey) {
      clearArrivingTimer(approachKey);
    }
    return {
      sourceCommand: "KV8_FORECAST",
      entity,
      data: row,
      tripStatus,
    };
  }

  const etaSeconds = extractEtaSeconds(row, receivedAt);
  if (!Number.isFinite(etaSeconds)) {
    return {
      sourceCommand: "KV8_FORECAST",
      entity,
      data: row,
      etaSeconds: null,
    };
  }

  if (etaSeconds > 15) {
    scheduleArriving15s(topic, baseCommand, etaSeconds);
  } else if (etaSeconds > 0 && etaSeconds <= 15) {
    if (approachKey) {
      clearArrivingTimer(approachKey);
    }
    emitDerivedCommand({
      type: "transit-command",
      protocol: "RET",
      command: "RET_TRAIN_ARRIVING_15S",
      topic,
      receivedAt,
      sourceCommand: "KV8_FORECAST",
      etaSeconds,
      entity,
      data: row,
    });
  }

  return {
    sourceCommand: "KV8_FORECAST",
    entity,
    data: row,
    etaSeconds,
  };
}

function clearArrivingTimer(approachKey) {
  const timer = arrivingTimers.get(approachKey);
  if (timer) {
    clearTimeout(timer);
    arrivingTimers.delete(approachKey);
  }
}

function scheduleArriving15s(topic, baseCommand, etaSeconds) {
  const stopCode = String(baseCommand.entity?.userstopcode || "").toLowerCase();
  const journeyKey = getJourneyKeyFromCommand(baseCommand);
  if (!stopCode || !journeyKey) {
    return;
  }

  const approachKey = `${stopCode}|${journeyKey}`;
  clearArrivingTimer(approachKey);

  const delayMs = Math.max(0, (etaSeconds - 15) * 1000);
  const timer = setTimeout(() => {
    arrivingTimers.delete(approachKey);
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
      sourceCommand: "SCHEDULED_ETA",
      etaSeconds: 15,
      entity: baseCommand.entity,
      data: baseCommand.data,
    });
  }, delayMs);

  arrivingTimers.set(approachKey, timer);
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
  } catch (error) {
    parseError = error?.message || String(error);
    matchingRows = [];
  }

  return { matchingRows, parseError };
}

function recordInboundMessage(decoded, matchingRowCount, parseError, feed) {
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
  return true;
}

async function startZmqBridge() {
  for (const topic of ZMQ_TOPICS) {
    subscriber.subscribe(topic);
  }

  subscriber.connect(ZMQ_ENDPOINT);
  bridgeStatus.zmq.state = "connected";
  bridgeStatus.zmq.connectedAt = new Date().toISOString();
  console.log(`[ZMQ] Connected to ${ZMQ_ENDPOINT}`);
  console.log(`[ZMQ] Subscribed topics: ${ZMQ_TOPICS.join(", ")}`);
  if (USERSTOPCODES.length > 0) {
    console.log(
      `[ZMQ] Filtering activity to userstopcodes: ${USERSTOPCODES.join(", ")}`,
    );
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

  for (const topic of ZMQ_TURBO_TOPICS) {
    turboSubscriber.subscribe(topic);
  }

  turboSubscriber.connect(ZMQ_TURBO_ENDPOINT);
  bridgeStatus.zmq.turbo.state = "connected";
  bridgeStatus.zmq.turbo.connectedAt = new Date().toISOString();
  console.log(`[ZMQ/TURBO] Connected to ${ZMQ_TURBO_ENDPOINT}`);
  console.log(`[ZMQ/TURBO] Subscribed topics: ${ZMQ_TURBO_TOPICS.join(", ")}`);

  for await (const msg of turboSubscriber) {
    if (!Array.isArray(msg) || msg.length === 0) {
      continue;
    }

    const [topicFrame, ...payloadFrames] = msg;
    const topic = topicFrame.toString("utf8");
    const payloadBuffer = payloadFrames.length
      ? Buffer.concat(payloadFrames)
      : Buffer.alloc(0);

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
      userstopcodes: USERSTOPCODES,
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
  console.log(
    `[RET] No train info received for ${USERSTOPCODES.join(",")} within ${NO_TRAIN_INITIAL_DELAY_MS}ms — sent RET_NO_TRAIN`,
  );
}

server.listen(PORT, () => {
  console.log(`[HTTP/WS] Server listening on http://localhost:${PORT}`);
  setTimeout(sendInitialNoTrain, NO_TRAIN_INITIAL_DELAY_MS);
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
