const path = require("path");
const http = require("http");
const crypto = require("crypto");
const zlib = require("zlib");
const express = require("express");
const WebSocket = require("ws");
const zmq = require("zeromq");
const { XMLParser } = require("fast-xml-parser");
require("dotenv").config();

const PORT = Number(process.env.PORT || 8080);
const ZMQ_ENDPOINT =
  process.env.ZMQ_ENDPOINT || "tcp://pubsub.besteffort.ndovloket.nl:7658";
const ZMQ_TOPICS = (
  process.env.ZMQ_TOPICS || "/RIG/KV15messages,/RIG/KV17cvlinfo,/RIG/KV6posinfo"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const MAX_PAYLOAD_BYTES = Number(process.env.MAX_PAYLOAD_BYTES || 200000);
const WS_CHANNEL_CONTROL = "control";
const WS_CHANNEL_CONTENT = "content";
const ROTTERDAM_TOPIC_PREFIX = process.env.ROTTERDAM_TOPIC_PREFIX || "/RIG/";
const ALLOWED_RET_COMMANDS = new Set([
  "RET_NO_TRAIN",
  "RET_TRAIN_ARRIVING_15S",
  "RET_TRAIN_ARRIVED",
]);

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
  trimValues: true,
  parseTagValue: false,
});

const subscriber = new zmq.Subscriber();
const wsClientMeta = new Map();

const bridgeStatus = {
  startedAt: new Date().toISOString(),
  zmq: {
    endpoint: ZMQ_ENDPOINT,
    topics: ZMQ_TOPICS,
    state: "initializing",
    connectedAt: null,
    lastMessageAt: null,
    totalMessages: 0,
    droppedMessages: 0,
    lastError: null,
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
    zmq: {
      ...bridgeStatus.zmq,
      secondsSinceLastMessage: secondsSince(bridgeStatus.zmq.lastMessageAt),
      secondsSinceConnected: secondsSince(bridgeStatus.zmq.connectedAt),
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
    dataownercode: node.dataownercode || null,
    lineplanningnumber: node.lineplanningnumber || null,
    operatingday: node.operatingday || null,
    journeynumber: node.journeynumber || null,
    reinforcementnumber: node.reinforcementnumber || null,
    userstopcode: node.userstopcode || null,
    passagesequencenumber: node.passagesequencenumber || null,
    vehiclenumber: node.vehiclenumber || null,
    timestamp: node.timestamp || null,
  };
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

  const numericCandidates = [
    "secondsleft",
    "remainingseconds",
    "timetostop",
    "punctuality",
  ];

  for (const field of numericCandidates) {
    const value = row[field];
    if (value === undefined || value === null || value === "") {
      continue;
    }

    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      return Math.round(numeric);
    }
  }

  const timeCandidates = [
    "expectedarrivaltime",
    "targetarrivaltime",
    "actualarrivaltime",
    "arrivaltime",
  ];

  for (const field of timeCandidates) {
    const arrivalDate = toDateOnDay(
      row[field],
      row.operatingday || row.timestamp,
      receivedAt,
    );
    if (!arrivalDate) {
      continue;
    }

    const eta = Math.round(
      (arrivalDate.getTime() - new Date(receivedAt).getTime()) / 1000,
    );
    return eta;
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

    if (containsNoTrainSignal(row, baseCommand)) {
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

    const isArrivalProgress =
      (protocol === "KV6" || protocol === "KV17") &&
      (command.includes("ARRIVAL") || command.includes("ONROUTE"));

    if (
      isArrivalProgress &&
      Number.isFinite(etaSeconds) &&
      etaSeconds >= 0 &&
      etaSeconds <= 15
    ) {
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

    const isArrived =
      command.includes("ONSTOP") ||
      command.includes("END") ||
      (command.includes("ARRIVAL") &&
        Number.isFinite(etaSeconds) &&
        etaSeconds <= 0);

    if (isArrived) {
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

async function startZmqBridge() {
  for (const topic of ZMQ_TOPICS) {
    subscriber.subscribe(topic);
  }

  subscriber.connect(ZMQ_ENDPOINT);
  bridgeStatus.zmq.state = "connected";
  bridgeStatus.zmq.connectedAt = new Date().toISOString();
  console.log(`[ZMQ] Connected to ${ZMQ_ENDPOINT}`);
  console.log(`[ZMQ] Subscribed topics: ${ZMQ_TOPICS.join(", ")}`);

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

      broadcastControlJson({
        type: "transit-update",
        topic,
        dropped: true,
        reason: `Payload exceeded MAX_PAYLOAD_BYTES (${MAX_PAYLOAD_BYTES})`,
        payloadBytes: payloadBuffer.length,
        receivedAt: droppedAt,
      });
      continue;
    }

    const receivedAt = new Date().toISOString();
    let decoded;
    try {
      decoded = detectAndDecodePayload(payloadBuffer);
    } catch (decodeError) {
      bridgeStatus.zmq.droppedMessages += 1;
      bridgeStatus.zmq.totalMessages += 1;
      bridgeStatus.zmq.lastMessageAt = receivedAt;

      broadcastControlJson({
        type: "transit-update",
        topic,
        dropped: true,
        reason: `Decode failed: ${decodeError.message}`,
        payloadBytes: payloadBuffer.length,
        receivedAt,
      });
      continue;
    }

    let commandMessages = [];
    let parseError = null;

    if (decoded.text && decoded.text.trimStart().startsWith("<")) {
      try {
        const parsedXml = xmlParser.parse(decoded.text);
        const baseCommands = buildTransitCommands(topic, parsedXml, receivedAt);
        commandMessages = buildRotterdamTrainStateCommands(
          topic,
          baseCommands,
        ).filter(isAllowedRetCommand);
      } catch (error) {
        parseError = error?.message || String(error);
        commandMessages = [];
      }
    }

    bridgeStatus.zmq.totalMessages += 1;
    bridgeStatus.zmq.lastMessageAt = receivedAt;

    // Full TCP telemetry goes to control clients for monitoring.
    broadcastControlJson({
      type: "transit-update",
      topic,
      encoding: decoded.encoding,
      commandCount: commandMessages.length,
      payloadBytes: payloadBuffer.length,
      payloadText: decoded.text,
      payloadBase64: payloadBuffer.toString("base64"),
      payloadPreview: decoded.text ? decoded.text.slice(0, 1000) : null,
      parseError,
      receivedAt,
    });

    for (const commandMessage of commandMessages) {
      // Mirror the exact command payload to control clients for dashboard visibility.
      broadcastControlJson(commandMessage);
      broadcastJson(commandMessage);
    }
  }
}

server.listen(PORT, () => {
  console.log(`[HTTP/WS] Server listening on http://localhost:${PORT}`);
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
