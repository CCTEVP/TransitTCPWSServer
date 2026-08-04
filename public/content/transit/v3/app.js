const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/content`;
const RECONNECT_DELAY_MS = 3000;
const HEARTBEAT_TIMEOUT_MS = 35000;
const LAST_COMMAND_KEY = "transitDisplay.lastCommand";

const commandVideoMap = {
  RET_NO_TRAIN: "./videos/no-train_1080x1920.mp4",
  RET_TRAIN_ARRIVING_15S: "./videos/train-arriving_1080x1920.mp4",
  RET_TRAIN_ARRIVED: "./videos/train-arrived_1080x1920.mp4",
  RET_TRAIN_DEPARTED: "./videos/train-departed_1080x1920.mp4",
};

const videoA = document.getElementById("videoA");
const videoB = document.getElementById("videoB");
const connectionDot = document.getElementById("connectionDot");

const videos = [videoA, videoB];
let activeVideo = videoA;
let inactiveVideo = videoB;

let socket = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let isPlaying = false;
let currentCommand = null;
let queuedCommand = null;
let isInitialNoTrain = true;
let pendingPlayback = null;

for (const video of videos) {
  video.muted = true;
  video.playsInline = true;
}

function setConnectionState(connected) {
  connectionDot.className = connected
    ? "connection-dot connected"
    : "connection-dot disconnected";
}

function isKnownCommand(command) {
  return Boolean(commandVideoMap[command]);
}

function loadLastCommand() {
  try {
    const command = localStorage.getItem(LAST_COMMAND_KEY);
    return isKnownCommand(command) ? command : "RET_NO_TRAIN";
  } catch {
    return "RET_NO_TRAIN";
  }
}

function saveLastCommand(command) {
  if (!isKnownCommand(command)) {
    return;
  }

  try {
    localStorage.setItem(LAST_COMMAND_KEY, command);
  } catch {
    // Ignore private-mode / quota errors.
  }
}

function swapActiveVideo(nextVideo) {
  activeVideo.classList.remove("active");
  inactiveVideo.classList.remove("active");

  activeVideo = nextVideo;
  inactiveVideo = nextVideo === videoA ? videoB : videoA;

  activeVideo.classList.add("active");
}

function waitForFirstFrame(video) {
  return new Promise((resolve) => {
    if (video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      resolve();
      return;
    }

    const onReady = () => {
      video.removeEventListener("loadeddata", onReady);
      video.removeEventListener("canplay", onReady);
      resolve();
    };

    video.addEventListener("loadeddata", onReady, { once: true });
    video.addEventListener("canplay", onReady, { once: true });
  });
}

function startVideoPlayback(video, command, onEnded) {
  video.loop = command === "RET_NO_TRAIN";
  video.onended = onEnded;

  return waitForFirstFrame(video)
    .then(() => video.play())
    .then(() => {
      swapActiveVideo(video);
      isPlaying = true;
      currentCommand = command;
      saveLastCommand(command);
      if (command !== "RET_NO_TRAIN") {
        isInitialNoTrain = false;
      }
    });
}

function cancelPendingPlayback() {
  if (!pendingPlayback) {
    return;
  }

  pendingPlayback.video.onended = null;
  pendingPlayback.video.pause();
  pendingPlayback.video.removeAttribute("src");
  pendingPlayback.video.load();
  pendingPlayback = null;
}

function interruptPlayback() {
  cancelPendingPlayback();
  for (const video of videos) {
    video.pause();
    video.currentTime = 0;
    video.loop = false;
  }
  isPlaying = false;
  currentCommand = null;
}

function enqueueCommand(command) {
  if (!isKnownCommand(command)) {
    return;
  }

  if (command === currentCommand || command === queuedCommand) {
    return;
  }

  const isInitialNoTrainActive =
    isInitialNoTrain &&
    (currentCommand === "RET_NO_TRAIN" ||
      pendingPlayback?.command === "RET_NO_TRAIN");

  if (isInitialNoTrainActive) {
    if (command === "RET_NO_TRAIN") {
      return;
    }

    isInitialNoTrain = false;
    interruptPlayback();
    playCommand(command);
    return;
  }

  if (
    currentCommand === "RET_NO_TRAIN" &&
    command !== "RET_NO_TRAIN" &&
    (isPlaying || pendingPlayback)
  ) {
    interruptPlayback();
    playCommand(command);
    return;
  }

  if (!isPlaying && !pendingPlayback) {
    playCommand(command);
    return;
  }

  queuedCommand = command;
}

function playCommand(command) {
  const src = commandVideoMap[command];
  if (!src) {
    return;
  }

  cancelPendingPlayback();

  const nextVideo = inactiveVideo;
  nextVideo.pause();
  nextVideo.currentTime = 0;
  nextVideo.src = src;
  nextVideo.load();

  pendingPlayback = { video: nextVideo, command };

  startVideoPlayback(nextVideo, command, onPlaybackFinished)
    .then(() => {
      if (pendingPlayback?.video === nextVideo) {
        pendingPlayback = null;
      }
    })
    .catch((error) => {
      console.error("[Video] Autoplay failed:", error);
      if (pendingPlayback?.video === nextVideo) {
        pendingPlayback = null;
      }
      onPlaybackFinished();
    });
}

function onPlaybackFinished() {
  const finishedCommand = currentCommand;

  if (finishedCommand === "RET_NO_TRAIN" && isInitialNoTrain) {
    isInitialNoTrain = false;
  }

  isPlaying = false;
  currentCommand = null;

  if (queuedCommand) {
    const nextCommand = queuedCommand;
    queuedCommand = null;
    playCommand(nextCommand);
    return;
  }

  if (finishedCommand === "RET_TRAIN_DEPARTED") {
    playCommand("RET_NO_TRAIN");
  }
}

function resetHeartbeat() {
  clearTimeout(heartbeatTimer);
  heartbeatTimer = setTimeout(() => {
    console.log("[WS] No heartbeat received, reconnecting...");
    if (socket) socket.close();
  }, HEARTBEAT_TIMEOUT_MS);
}

function cleanup() {
  clearTimeout(heartbeatTimer);
  socket = null;
}

function connect() {
  if (socket && socket.readyState === WebSocket.OPEN) return;

  setConnectionState(false);

  try {
    socket = new WebSocket(WS_URL);
  } catch {
    setConnectionState(false);
    scheduleReconnect();
    return;
  }

  socket.addEventListener("open", () => {
    setConnectionState(true);
    resetHeartbeat();
    console.log("[WS] Connected to", WS_URL);
  });

  socket.addEventListener("message", (event) => {
    resetHeartbeat();

    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    if (msg.type === "transit-command" && isKnownCommand(msg.command)) {
      enqueueCommand(msg.command);
    }
  });

  socket.addEventListener("close", () => {
    cleanup();
    setConnectionState(false);
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    if (socket) socket.close();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, RECONNECT_DELAY_MS);
}

const startupCommand = loadLastCommand();
isInitialNoTrain = startupCommand === "RET_NO_TRAIN";
enqueueCommand(startupCommand);
connect();
