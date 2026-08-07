const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/content`;
//const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://transitproject-763503917257.europe-west1.run.app/content`;
const RECONNECT_DELAY_MS = 3000;
// Must be > server WS_PING_INTERVAL_MS (default 25s). JSON heartbeats reset this.
const HEARTBEAT_TIMEOUT_MS = 70000;

// "countdown" = play Video 2 + on-screen 10→0, then Video 3
// "video"     = play Video 2 through (or until ARRIVED), then Video 3 (no overlay)
const ARRIVING_MODE = "countdown";
const COUNTDOWN_FROM_SEC = 10;

// Video 1 idle / Video 2 arriving (swappable) / Video 3 arrived
const VIDEO_IDLE = "./videos/video01.mp4";
const ARRIVING_VIDEO_SRC = "./videos/video02.mp4";
const VIDEO_ARRIVED = "./videos/video03.mp4";
const VIDEO_DEPARTED = "./videos/video01.mp4";

const commandVideoMap = {
  RET_NO_TRAIN: VIDEO_IDLE,
  RET_TRAIN_ARRIVING_15S: ARRIVING_VIDEO_SRC,
  RET_TRAIN_ARRIVED: VIDEO_ARRIVED,
  RET_TRAIN_DEPARTED: VIDEO_DEPARTED,
};

const STATUS_DOT_CLASS = {
  RET_NO_TRAIN: "status-no-train",
  RET_TRAIN_ARRIVING_15S: "status-arriving",
  RET_TRAIN_ARRIVED: "status-arrived",
  RET_TRAIN_DEPARTED: "status-departing",
};

const videoA = document.getElementById("videoA");
const videoB = document.getElementById("videoB");
const connectionDot = document.getElementById("connectionDot");
const countdownEl = document.getElementById("countdown");

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
let wsConnected = false;
let statusCommand = "RET_NO_TRAIN";

let countdownTimer = null;
let countdownValue = null;

for (const video of videos) {
  video.muted = true;
  video.playsInline = true;
  video.loop = true;
}

function updateStatusDot() {
  const classes = ["connection-dot"];
  if (!wsConnected) {
    classes.push("disconnected");
  } else {
    classes.push("connected");
    classes.push(STATUS_DOT_CLASS[statusCommand] || "status-no-train");
  }
  connectionDot.className = classes.join(" ");
}

function setConnectionState(connected) {
  wsConnected = connected;
  updateStatusDot();
}

function setStatusCommand(command) {
  if (STATUS_DOT_CLASS[command]) {
    statusCommand = command;
  }
  updateStatusDot();
}

function isKnownCommand(command) {
  return Boolean(commandVideoMap[command]);
}

function showCountdown(value) {
  if (!countdownEl) return;
  countdownValue = value;
  countdownEl.textContent = String(value);
  countdownEl.hidden = false;
  countdownEl.setAttribute("aria-hidden", "false");
}

function hideCountdown() {
  if (!countdownEl) return;
  countdownValue = null;
  countdownEl.hidden = true;
  countdownEl.textContent = "";
  countdownEl.setAttribute("aria-hidden", "true");
}

function stopCountdown() {
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
  hideCountdown();
}

function startCountdown(fromSec, onComplete) {
  stopCountdown();
  let remaining = Math.max(0, Math.floor(fromSec));
  showCountdown(remaining);

  if (remaining <= 0) {
    onComplete();
    return;
  }

  countdownTimer = setInterval(() => {
    remaining -= 1;
    if (remaining <= 0) {
      stopCountdown();
      onComplete();
      return;
    }
    showCountdown(remaining);
  }, 1000);
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

function startVideoPlayback(video, command) {
  video.loop = true;
  video.onended = null;

  return waitForFirstFrame(video)
    .then(() => video.play())
    .then(() => {
      swapActiveVideo(video);
      isPlaying = true;
      currentCommand = command;
      setStatusCommand(command);
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
  stopCountdown();
  cancelPendingPlayback();
  for (const video of videos) {
    video.pause();
    video.currentTime = 0;
    video.loop = true;
    video.onended = null;
  }
  isPlaying = false;
  currentCommand = null;
}

function enqueueCommand(command) {
  if (!isKnownCommand(command)) {
    return;
  }

  // ARRIVED while arriving → cut straight to Video 3
  if (
    command === "RET_TRAIN_ARRIVED" &&
    (currentCommand === "RET_TRAIN_ARRIVING_15S" ||
      pendingPlayback?.command === "RET_TRAIN_ARRIVING_15S" ||
      queuedCommand === "RET_TRAIN_ARRIVING_15S")
  ) {
    queuedCommand = null;
    interruptPlayback();
    playCommand("RET_TRAIN_ARRIVED");
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

  // Higher-priority state changes interrupt arriving/arrived
  if (
    (command === "RET_TRAIN_DEPARTED" || command === "RET_NO_TRAIN") &&
    (currentCommand === "RET_TRAIN_ARRIVING_15S" ||
      currentCommand === "RET_TRAIN_ARRIVED" ||
      pendingPlayback?.command === "RET_TRAIN_ARRIVING_15S" ||
      pendingPlayback?.command === "RET_TRAIN_ARRIVED")
  ) {
    queuedCommand = null;
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
  stopCountdown();

  const nextVideo = inactiveVideo;
  nextVideo.pause();
  nextVideo.currentTime = 0;
  nextVideo.src = src;
  nextVideo.load();

  pendingPlayback = { video: nextVideo, command };
  setStatusCommand(command);

  const isArriving = command === "RET_TRAIN_ARRIVING_15S";

  startVideoPlayback(nextVideo, command)
    .then(() => {
      if (pendingPlayback?.video === nextVideo) {
        pendingPlayback = null;
      }
      // Countdown drives Video 2 → Video 3; video mode waits for ARRIVED (clips loop).
      if (isArriving && ARRIVING_MODE === "countdown") {
        startCountdown(COUNTDOWN_FROM_SEC, () => {
          if (currentCommand !== "RET_TRAIN_ARRIVING_15S") {
            return;
          }
          queuedCommand = null;
          interruptPlayback();
          playCommand("RET_TRAIN_ARRIVED");
        });
      }
    })
    .catch((error) => {
      console.error("[Video] Autoplay failed:", error);
      if (pendingPlayback?.video === nextVideo) {
        pendingPlayback = null;
      }
      if (queuedCommand) {
        const nextCommand = queuedCommand;
        queuedCommand = null;
        playCommand(nextCommand);
      }
    });
}

function resetHeartbeat() {
  clearTimeout(heartbeatTimer);
  heartbeatTimer = setTimeout(() => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    console.log("[WS] No heartbeat received, reconnecting...");
    try {
      socket.close();
    } catch {
      // ignore
    }
  }, HEARTBEAT_TIMEOUT_MS);
}

function cleanup() {
  clearTimeout(heartbeatTimer);
  heartbeatTimer = null;
  socket = null;
}

function connect() {
  if (
    socket &&
    (socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }

  setConnectionState(false);

  try {
    socket = new WebSocket(WS_URL);
  } catch {
    setConnectionState(false);
    scheduleReconnect();
    return;
  }

  socket.addEventListener("open", () => {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
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
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.close();
    }
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, RECONNECT_DELAY_MS);
}

updateStatusDot();
// Default: Video 1 (idle) loops until the server pushes a command.
playCommand("RET_NO_TRAIN");
connect();
