(function () {
  const DESIGN_WIDTH = 1080;
  const DESIGN_HEIGHT = 1920;
  const DEFAULT_API = "https://dynode-main-8eca196.zuplo.app";
  const STATUS_LABELS = {
    A: "Arrived",
    C: "Cancelled",
    D: "Departed",
    E: "New time",
    N: "New info",
  };

  const params = new URLSearchParams(window.location.search);
  const apiBase = (params.get("api") || DEFAULT_API).replace(/\/$/, "");
  const direction = (params.get("direction") || "D").trim().toUpperCase();
  const refreshSeconds = Math.max(
    15,
    Number(params.get("refresh") || 180) || 180,
  );
  const iataParam = (params.get("iata") || "").trim().toUpperCase();
  const gatesParam = (params.get("gates") || "")
    .split(",")
    .map((g) => g.trim().toUpperCase())
    .filter(Boolean)
    .join(",");
  const gateFilter = new Set(
    gatesParam ? gatesParam.split(",").filter(Boolean) : [],
  );

  const boardEl = document.getElementById("board");
  const boardFrameEl = document.getElementById("boardFrame");
  const gateLabelEl = document.getElementById("gateLabel");
  const gateEl = document.getElementById("gate");
  const iataEl = document.getElementById("iata");
  const contentEl = document.getElementById("content");
  const clockEl = document.getElementById("clock");
  const eyebrowEl = document.getElementById("eyebrow");
  const headlineEl = document.getElementById("headline");

  function fitBoardToViewport() {
    if (!boardEl || !boardFrameEl) return;
    const scale = Math.min(
      window.innerWidth / DESIGN_WIDTH,
      window.innerHeight / DESIGN_HEIGHT,
    );
    boardFrameEl.style.width = `${DESIGN_WIDTH * scale}px`;
    boardFrameEl.style.height = `${DESIGN_HEIGHT * scale}px`;
    boardEl.style.transform = `scale(${scale})`;
  }

  let fitFrame = 0;
  function scheduleFit() {
    cancelAnimationFrame(fitFrame);
    fitFrame = requestAnimationFrame(fitBoardToViewport);
  }

  fitBoardToViewport();
  window.addEventListener("resize", scheduleFit);
  window.addEventListener("orientationchange", scheduleFit);

  function getPlayerQuery() {
    const resourceId = params
      .get("com.broadsign.suite.bsp.resource_id")
      ?.trim();
    if (resourceId) {
      return {
        key: "com.broadsign.suite.bsp.resource_id",
        value: resourceId,
      };
    }

    const player = params.get("player")?.trim();
    if (player) {
      return { key: "player", value: player };
    }

    return null;
  }

  function buildFlightsUrl(extraParams = {}) {
    const playerQuery = getPlayerQuery();
    const url = new URL(`${apiBase}/flights/norway`);
    if (playerQuery) {
      url.searchParams.set(playerQuery.key, playerQuery.value);
    }
    if (iataParam) {
      url.searchParams.set("iata", iataParam);
    }
    if (gatesParam) {
      url.searchParams.set("gates", gatesParam);
    }
    for (const [key, value] of Object.entries(extraParams)) {
      if (value != null && value !== "") {
        url.searchParams.set(key, value);
      }
    }
    return url;
  }

  function setDirectionLabels(dir) {
    if (dir === "A") {
      document.title = "Next Arrival";
      eyebrowEl.textContent = "Arrivals";
      headlineEl.textContent = "Next arrival";
      if (gateLabelEl) gateLabelEl.textContent = "Belt";
      return;
    }

    document.title = "Next Departure";
    eyebrowEl.textContent = "Departures";
    headlineEl.textContent = "Next departure";
    if (gateLabelEl) gateLabelEl.textContent = "Gate";
  }

  function tickClock() {
    const now = new Date();
    clockEl.textContent = now.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  }

  function formatTime(iso) {
    if (!iso) return "—";
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return iso;
    return date.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "Europe/Oslo",
    });
  }

  function flightDirection(flight, fallback = "D") {
    const raw =
      flight?.arr_dep ||
      flight?.arrDep ||
      flight?.direction ||
      flight?.Direction ||
      fallback;
    return String(raw).trim().toUpperCase() || fallback;
  }

  function gateValue(flight) {
    return flight?.gate || "—";
  }

  function beltValue(flight) {
    return flight?.beltNumber || flight?.belt || "—";
  }

  function displayPoint(flight, dir) {
    if (dir === "A") {
      const belt = beltValue(flight);
      if (belt !== "—") return belt;
      return gateValue(flight);
    }
    return gateValue(flight);
  }

  function passesGateFilter(flight, dir) {
    if (gateFilter.size === 0) return true;
    const gate = String(gateValue(flight) || "").toUpperCase();
    const belt = String(beltValue(flight) || "").toUpperCase();
    if (dir === "A") {
      return (
        (belt && belt !== "—" && gateFilter.has(belt)) ||
        (gate && gate !== "—" && gateFilter.has(gate))
      );
    }
    return gate && gate !== "—" && gateFilter.has(gate);
  }

  function etaLabel(flight, dir) {
    const code = flight.status && flight.status.code;
    const statusTime = flight.status && flight.status.time;
    if (!statusTime) return null;
    if (code === "A") {
      return { label: "Arrived", time: formatTime(statusTime) };
    }
    if (code === "E" || flight.delayed === "Y") {
      return {
        label: dir === "D" ? "ETD" : "ETA",
        time: formatTime(statusTime),
      };
    }
    return { label: "Updated", time: formatTime(statusTime) };
  }

  function statusClass(code, delayed) {
    if (code === "C") return "is-cancelled";
    if (code === "E" || delayed === "Y") return "is-new-time";
    return "";
  }

  function statusLabel(flight) {
    const code = flight.status && flight.status.code;
    if (flight.delayed === "Y" && code !== "C") {
      return code === "E" ? "Delayed · New time" : "Delayed";
    }
    return STATUS_LABELS[code] || code || "Scheduled";
  }

  function collectFlights(payload, preferredDirection) {
    if (!payload || typeof payload !== "object") return [];

    if (preferredDirection === "A" && Array.isArray(payload.arrivals)) {
      return payload.arrivals;
    }
    if (preferredDirection === "D" && Array.isArray(payload.departures)) {
      return payload.departures;
    }

    const flights = Array.isArray(payload.flights) ? payload.flights : [];
    const filtered = flights.filter(
      (flight) => flightDirection(flight, preferredDirection) === preferredDirection,
    );
    if (filtered.length) return filtered;

    // Single-direction payload with no arr_dep markers.
    if (flights.length) {
      const payloadDir = String(payload.direction || "").toUpperCase();
      if (!payloadDir || payloadDir === preferredDirection) return flights;
    }

    return [];
  }

  function pickNextFlight(flights, dir) {
    const now = Date.now();
    const sorted = (flights || [])
      .filter((flight) => flight.scheduleTime)
      .filter((flight) => passesGateFilter(flight, dir))
      .slice()
      .sort(
        (a, b) =>
          new Date(a.scheduleTime).getTime() -
          new Date(b.scheduleTime).getTime(),
      );

    const upcoming = sorted.find(
      (flight) =>
        new Date(flight.scheduleTime).getTime() >= now - 15 * 60 * 1000,
    );
    return upcoming || sorted[0] || null;
  }

  function renderError(message) {
    contentEl.innerHTML = `<p class="error">${message}</p>`;
  }

  function renderFlight(payload) {
    const resolvedDirection = (
      direction ||
      payload.direction ||
      "D"
    ).toUpperCase();

    setDirectionLabels(resolvedDirection);

    iataEl.textContent =
      payload.iata || payload.airportCode || iataParam || "—";

    if (payload.error) {
      renderError(payload.message || payload.error);
      return;
    }

    const flights = collectFlights(payload, resolvedDirection);
    const next = pickNextFlight(flights, resolvedDirection);
    if (!next) {
      gateEl.textContent = gatesParam || payload.gate || "—";
      contentEl.innerHTML = `<p class="empty">No upcoming ${
        resolvedDirection === "D" ? "departures" : "arrivals"
      } for this ${resolvedDirection === "D" ? "gate" : "belt"}.</p>`;
      return;
    }

    const point = displayPoint(next, resolvedDirection);
    gateEl.textContent = point;

    const status = statusLabel(next);
    const klass = statusClass(next.status && next.status.code, next.delayed);
    const eta = etaLabel(next, resolvedDirection);
    const placeLabel = resolvedDirection === "D" ? "To" : "From";
    const city = next.airportName || next.airport || "—";
    const cityCode = next.airportName && next.airport ? next.airport : "";
    const secondaryLabel = resolvedDirection === "D" ? "Gate" : "Belt";

    contentEl.innerHTML = `
      <div class="flight-id">${next.flightId || "—"}</div>
      <div>
        <div class="meta-label">${placeLabel}</div>
        <div class="city">${city}</div>
        ${cityCode ? `<div class="city-code">${cityCode}</div>` : ""}
      </div>
      <dl class="detail-grid">
        <div class="detail">
          <dt>Scheduled</dt>
          <dd>${formatTime(next.scheduleTime)}</dd>
        </div>
        <div class="detail">
          <dt>${eta ? eta.label : resolvedDirection === "D" ? "ETD" : "ETA"}</dt>
          <dd>${eta ? eta.time : formatTime(next.scheduleTime)}</dd>
        </div>
        <div class="detail">
          <dt>Airline</dt>
          <dd>${next.airline || "—"}</dd>
        </div>
        <div class="detail">
          <dt>${secondaryLabel}</dt>
          <dd>${point}</dd>
        </div>
      </dl>
      <div class="status ${klass}">${status}</div>
    `;
  }

  function loadFlights() {
    const playerQuery = getPlayerQuery();
    if (!playerQuery && !iataParam && !gatesParam) {
      renderError(
        "Missing player id or iata/gates. Add ?player=…, ?com.broadsign.suite.bsp.resource_id=…, ?iata=…, or ?gates=… to the URL.",
      );
      return;
    }

    // Default JS response from Zuplo (data = {...};) — works without CORS preflight.
    const requestUrl = buildFlightsUrl({ direction });

    const previous = window.data;
    window.data = undefined;

    const script = document.createElement("script");
    script.src = requestUrl.toString();
    script.async = true;
    script.onload = function () {
      const payload = window.data;
      window.data = previous;
      script.remove();
      if (!payload) {
        renderError("Empty response from gateway.");
        return;
      }
      renderFlight(payload);
    };
    script.onerror = function () {
      script.remove();
      window.data = previous;
      renderError("Could not load flight data from the gateway.");
    };
    document.head.appendChild(script);
  }

  setDirectionLabels(direction);
  tickClock();
  setInterval(tickClock, 1000);
  loadFlights();
  setInterval(loadFlights, refreshSeconds * 1000);
})();
