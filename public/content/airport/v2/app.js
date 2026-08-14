(function () {
  const DESIGN_WIDTH = 1920;
  const DESIGN_HEIGHT = 1080;
  const DEFAULT_API = "https://dynode-main-8eca196.zuplo.app";
  const MAX_ROWS = 15;
  const STATUS_LABELS = {
    A: "Arrived",
    C: "Cancelled",
    D: "Departed",
    E: "New time",
    N: "New info",
  };

  const params = new URLSearchParams(window.location.search);
  const apiBase = (params.get("api") || DEFAULT_API).replace(/\/$/, "");
  const refreshSeconds = Math.max(
    15,
    Number(params.get("refresh") || 180) || 180,
  );
  const iataParam = (params.get("iata") || "").trim().toUpperCase();
  // Optional: limit gates, comma-separated (e.g. ?gates=A12,B3) — forwarded to API
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
  const arrivalsListEl = document.getElementById("arrivalsList");
  const departuresListEl = document.getElementById("departuresList");
  const arrivalsFooterEl = document.getElementById("arrivalsFooter");
  const departuresFooterEl = document.getElementById("departuresFooter");
  const iataLeftEl = document.getElementById("iataLeft");
  const clockEl = document.getElementById("clock");

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

  function tickClock() {
    const now = new Date();
    clockEl.textContent = now.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatTime(iso) {
    if (!iso) return "—";
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return String(iso);
    return date.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "Europe/Oslo",
    });
  }

  function flightDirection(flight) {
    const raw =
      flight?.arr_dep ||
      flight?.arrDep ||
      flight?.direction ||
      flight?.Direction ||
      "";
    return String(raw).trim().toUpperCase();
  }

  function statusCode(flight) {
    return String(flight?.status?.code || flight?.statusCode || "")
      .trim()
      .toUpperCase();
  }

  function expectedTime(flight) {
    return (
      flight?.status?.time ||
      flight?.expectedTime ||
      flight?.estimatedTime ||
      null
    );
  }

  function viaLabel(flight) {
    const via =
      flight?.viaAirportName ||
      flight?.via_airport_name ||
      flight?.viaAirport ||
      flight?.via_airport ||
      flight?.via ||
      "";
    if (Array.isArray(via)) {
      return via.filter(Boolean).join(" · ") || "—";
    }
    return via || "—";
  }

  function cityLabel(flight) {
    return flight?.airportName || flight?.airport || "—";
  }

  function gateValue(flight) {
    return flight?.gate || "—";
  }

  function beltValue(flight) {
    return flight?.beltNumber || flight?.belt || "—";
  }

  function statusClass(flight) {
    const code = statusCode(flight);
    if (code === "C") return "is-cancelled";
    if (code === "E" || flight?.delayed === "Y") return "is-new-time";
    return "";
  }

  function statusDotClass(flight) {
    const code = statusCode(flight);
    if (code === "C") return "is-bad";
    if (code === "E" || flight?.delayed === "Y") return "is-warn";
    return "";
  }

  function statusLabel(flight) {
    const code = statusCode(flight);
    if (flight?.delayed === "Y" && code !== "C") {
      return code === "E" ? "Delayed" : "Delayed";
    }
    return STATUS_LABELS[code] || (code ? code : "On time");
  }

  function sortUpcoming(flights) {
    const now = Date.now() - 20 * 60 * 1000;
    return (flights || [])
      .filter((flight) => flight?.scheduleTime)
      .slice()
      .sort(
        (a, b) =>
          new Date(a.scheduleTime).getTime() -
          new Date(b.scheduleTime).getTime(),
      )
      .filter((flight) => new Date(flight.scheduleTime).getTime() >= now)
      .slice(0, MAX_ROWS);
  }

  function passesGateFilter(flight) {
    if (gateFilter.size === 0) return true;
    const gate = String(gateValue(flight) || "").toUpperCase();
    const belt = String(beltValue(flight) || "").toUpperCase();
    return (
      (gate && gate !== "—" && gateFilter.has(gate)) ||
      (belt && belt !== "—" && gateFilter.has(belt))
    );
  }

  function uniqueSortedValues(flights, picker) {
    const values = new Set();
    for (const flight of flights || []) {
      const value = picker(flight);
      if (value && value !== "—") values.add(String(value).toUpperCase());
    }
    return [...values].sort((a, b) =>
      a.localeCompare(b, "en", { numeric: true }),
    );
  }

  function normalizeFlightLists(payload) {
    if (!payload || typeof payload !== "object") {
      return { arrivals: [], departures: [], meta: {} };
    }

    const meta = {
      gate: payload.gate || null,
      iata: payload.iata || payload.airportCode || null,
      gates: payload.gates || null,
    };

    if (Array.isArray(payload.arrivals) || Array.isArray(payload.departures)) {
      return {
        arrivals: payload.arrivals || [],
        departures: payload.departures || [],
        meta,
      };
    }

    const flights = Array.isArray(payload.flights) ? payload.flights : [];
    const arrivals = [];
    const departures = [];
    for (const flight of flights) {
      const dir = flightDirection(flight);
      if (dir === "A") arrivals.push(flight);
      else if (dir === "D") departures.push(flight);
    }

    // Older single-direction payloads: treat whole list as that direction.
    if (arrivals.length === 0 && departures.length === 0 && flights.length) {
      const fallback = String(payload.direction || "").toUpperCase();
      if (fallback === "A") return { arrivals: flights, departures: [], meta };
      return { arrivals: [], departures: flights, meta };
    }

    return { arrivals, departures, meta };
  }

  function renderRows(listEl, flights, direction) {
    const rows = sortUpcoming(flights).filter((flight) =>
      passesGateFilter(flight),
    );

    if (!rows.length) {
      listEl.innerHTML = `<p class="empty">No upcoming ${
        direction === "A" ? "arrivals" : "departures"
      }.</p>`;
      return rows;
    }

    listEl.innerHTML = rows
      .map((flight) => {
        const klass = statusClass(flight);
        const dot = statusDotClass(flight);
        const expect = expectedTime(flight);
        return `
          <div class="flight-row ${klass}">
            <span class="col col-status"><span class="dot ${dot}"></span></span>
            <span class="col col-flight">${escapeHtml(flight.flightId || "—")}</span>
            <span class="col col-city">${escapeHtml(cityLabel(flight))}</span>
            <span class="col col-via">${escapeHtml(viaLabel(flight))}</span>
            <span class="col col-time">${escapeHtml(formatTime(flight.scheduleTime))}</span>
            <span class="col col-eta">${escapeHtml(formatTime(expect || flight.scheduleTime))}</span>
            <span class="col col-gate">${escapeHtml(gateValue(flight))}</span>
            <span class="col col-belt">${escapeHtml(beltValue(flight))}</span>
            <span class="col col-remark">${escapeHtml(statusLabel(flight))}</span>
          </div>
        `;
      })
      .join("");

    return rows;
  }

  function renderBoard(payload) {
    if (payload?.error) {
      const message = payload.message || payload.error;
      arrivalsListEl.innerHTML = `<p class="error">${message}</p>`;
      departuresListEl.innerHTML = `<p class="error">${message}</p>`;
      return;
    }

    const { arrivals, departures, meta } = normalizeFlightLists(payload);
    const shownArrivals = renderRows(arrivalsListEl, arrivals, "A");
    const shownDepartures = renderRows(departuresListEl, departures, "D");

    const arrivalGates = uniqueSortedValues(shownArrivals, gateValue);
    const arrivalBelts = uniqueSortedValues(shownArrivals, beltValue);
    const departureGates = uniqueSortedValues(shownDepartures, gateValue);

    const arrivalBits = [...arrivalGates, ...arrivalBelts];
    arrivalsFooterEl.textContent = arrivalBits.length
      ? `Arrivals · ${arrivalBits.join(" · ")}`
      : "Arrivals";
    departuresFooterEl.textContent = departureGates.length
      ? `Departures · ${departureGates.join(" · ")}`
      : "Departures";

    iataLeftEl.textContent = meta.iata || iataParam || meta.gate || "Avinor";
  }

  function fetchJsonp(url) {
    return new Promise((resolve, reject) => {
      const previous = window.data;
      window.data = undefined;
      const script = document.createElement("script");
      script.src = url;
      script.async = true;
      script.onload = function () {
        const payload = window.data;
        window.data = previous;
        script.remove();
        if (!payload) {
          reject(new Error("Empty response from gateway."));
          return;
        }
        resolve(payload);
      };
      script.onerror = function () {
        script.remove();
        window.data = previous;
        reject(new Error("Could not load flight data from the gateway."));
      };
      document.head.appendChild(script);
    });
  }

  async function loadFlights() {
    const playerQuery = getPlayerQuery();
    if (!playerQuery && !iataParam && !gatesParam) {
      const message =
        "No airport or gate/belt provided. Add ?iata=… and/or ?gates=… (or a player id) to the URL.";
      arrivalsListEl.innerHTML = `<p class="error">${message}</p>`;
      departuresListEl.innerHTML = `<p class="error">${message}</p>`;
      return;
    }

    try {
      // Prefer combined feed (no direction). Fall back to A + D if needed.
      let payload = await fetchJsonp(buildFlightsUrl().toString());
      const normalized = normalizeFlightLists(payload);
      const hasBothSides =
        normalized.arrivals.length > 0 || normalized.departures.length > 0;

      if (
        !hasBothSides &&
        Array.isArray(payload?.flights) &&
        payload.flights.length === 0
      ) {
        const [arrivalsPayload, departuresPayload] = await Promise.all([
          fetchJsonp(buildFlightsUrl({ direction: "A" }).toString()),
          fetchJsonp(buildFlightsUrl({ direction: "D" }).toString()),
        ]);

        payload = {
          iata: arrivalsPayload.iata || departuresPayload.iata,
          airportCode:
            arrivalsPayload.airportCode || departuresPayload.airportCode,
          gate: arrivalsPayload.gate || departuresPayload.gate,
          arrivals: arrivalsPayload.flights || arrivalsPayload.arrivals || [],
          departures:
            departuresPayload.flights || departuresPayload.departures || [],
        };
      }

      renderBoard(payload);
    } catch (error) {
      const message = error?.message || "Could not load flight data.";
      arrivalsListEl.innerHTML = `<p class="error">${message}</p>`;
      departuresListEl.innerHTML = `<p class="error">${message}</p>`;
    }
  }

  tickClock();
  setInterval(tickClock, 1000);
  loadFlights();
  setInterval(loadFlights, refreshSeconds * 1000);
})();
