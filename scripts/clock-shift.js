/**
 * Preload that shifts the process clock so a replayed capture looks live.
 *
 *   CLOCK_OFFSET_MS=<captureStart - replayStart> node --require ./scripts/clock-shift.js src/server.js
 *
 * Only "now" moves (Date.now() and new Date() without arguments). Timers keep
 * real durations, so the replay must run at 1x speed (scripts/replay-feed.js).
 */
const offset = Number(process.env.CLOCK_OFFSET_MS || 0);

if (offset) {
  const RealDate = Date;

  class ShiftedDate extends RealDate {
    constructor(...args) {
      if (args.length === 0) {
        super(RealDate.now() + offset);
      } else {
        super(...args);
      }
    }

    static now() {
      return RealDate.now() + offset;
    }
  }

  globalThis.Date = ShiftedDate;
  console.log(`[clock-shift] clock offset ${offset} ms (now reads ${new Date().toISOString()})`);
}
