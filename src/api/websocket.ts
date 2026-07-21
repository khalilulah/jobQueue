import { Server as SocketIOServer } from "socket.io";
import { queueEvents } from "../events/queueEvents";
import { redis } from "../config/redis";
import { QUEUE_KEYS } from "../queue/keys";

/**
 * Wires up the WebSocket layer:
 *
 * 1. When a browser connects, immediately send it a snapshot of current
 *    queue state (depths, active jobs, recent completions) so it renders
 *    real data right away rather than a blank screen.
 *
 * 2. Register listeners on queueEvents for every event the queue emits,
 *    forwarding each one to ALL connected browsers via io.emit().
 *
 * This function is called once during server startup, after the Socket.io
 * server (io) has been created. After that, it runs passively — no polling,
 * no timers, just event forwarding as things happen.
 */
export function setupWebSocket(io: SocketIOServer): void {
  // ── Per-connection handler ───────────────────────────────────────────
  // Fires once per browser that opens the dashboard. We use this to send
  // the initial snapshot — the REST endpoints will do the same thing more
  // richly, but this ensures even a WebSocket-only connection gets real
  // data immediately rather than waiting for the next queue event.
  io.on("connection", async (socket) => {
    console.log(`[ws] client connected: ${socket.id}`);

    // Send a snapshot of current queue state to this specific new client.
    // socket.emit (not io.emit) because this is only for the new arrival —
    // everyone else already has the current state from prior events.
    try {
      const snapshot = await buildStatsSnapshot();
      socket.emit("stats:snapshot", snapshot);
    } catch (err) {
      console.error(
        "[ws] failed to send initial snapshot:",
        (err as Error).message,
      );
    }

    socket.on("disconnect", () => {
      console.log(`[ws] client disconnected: ${socket.id}`);
    });
  });

  // ── Queue event forwarding ───────────────────────────────────────────
  // Each listener here is the production equivalent of the test's
  // queueEvents.once('job:enqueued', resolve) — except instead of
  // resolving a Promise, we broadcast to every connected browser.
  //
  // io.emit(eventName, data) sends to ALL currently connected clients.
  // This is correct for a dashboard showing global queue state — every
  // browser should see the same events regardless of when they connected.

  queueEvents.on("job:enqueued", (job) => {
    io.emit("job:enqueued", {
      jobId: job.id,
      type: job.type,
      priority: job.priority,
      maxRetries: job.maxRetries,
      createdAt: job.createdAt,
    });
  });

  queueEvents.on("job:started", (data) => {
    io.emit("job:started", data);
  });

  queueEvents.on("job:completed", (data) => {
    io.emit("job:completed", data);
  });

  queueEvents.on("job:failed", (data) => {
    io.emit("job:failed", data);
  });

  queueEvents.on("job:dlq", (data) => {
    io.emit("job:dlq", data);
  });

  queueEvents.on("job:requeued", (data) => {
    io.emit("job:requeued", data);
  });

  queueEvents.on("reaper:reclaimed", (data) => {
    io.emit("reaper:reclaimed", data);
  });

  // ── Periodic stats broadcast ─────────────────────────────────────────
  // Even if no jobs are moving, the Overview screen's queue-depth cards
  // should stay accurate. We push a stats update every 2 seconds so the
  // numbers don't go stale during quiet periods.
  setInterval(async () => {
    try {
      const stats = await buildStatsSnapshot();
      io.emit("stats:update", stats);
    } catch (err) {
      // Don't crash the interval on a transient Redis error — just skip
      // this tick and try again in 2 seconds.
      console.error("[ws] stats broadcast error:", (err as Error).message);
    }
  }, 2000);
}

// ── Snapshot builder ─────────────────────────────────────────────────────
// Queries Redis for current queue depths and returns a plain object the
// browser can use to render the Overview screen's cards immediately.
// Used both on initial connection and in the periodic stats broadcast.
async function buildStatsSnapshot() {
  const [high, medium, low, processing, delayed, dead] = await Promise.all([
    redis.llen(QUEUE_KEYS.pending.high),
    redis.llen(QUEUE_KEYS.pending.medium),
    redis.llen(QUEUE_KEYS.pending.low),
    redis.llen(QUEUE_KEYS.processing),
    redis.zcard(QUEUE_KEYS.delayed),
    redis.llen(QUEUE_KEYS.dead),
  ]);

  return {
    queueDepth: { high, medium, low, total: high + medium + low },
    processing,
    delayed,
    dlq: dead,
    timestamp: Date.now(),
  };
}
