import { Router, Request, Response } from "express";
import { redis } from "../../config/redis";
import { QUEUE_KEYS } from "../../queue/keys";
import { queueEvents } from "../../events/queueEvents";
import { Job } from "../../queue/types";

export const dlqRouter = Router();

/**
 * GET /api/dlq?limit=50&offset=0
 *
 * Returns the contents of the dead-letter queue with full failure metadata
 * (lastError, failedAt, attempts). This is the DLQ screen's primary data
 * source on load — after that, job:dlq WebSocket events push new arrivals
 * in real time without a page refresh.
 */
dlqRouter.get("/", async (req: Request, res: Response) => {
  const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
  const offset = parseInt(req.query.offset as string) || 0;

  try {
    const entries = await redis.lrange(
      QUEUE_KEYS.dead,
      offset,
      offset + limit - 1,
    );
    const total = await redis.llen(QUEUE_KEYS.dead);

    const jobs = entries.map((raw) => JSON.parse(raw));

    res.json({ jobs, total, limit, offset });
  } catch (err) {
    console.error("[api] GET /api/dlq error:", (err as Error).message);
    res.status(500).json({ error: "Failed to fetch DLQ" });
  }
});

/**
 * POST /api/dlq/:id/requeue
 *
 * Moves a single job from the dead-letter queue back into the pending
 * queue at its original priority, resetting its attempt count so it
 * gets the full retry budget again.
 *
 * This is the "retry this specific failed job" action on the DLQ screen.
 * The requeue also emits job:requeued on the event bus so every connected
 * browser sees the job disappear from the DLQ list in real time.
 */
dlqRouter.post("/:id/requeue", async (req: Request, res: Response) => {
  const id = req.params.id as string;

  try {
    const raw = await findInDlq(id);

    if (!raw) {
      res.status(404).json({ error: `Job ${id} not found in DLQ` });
      return;
    }

    const job: Job = JSON.parse(raw);

    // Reset attempt count so the job gets a fresh retry budget.
    // Strip the DLQ-specific fields (lastError, failedAt) — those
    // only belong on a dead record, not on a live job about to be retried.
    const { lastError: _lastError, failedAt: _failedAt, ...jobFields } = job;
    const requeuedJob = {
      ...jobFields,
      attempts: 0,
    };
    const priority = job.priority ?? "medium";
    const targetList = QUEUE_KEYS.pending[priority];

    // Remove from DLQ first, then push to pending.
    // Two separate commands (not atomic) — acceptable here because this
    // is a human-triggered action from the dashboard, not high-concurrency
    // automated processing. The gap is negligible in practice.
    await redis.lrem(QUEUE_KEYS.dead, 1, raw);
    await redis.rpush(targetList, JSON.stringify(requeuedJob));

    // Announce the requeue so the DLQ screen removes this job from its
    // list immediately, without waiting for a page refresh or next poll.
    queueEvents.emit("job:requeued", { jobId: id });

    res.json({ success: true, jobId: id, requeuedTo: targetList });
  } catch (err) {
    console.error(
      "[api] POST /api/dlq/:id/requeue error:",
      (err as Error).message,
    );
    res.status(500).json({ error: "Failed to requeue job" });
  }
});

/**
 * POST /api/dlq/requeue-all
 *
 * Moves every job in the DLQ back into its priority-tiered pending queue,
 * resetting all attempt counts. Use after fixing the underlying bug that
 * caused the failures — blindly requeuing without fixing the root cause
 * will just refill the DLQ with the same jobs.
 */
dlqRouter.post("/requeue-all", async (_req: Request, res: Response) => {
  try {
    const entries = await redis.lrange(QUEUE_KEYS.dead, 0, -1);

    if (entries.length === 0) {
      res.json({ success: true, requeued: 0 });
      return;
    }

    let requeued = 0;

    for (const raw of entries) {
      const job: Job = JSON.parse(raw);
      const { lastError: _lastError, failedAt: _failedAt, ...jobFields } = job;

      const requeuedJob = { ...jobFields, attempts: 0 };
      const targetList = QUEUE_KEYS.pending[job.priority ?? "medium"];

      await redis.rpush(targetList, JSON.stringify(requeuedJob));
      queueEvents.emit("job:requeued", { jobId: job.id });
      requeued++;
    }

    // Clear the entire DLQ after successfully moving everything out.
    await redis.del(QUEUE_KEYS.dead);

    res.json({ success: true, requeued });
  } catch (err) {
    console.error(
      "[api] POST /api/dlq/requeue-all error:",
      (err as Error).message,
    );
    res.status(500).json({ error: "Failed to requeue all jobs" });
  }
});

/**
 * DELETE /api/dlq/:id
 *
 * Permanently removes a single job from the DLQ with no retry.
 * Use when a job is genuinely invalid (bad payload, test data, etc.)
 * and should never be processed. This action is irreversible.
 */
dlqRouter.delete("/:id", async (req: Request, res: Response) => {
  const id = req.params.id as string;

  try {
    const raw = await findInDlq(id);

    if (!raw) {
      res.status(404).json({ error: `Job ${id} not found in DLQ` });
      return;
    }

    await redis.lrem(QUEUE_KEYS.dead, 1, raw);

    res.json({ success: true, jobId: id });
  } catch (err) {
    console.error("[api] DELETE /api/dlq/:id error:", (err as Error).message);
    res.status(500).json({ error: "Failed to delete job" });
  }
});

// ── Helper ────────────────────────────────────────────────────────────────

// Scans the DLQ list for a job with the given ID and returns its raw JSON
// string (needed for LREM, which matches by exact value, not by ID field).
async function findInDlq(id: string): Promise<string | null> {
  const entries = await redis.lrange(QUEUE_KEYS.dead, 0, -1);

  for (const raw of entries) {
    const parsed = JSON.parse(raw);
    if (parsed.id === id) return raw;
  }

  return null;
}
