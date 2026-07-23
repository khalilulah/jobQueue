import { Router, Request, Response } from "express";
import { redis } from "../../config/redis";
import { QUEUE_KEYS } from "../../queue/keys";

export const statsRouter = Router();

/**
 * GET /api/stats
 *
 * Returns a single snapshot of every number the Overview screen's cards
 * need. This is what the frontend fetches on first load — after that,
 * stats:update WebSocket events keep the numbers live without further
 * REST calls.
 *
 * All six Redis queries run in parallel via Promise.all so this resolves
 * in one round-trip time rather than six sequential ones.
 */
statsRouter.get("/", async (_req: Request, res: Response) => {
  try {
    const [high, medium, low, processing, delayed, dlq, completed] =
      await Promise.all([
        redis.llen(QUEUE_KEYS.pending.high),
        redis.llen(QUEUE_KEYS.pending.medium),
        redis.llen(QUEUE_KEYS.pending.low),
        redis.llen(QUEUE_KEYS.processing),
        redis.zcard(QUEUE_KEYS.delayed),
        redis.llen(QUEUE_KEYS.dead),
        redis.llen(QUEUE_KEYS.completed),
      ]);

    res.json({
      queueDepth: {
        high,
        medium,
        low,
        total: high + medium + low,
      },
      processing,
      delayed,
      dlq,
      recentlyCompleted: completed,
      timestamp: Date.now(),
    });
  } catch (err) {
    console.error("[api] GET /api/stats error:", (err as Error).message);
    res.status(500).json({ error: "Failed to fetch stats" });
  }
});
