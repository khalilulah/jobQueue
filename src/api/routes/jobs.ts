import { Router, Request, Response } from "express";
import { redis } from "../../config/redis";
import { QUEUE_KEYS, Priority, PRIORITY_ORDER } from "../../queue/keys";

export const jobsRouter = Router();

// The five states a job can be in, each backed by a different Redis
// structure. This mapping is what lets a single endpoint serve all of them.
type JobStatus = "queued" | "active" | "delayed" | "completed" | "failed";

/**
 * GET /api/jobs?status=queued|active|delayed|completed|failed
 *                &priority=high|medium|low   (only relevant for status=queued)
 *                &limit=50&offset=0
 *
 * Returns a paginated list of jobs. Each status maps to a different Redis
 * structure — the handler reads from the right place based on the query param
 * rather than maintaining a separate index. Pagination is done with LRANGE
 * (for Lists) and ZRANGE (for the Sorted Set), both of which support offset
 * and count natively, so we never read the entire list into memory.
 */
jobsRouter.get("/", async (req: Request, res: Response) => {
  const status = (req.query.status as JobStatus) ?? "queued";
  const priority = req.query.priority as Priority | undefined;
  const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
  const offset = parseInt(req.query.offset as string) || 0;

  try {
    const jobs = await fetchJobsByStatus(status, { priority, limit, offset });
    res.json({ status, jobs, limit, offset, count: jobs.length });
  } catch (err) {
    console.error("[api] GET /api/jobs error:", (err as Error).message);
    res.status(500).json({ error: "Failed to fetch jobs" });
  }
});

/**
 * GET /api/jobs/:id
 *
 * Finds a single job by ID across every Redis structure. This is a scan
 * operation — we check each structure in turn until we find a match.
 * This is acceptable for a dashboard's detail-panel lookup (one request
 * per user click) but would not be suitable for bulk operations.
 *
 * In a production system you'd add a secondary Redis Hash
 * (queue:job:index) mapping jobId → current location, but that adds
 * write complexity to every state transition. For a portfolio dashboard
 * the scan approach is the right tradeoff.
 */
jobsRouter.get("/:id", async (req: Request, res: Response) => {
  const id = req.params.id as string;

  try {
    const job = await findJobById(id);

    if (!job) {
      res.status(404).json({ error: `Job ${id} not found` });
      return;
    }

    res.json(job);
  } catch (err) {
    console.error("[api] GET /api/jobs/:id error:", (err as Error).message);
    res.status(500).json({ error: "Failed to fetch job" });
  }
});

// ── Helpers ──────────────────────────────────────────────────────────────

async function fetchJobsByStatus(
  status: JobStatus,
  options: { priority?: Priority; limit: number; offset: number },
): Promise<unknown[]> {
  const { priority, limit, offset } = options;
  const start = offset;
  const end = offset + limit - 1;

  switch (status) {
    case "queued": {
      // If a specific priority tier is requested, read only that list.
      // If not, read all three tiers and merge, preserving within-tier FIFO
      // order but interleaving tiers high → medium → low (same order the
      // worker checks them, so the list reflects real processing order).
      const tiers = priority ? [priority] : PRIORITY_ORDER;

      // Fetch from each tier in parallel, then flatten. We read more than
      // the requested page from each tier here so the merged result has
      // enough items to slice after combining. In practice most deployments
      // don't need cross-tier pagination to be perfectly precise — the
      // frontend filters by priority anyway, making single-tier the common case.
      const results = await Promise.all(
        tiers.map((tier) => redis.lrange(QUEUE_KEYS.pending[tier], 0, end)),
      );

      const merged = results
        .flat()
        .map(parseWithStatus("queued"))
        .slice(start, start + limit);

      return merged;
    }

    case "active": {
      const entries = await redis.lrange(QUEUE_KEYS.processing, start, end);
      return entries.map(parseWithStatus("active"));
    }

    case "delayed": {
      // ZRANGE with WITHSCORES returns [member, score, member, score, ...].
      // We pair them up to attach the runAt timestamp as a readable field.
      const entries = await redis.zrange(
        QUEUE_KEYS.delayed,
        start,
        end,
        "WITHSCORES",
      );
      return pairZrangeWithScores(entries, "delayed");
    }

    case "completed": {
      // Completed log is newest-first from the right end of the list
      // (RPUSH adds to the right, so right = most recent).
      const entries = await redis.lrange(
        QUEUE_KEYS.completed,
        -(end + 1),
        -(start + 1),
      );
      return entries.map(parseWithStatus("completed")).reverse();
    }

    case "failed": {
      const entries = await redis.lrange(QUEUE_KEYS.dead, start, end);
      return entries.map(parseWithStatus("failed"));
    }

    default:
      return [];
  }
}

async function findJobById(id: string): Promise<unknown | null> {
  // Check each structure. Order chosen to match "most likely current
  // location" — active and queued are the most common states during
  // normal operation, so we check those first.
  const searches: Array<() => Promise<unknown | null>> = [
    () => scanListForId(QUEUE_KEYS.processing, id, "active"),
    () => scanListForId(QUEUE_KEYS.pending.high, id, "queued"),
    () => scanListForId(QUEUE_KEYS.pending.medium, id, "queued"),
    () => scanListForId(QUEUE_KEYS.pending.low, id, "queued"),
    () => scanSortedSetForId(QUEUE_KEYS.delayed, id, "delayed"),
    () => scanListForId(QUEUE_KEYS.completed, id, "completed"),
    () => scanListForId(QUEUE_KEYS.dead, id, "failed"),
  ];

  for (const search of searches) {
    const result = await search();
    if (result) return result;
  }

  return null;
}

async function scanListForId(
  key: string,
  id: string,
  status: string,
): Promise<unknown | null> {
  // LRANGE 0 -1 reads the entire list. For a detail-panel lookup
  // this is acceptable — it happens once per user click, not in a loop.
  const entries = await redis.lrange(key, 0, -1);

  for (const raw of entries) {
    const parsed = JSON.parse(raw);
    if (parsed.id === id || parsed.jobId === id) {
      return { ...parsed, status };
    }
  }

  return null;
}

async function scanSortedSetForId(
  key: string,
  id: string,
  status: string,
): Promise<unknown | null> {
  const entries = await redis.zrange(key, 0, -1, "WITHSCORES");
  const pairs = pairZrangeWithScores(entries, status);

  for (const job of pairs) {
    const j = job as { id?: string; jobId?: string };
    if (j.id === id || j.jobId === id) return job;
  }

  return null;
}

// ZRANGE WITHSCORES returns a flat array: [member, score, member, score...]
// This helper pairs them into objects with a readable runAt field.
function pairZrangeWithScores(entries: string[], status: string): unknown[] {
  const result: unknown[] = [];

  for (let i = 0; i < entries.length; i += 2) {
    const parsed = JSON.parse(entries[i]);
    const runAt = parseInt(entries[i + 1]);
    result.push({ ...parsed, status, runAt });
  }

  return result;
}

// Parses a raw JSON string from Redis and attaches a status field so the
// frontend knows which state the job is currently in.
function parseWithStatus(status: string) {
  return (raw: string) => ({ ...JSON.parse(raw), status });
}
