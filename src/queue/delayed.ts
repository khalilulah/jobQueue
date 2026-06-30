import { redis } from "../config/redis";
import { QUEUE_KEYS } from "./keys";
import { Job } from "./types";

const POLL_INTERVAL_MS = 1000;

/**
 * Starts a recurring poller that checks `queue:delayed` for any jobs whose
 * runAt timestamp has passed, and moves them into `queue:pending` so a
 * worker's BRPOPLPUSH can pick them up.
 *
 * This single mechanism serves two purposes we discussed as one and the
 * same: scheduled/delayed jobs, and retry backoff. Both just add an entry
 * to this sorted set scored by "when should this become eligible again" —
 * this poller doesn't know or care which case it's looking at.
 */
export async function startDelayedJobPoller(): Promise<void> {
  console.log(
    "[poller] started, checking queue:delayed every",
    POLL_INTERVAL_MS,
    "ms",
  );

  // eslint-disable-next-line no-constant-condition
  while (true) {
    await checkDelayedJobs();
    await sleep(POLL_INTERVAL_MS);
  }
}

async function checkDelayedJobs(): Promise<void> {
  const now = Date.now();

  // ZRANGEBYSCORE with min=0 catches every job scored from the very
  // beginning of time up to right now — i.e. everything that's due.
  // Jobs scored further in the future simply don't show up in this result,
  // no matter how many of them exist — this is the efficient "what's ready"
  // query a plain List can't give us, which is the whole reason we used
  // a sorted set for this in the first place.
  const dueJobs = await redis.zrangebyscore(QUEUE_KEYS.delayed, 0, now);

  for (const rawJob of dueJobs) {
    await moveToPending(rawJob);
  }
}

async function moveToPending(rawJob: string): Promise<void> {
  // ZREM's return value is the number of members actually removed.
  // If it's 1, WE were the one who successfully claimed this job — safe
  // to push it onward. If it's 0, some other process already claimed it
  // a moment earlier (relevant if you ever run more than one poller
  // instance), so we do nothing further with it.
  const removedCount = await redis.zrem(QUEUE_KEYS.delayed, rawJob);

  if (removedCount === 1) {
    // We need to know which priority tier this job belongs in — it was
    // carried all the way through from the original enqueue() call, so
    // parsing the job is enough to route it correctly.
    //
    // Fallback to 'medium' guards against jobs written to Redis before the
    // `priority` field existed — JSON.parse doesn't actually verify the
    // shape matches `Job`, it just trusts the type annotation, so old data
    // genuinely can be missing this field at runtime. TypeScript's error
    // here was correctly flagging that real possibility, not being overly
    // cautious — without this fallback, an old job would crash trying to
    // index `undefined` into QUEUE_KEYS.pending.
    const job: Job = JSON.parse(rawJob);
    const priority = job.priority ?? "medium";
    const targetList = QUEUE_KEYS.pending[priority];

    await redis.rpush(targetList, rawJob);
    console.log(`[poller] moved due job from delayed -> pending:${priority}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
