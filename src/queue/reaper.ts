import { redis } from "../config/redis";
import { QUEUE_KEYS } from "./keys";
import { Job } from "./types";
import { handleFailure } from "./retry";

const SWEEP_INTERVAL_MS = 5000;
const VISIBILITY_TIMEOUT_MS = 10000; // how long a job can sit in processing before we assume its worker died

/**
 * Starts a recurring sweep of `queue:processing`, looking for jobs that
 * have been sitting there longer than VISIBILITY_TIMEOUT_MS — our signal
 * that the worker handling them likely crashed before finishing (since a
 * live worker would have moved the job out of processing on success or
 * failure well before this threshold).
 *
 * Reclaimed jobs go back through the exact same retry/DLQ decision as an
 * explicit handler failure — from the system's point of view, "the worker
 * died" and "the handler threw" both just mean "this attempt didn't
 * succeed", and should be treated the same way.
 */
export async function startReaper(): Promise<void> {
  console.log(
    "[reaper] started, sweeping queue:processing every",
    SWEEP_INTERVAL_MS,
    "ms",
  );

  // eslint-disable-next-line no-constant-condition
  while (true) {
    await sweepStuckJobs();
    await sleep(SWEEP_INTERVAL_MS);
  }
}

async function sweepStuckJobs(): Promise<void> {
  // LRANGE 0 -1 reads the entire list. This is fine at the scale we're
  // operating at; a production system with a very large processing list
  // would want a more targeted approach, but reading the whole thing
  // every 5 seconds is cheap for realistic job volumes here.
  const entries = await redis.lrange(QUEUE_KEYS.processing, 0, -1);

  for (const rawJob of entries) {
    const job: Job = JSON.parse(rawJob);

    // Missing processingStartedAt is the small gap discussed when wiring
    // up the timestamp — treat it as "definitely stuck" rather than
    // ignoring it, so it still gets reclaimed.
    const startedAt = job.processingStartedAt ?? 0;
    const age = Date.now() - startedAt;

    if (age > VISIBILITY_TIMEOUT_MS) {
      console.log(
        `[reaper] job ${job.id} has been in processing for ${age}ms (limit ${VISIBILITY_TIMEOUT_MS}ms) — reclaiming`,
      );
      await reclaimJob(rawJob, job);
    }
  }
}

async function reclaimJob(rawJob: string, job: Job): Promise<void> {
  // Same atomicity concern as the delayed-job poller: removal must be the
  // thing that decides "did I actually get to claim this". If the job has
  // already been removed by the worker itself (it finished just as the
  // reaper was about to act) or by another reaper sweep, LREM returns 0 and
  // we correctly do nothing further.
  const removedCount = await redis.lrem(QUEUE_KEYS.processing, 1, rawJob);

  if (removedCount === 0) {
    return;
  }

  // From here on, this is functionally identical to a handler throwing —
  // we reuse the exact same retry/DLQ decision, just triggered by a
  // timeout instead of a caught exception.
  await handleFailure(
    rawJob,
    job,
    new Error("processing visibility timeout exceeded"),
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
