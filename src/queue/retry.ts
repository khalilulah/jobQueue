import { redis } from "../config/redis";
import { queueEvents } from "../events/queueEvents";
import { QUEUE_KEYS } from "./keys";
import { Job } from "./types";

const BASE_DELAY_MS = 1000; // 1 second
const MULTIPLIER = 2;

/**
 * Computes how long to wait before the next retry attempt, using
 * exponential backoff with full jitter.
 *
 * attemptNumber is 1-indexed: the delay BEFORE the 1st retry (i.e. after
 * the job has failed once already) is computed with attemptNumber = 1.
 *
 * Formula: delay = (random * (base * multiplier^attemptNumber))
 *
 * Pure function, deliberately: no Redis, no Date.now() side effects baked
 * in beyond what's passed in implicitly via Math.random() — this makes it
 * trivial to reason about and test independent of any queue mechanics.
 */
export function calculateBackoffDelay(attemptNumber: number): number {
  const maxDelay = BASE_DELAY_MS * Math.pow(MULTIPLIER, attemptNumber);
  return Math.floor(Math.random() * maxDelay);
}

/**
 * Called when a job's handler has thrown. Decides whether to schedule a
 * retry (via the delayed sorted set) or, if retries are exhausted, move
 * the job to the dead-letter queue. Either way, the job comes out of
 * `processing` — it's no longer actively being worked on.
 *
 * rawJob is the exact original string popped from `processing`, so we can
 * LREM it precisely (same reasoning as in worker.ts's success path).
 */
export async function handleFailure(
  rawJob: string,
  job: Job,
  error: Error,
): Promise<void> {
  const updatedJob: Job = {
    ...job,
    attempts: job.attempts + 1,
  };

  if (updatedJob.attempts >= updatedJob.maxRetries) {
    console.log(
      `[retry] job ${job.id} exhausted retries (${updatedJob.attempts}/${updatedJob.maxRetries}) — moving to dead-letter queue`,
    );

    const deadJobRecord = {
      ...updatedJob,
      lastError: error.message,
      failedAt: Date.now(),
    };

    await redis.rpush(QUEUE_KEYS.dead, JSON.stringify(deadJobRecord));

    // NEW: announce DLQ move so the browser's DLQ screen updates live.
    queueEvents.emit("job:dlq", {
      jobId: job.id,
      type: job.type,
      attempts: updatedJob.attempts,
      lastError: error.message,
      failedAt: deadJobRecord.failedAt,
    });
  } else {
    const delay = calculateBackoffDelay(updatedJob.attempts);
    const runAt = Date.now() + delay;

    console.log(
      `[retry] job ${job.id} failed (attempt ${updatedJob.attempts}/${updatedJob.maxRetries}) — retrying in ${delay}ms`,
    );

    // Sorted set, scored by the timestamp it becomes eligible again —
    // this is the delayed-jobs mechanism doing double duty for backoff,
    // exactly as discussed: one mechanism, two use cases.
    await redis.zadd(QUEUE_KEYS.delayed, runAt, JSON.stringify(updatedJob));

    // NEW: announce the retry schedule so the browser can show the job
    // in a "waiting to retry" state with a real timestamp, not a spinner.
    queueEvents.emit("job:failed", {
      jobId: job.id,
      type: job.type,
      attempt: updatedJob.attempts,
      maxRetries: updatedJob.maxRetries,
      nextRetryAt: runAt,
      error: error.message,
    });
  }

  // Either way: this job is no longer "in flight", so it comes out of
  // processing. We remove the ORIGINAL raw string (pre-attempt-increment),
  // since that's the exact value BRPOPLPUSH put there.
  await redis.lrem(QUEUE_KEYS.processing, 1, rawJob);
}
