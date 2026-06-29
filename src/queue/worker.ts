import { redis, createBlockingConnection } from "../config/redis";
import { QUEUE_KEYS } from "./keys";
import { Job } from "./types";
import { jobRegistry } from "../jobs/registry";

/**
 * Starts an infinite worker loop on a dedicated blocking connection.
 * Happy path only for now: pop -> run handler -> remove from processing.
 * Failure handling (retry/backoff/DLQ) is deliberately not here yet.
 */
export async function startWorker(): Promise<void> {
  const blockingConnection = createBlockingConnection();

  console.log("[worker] started, waiting for jobs...");

  // eslint-disable-next-line no-constant-condition
  while (true) {
    // BRPOPLPUSH blocks this connection until a job is available, then
    // atomically moves it from pending -> processing in one step ( this is what prevents the "job vanishes if the worker crashes right after taking it" problem).
    // ioredis's signature for this command takes the timeout as the last
    // argument; 0 means "block forever, no timeout".
    const rawJob = await blockingConnection.brpoplpush(
      QUEUE_KEYS.pending,
      QUEUE_KEYS.processing,
      0,
    );

    if (!rawJob) {
      // With timeout 0 this branch should never actually hit, but keeping
      // it here makes the loop's logic explicit rather than assuming.
      continue;
    }

    await processJob(rawJob);
  }
}

async function processJob(rawJob: string): Promise<void> {
  const job: Job = JSON.parse(rawJob);

  console.log(`[worker] picked up job ${job.id} (type: ${job.type})`);

  const handler = jobRegistry[job.type];

  if (!handler) {
    console.error(`[worker] no handler registered for job type "${job.type}"`);
    // No retry logic yet — for now we just leave it sitting in `processing`.
    // We'll address this properly once failure handling exists.
    return;
  }

  try {
    await handler(job.payload);

    // Success: remove this exact job string from the processing list.
    // We pass the same raw string we received from BRPOPLPUSH, since LREM
    // matches by exact value — re-serializing the parsed object could
    // theoretically produce a different string (key ordering, spacing),
    // so we keep and reuse the original string rather than re-stringifying.
    await redis.lrem(QUEUE_KEYS.processing, 1, rawJob);

    console.log(`[worker] job ${job.id} completed and removed from processing`);
  } catch (err) {
    console.error(`[worker] job ${job.id} failed:`, (err as Error).message);
    // Failure handling (retry/backoff/DLQ) comes next — left as a stub.
  }
}
