import { redis, createBlockingConnection } from "../config/redis";
import { QUEUE_KEYS, PRIORITY_ORDER } from "./keys";
import { Job } from "./types";
import { jobRegistry } from "../jobs/registry";
import { handleFailure } from "./retry";
import { sleep } from "./reaper";

let shuttingDown = false;
let activeJobCount = 0;

/**
 * Signals the worker loop to stop picking up NEW jobs after its current
 * blocking call returns. Does not interrupt a job already in progress —
 * see waitForActiveJobs() for how the caller waits for in-flight work to
 * actually finish before the process exits.
 */
export function requestShutdown(): void {
  shuttingDown = true;
}

/**
 * Resolves once no job is currently being processed by this worker.
 * Used during shutdown to avoid exiting the process while a handler is
 * still mid-execution.
 */
export async function waitForActiveJobs(): Promise<void> {
  while (activeJobCount > 0) {
    await sleep(100);
  }
}

/**
 * Starts an infinite worker loop on a dedicated blocking connection.
 *
 * Priority: instead of a single BRPOPLPUSH on one list, we first do a
 * quick, NON-blocking sweep across the priority tiers in order (high,
 * medium, low) using plain RPOPLPUSH. If all three are empty, only then
 * do we fall back to a short blocking wait on the lowest tier — just to
 * avoid a tight, CPU-spinning empty loop while genuinely idle.
 */
export async function startWorker(): Promise<void> {
  const blockingConnection = createBlockingConnection();

  console.log("[worker] started, waiting for jobs...");

  while (!shuttingDown) {
    const rawJob = await claimNextJob(blockingConnection);

    if (!rawJob) {
      continue;
    }

    activeJobCount++;
    try {
      await processJob(rawJob);
    } finally {
      activeJobCount--;
    }
  }

  console.log("[worker] shutdown requested, loop exited");
}

async function claimNextJob(
  blockingConnection: ReturnType<typeof createBlockingConnection>,
): Promise<string | null> {
  // Non-blocking sweep, highest priority first. RPOPLPUSH returns null
  // immediately (not blocking) if the list is empty, letting us move on
  // to check the next tier without waiting.
  for (const priority of PRIORITY_ORDER) {
    const rawJob = await redis.rpoplpush(
      QUEUE_KEYS.pending[priority],
      QUEUE_KEYS.processing,
    );
    if (rawJob) {
      return rawJob;
    }
  }

  // All three tiers were empty at the moment we checked. Rather than loop
  // again immediately (wasted CPU/network), block briefly on the lowest
  // tier — if ANY job arrives on ANY tier while we're blocked here, we'll
  // pick it up on our next loop iteration's sweep almost immediately
  // afterward, since this blocking call has a short timeout rather than 0.
  const rawJob = await blockingConnection.brpoplpush(
    QUEUE_KEYS.pending.low,
    QUEUE_KEYS.processing,
    2, // seconds — short, so we re-check shuttingDown and re-sweep regularly
  );

  return rawJob;
}

async function processJob(rawJob: string): Promise<void> {
  const job: Job = JSON.parse(rawJob);

  // Stamp when this job actually entered processing. We can't do this as
  // part of BRPOPLPUSH itself (it moves opaque strings, it can't modify
  // their contents) so this is a necessary, separate follow-up step —
  // see the discussion on why this small gap is acceptable.
  const stampedJob: Job = {
    ...job,
    processingStartedAt: Date.now(),
  };
  const stampedRawJob = JSON.stringify(stampedJob);

  // Swap the unstamped entry for the stamped one. From this point on,
  // `stampedRawJob` — not the original `rawJob` — is what's actually
  // sitting in `queue:processing`, so all later LREM calls must use it.
  await redis.lrem(QUEUE_KEYS.processing, 1, rawJob);
  await redis.rpush(QUEUE_KEYS.processing, stampedRawJob);

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
    // We use stampedRawJob since that's what's actually sitting in
    // `processing` now (see the swap above) — using the original rawJob
    // here would silently fail to remove anything, since it's no longer
    // present in the list.
    await redis.lrem(QUEUE_KEYS.processing, 1, stampedRawJob);

    console.log(`[worker] job ${job.id} completed and removed from processing`);
  } catch (err) {
    console.error(`[worker] job ${job.id} failed:`, (err as Error).message);
    await handleFailure(stampedRawJob, stampedJob, err as Error);
  }
}
