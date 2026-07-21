import { randomUUID } from "crypto";
import { redis } from "../config/redis";
import { QUEUE_KEYS, Priority } from "./keys";
import { Job, EnqueueOptions } from "./types";
import { queueEvents } from "../events/queueEvents";

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_PRIORITY: Priority = "medium";

/**
 * Adds a new job to the appropriate priority-tiered pending list, ready
 * for any worker to pick up. Mechanically this is just RPUSH onto one of
 * the three queue:pending:* lists — the interesting part is the worker
 * checking those lists in priority order, which is where priority
 * actually takes effect (see worker.ts).
 */
export async function enqueue<TPayload>(
  options: EnqueueOptions<TPayload>,
): Promise<string> {
  const job: Job<TPayload> = {
    id: randomUUID(),
    type: options.type,
    payload: options.payload,
    attempts: 0,
    maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES,
    priority: options.priority ?? DEFAULT_PRIORITY,
    createdAt: Date.now(),
  };

  const targetList = QUEUE_KEYS.pending[job.priority];

  await redis.rpush(targetList, JSON.stringify(job));

  // New: announce the enqueue on the event bus. The producer doesn't know
  // who's listening — it just fires the event and returns.
  queueEvents.emit("job:enqueued", job as Job);

  return job.id;
}
