import { randomUUID } from "crypto";
import { redis } from "../config/redis";
import { QUEUE_KEYS } from "./keys";
import { Job, EnqueueOptions } from "./types";

const DEFAULT_MAX_RETRIES = 3;

/**
 * Adds a new job to the pending queue, ready for any worker to pick up.
 *
 * Mechanically this is just RPUSH onto queue:pending — the "push right, pop left"
 * convention we settled on for FIFO ordering. The interesting part isn't the
 * Redis call, it's building a complete, well-formed Job object before it goes in.
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
    createdAt: Date.now(),
  };

  // Redis lists store strings, not objects — every value crossing the
  // network to Redis has to be serialized. JSON is the obvious choice:
  // human-readable (you can inspect it with redis-cli for debugging),
  // and trivially parsed back into the same shape on the way out.
  await redis.rpush(QUEUE_KEYS.pending, JSON.stringify(job));

  return job.id;
}
