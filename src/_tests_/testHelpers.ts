import { redis } from "../config/redis";
import { QUEUE_KEYS } from "../queue/keys";

/**
 * Clears every queue-related key in Redis. Call this before each test
 * (or test suite) that needs a known-empty starting state — without it,
 * leftover jobs from a previous test run can make results ambiguous,
 * exactly the kind of confusing log output we ran into during manual
 * testing before this suite existed.
 */
export async function flushQueues(): Promise<void> {
  await redis.del(
    QUEUE_KEYS.pending.high,
    QUEUE_KEYS.pending.medium,
    QUEUE_KEYS.pending.low,
    QUEUE_KEYS.processing,
    QUEUE_KEYS.delayed,
    QUEUE_KEYS.dead,
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
