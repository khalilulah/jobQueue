import { redis } from "./config/redis";
import { QUEUE_KEYS } from "./queue/keys";

async function flush() {
  const keysToDelete = [
    QUEUE_KEYS.pending.high,
    QUEUE_KEYS.pending.medium,
    QUEUE_KEYS.pending.low,
    QUEUE_KEYS.processing,
    QUEUE_KEYS.delayed,
    QUEUE_KEYS.dead,
  ];

  for (const key of keysToDelete) {
    const deleted = await redis.del(key);
    console.log(`[flush] ${key}: ${deleted ? "cleared" : "was already empty"}`);
  }

  console.log("[flush] done — all queues are now empty");
  process.exit(0);
}

flush();
