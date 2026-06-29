import { redis } from "./config/redis";
import { enqueue } from "./queue/producer";
import { QUEUE_KEYS } from "./queue/keys";

async function main() {
  const id1 = await enqueue({
    type: "send_welcome_email",
    payload: { userId: 1, email: "khalil@example.com" },
  });
  console.log("[test] enqueued job:", id1);

  const id2 = await enqueue({
    type: "send_welcome_email",
    payload: { userId: 2, email: "someone@example.com" },
    maxRetries: 5,
  });
  console.log("[test] enqueued job:", id2);

  // Inspect the raw contents of the pending list directly —
  // this is exactly what redis-cli LRANGE queue:pending 0 -1 would show you.
  const raw = await redis.lrange(QUEUE_KEYS.pending, 0, -1);
  console.log("[test] current contents of queue:pending:");
  raw.forEach((entry) => console.log(JSON.parse(entry)));

  process.exit(0);
}

main();
