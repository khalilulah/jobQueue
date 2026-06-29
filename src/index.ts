import { startWorker } from "./queue/worker";
import { enqueue } from "./queue/producer";

async function main() {
  // Start the worker loop. We deliberately do NOT await this — startWorker()
  // contains a `while (true)` loop that never resolves, so awaiting it here
  // would block this function from ever reaching the enqueue() calls below.
  // Letting it run un-awaited is exactly what lets producer and worker code
  // run concurrently within the same process.
  startWorker().catch((err) => {
    console.error("[worker] fatal error:", err);
    process.exit(1);
  });

  // Act as a producer: push a couple of test jobs in, a moment apart,
  // so you can watch the worker pick each one up live in the same log output.
  await new Promise((resolve) => setTimeout(resolve, 1000));

  const id1 = await enqueue({
    type: "send_welcome_email",
    payload: { userId: 1, email: "khalil@example.com" },
  });
  console.log("[producer] enqueued job:", id1);

  await new Promise((resolve) => setTimeout(resolve, 2000));

  const id2 = await enqueue({
    type: "send_welcome_email",
    payload: { userId: 2, email: "someone@example.com" },
  });
  console.log("[producer] enqueued job:", id2);

  await new Promise((resolve) => setTimeout(resolve, 1000));

  // Deliberately failing job, with a low maxRetries so we can watch it
  // exhaust retries and land in the dead-letter queue without waiting long.
  const id3 = await enqueue({
    type: "always_fails",
    payload: {},
    maxRetries: 2,
  });
  console.log("[producer] enqueued job:", id3);

  // No process.exit() here — the worker's while(true) loop must keep the
  // process alive indefinitely, the same way it would in production.
}

main();
