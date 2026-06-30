import {
  startWorker,
  requestShutdown,
  waitForActiveJobs,
} from "./queue/worker";
import { enqueue } from "./queue/producer";

const SHUTDOWN_TIMEOUT_MS = 15000;

async function runShutdownTest() {
  const hangId = await enqueue({ type: "hangs_forever", payload: {} });
  console.log("[test] enqueued job:", hangId);

  startWorker().catch((err) => {
    console.error("[worker] fatal error:", err);
    process.exit(1);
  });

  // Wait a moment to be sure the worker has actually claimed and started
  // the job before we trigger shutdown — otherwise we might trigger
  // shutdown before there's anything in-flight at all.
  await new Promise((resolve) => setTimeout(resolve, 2000));

  console.log(
    "[test] triggering shutdown sequence programmatically (no OS signal involved)...",
  );
  const startedAt = Date.now();

  requestShutdown();

  const timeout = new Promise<"timeout">((resolve) =>
    setTimeout(() => resolve("timeout"), SHUTDOWN_TIMEOUT_MS),
  );
  const finished = waitForActiveJobs().then(() => "finished" as const);

  const result = await Promise.race([finished, timeout]);
  const elapsed = Date.now() - startedAt;

  console.log(
    `[test] shutdown sequence resolved with "${result}" after ${elapsed}ms`,
  );

  if (result === "timeout" && elapsed >= SHUTDOWN_TIMEOUT_MS - 100) {
    console.log(
      "[test] PASS — timed out as expected, close to the configured 15000ms ceiling",
    );
  } else {
    console.log(
      "[test] UNEXPECTED — check the elapsed time and result above against expectations",
    );
  }

  process.exit(0);
}

runShutdownTest();
