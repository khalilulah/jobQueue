import {
  startWorker,
  requestShutdown,
  waitForActiveJobs,
} from "./queue/worker";
import { startDelayedJobPoller } from "./queue/delayed";
import { startReaper } from "./queue/reaper";

const SHUTDOWN_TIMEOUT_MS = 15000; // give in-flight jobs up to 15s, then force-exit anyway

async function main(): Promise<void> {
  console.log("[main] starting job queue system...");

  // Each of these contains a `while (true)` loop that never resolves on
  // its own, so each is started un-awaited — this is what lets the worker,
  // poller, and reaper all run concurrently within this single process.
  // (In a real deployment, these would more likely be separate processes/
  // containers, scaled independently — combining them here is a local-dev
  // convenience, not a structural requirement.)
  startWorker().catch((err) => {
    console.error("[worker] fatal error:", err);
    process.exit(1);
  });

  startDelayedJobPoller().catch((err) => {
    console.error("[poller] fatal error:", err);
    process.exit(1);
  });

  startReaper().catch((err) => {
    console.error("[reaper] fatal error:", err);
    process.exit(1);
  });

  console.log("[main] all processes started");
}

/**
 * Handles SIGINT (Ctrl+C) and SIGTERM (sent by process managers, Docker,
 * Kubernetes, etc.) for an orderly shutdown: stop claiming new jobs, wait
 * for any in-flight job to finish, then exit — bounded by
 * SHUTDOWN_TIMEOUT_MS so an unexpectedly slow handler can't block a
 * deploy/restart indefinitely. If the timeout is hit, the abandoned job
 * is left for the reaper to reclaim on the next worker's first sweep.
 *
 * This only covers an orderly shutdown request — it does nothing for a
 * hard crash or `kill -9`, which the reaper covers separately.
 */
async function handleShutdownSignal(signal: string): Promise<void> {
  console.log(
    `[shutdown] received ${signal}, finishing in-flight work before exit...`,
  );

  requestShutdown();

  const timeout = new Promise<"timeout">((resolve) =>
    setTimeout(() => resolve("timeout"), SHUTDOWN_TIMEOUT_MS),
  );
  const finished = waitForActiveJobs().then(() => "finished" as const);

  const result = await Promise.race([finished, timeout]);

  if (result === "timeout") {
    console.warn(
      `[shutdown] timed out after ${SHUTDOWN_TIMEOUT_MS}ms waiting for in-flight work — forcing exit. Any abandoned job will be recovered by the reaper.`,
    );
  } else {
    console.log("[shutdown] clean shutdown complete, exiting");
  }

  process.exit(0);
}

process.on("SIGINT", () => handleShutdownSignal("SIGINT"));
process.on("SIGTERM", () => handleShutdownSignal("SIGTERM"));

main();
