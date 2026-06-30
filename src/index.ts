import {
  startWorker,
  requestShutdown,
  waitForActiveJobs,
} from "./queue/worker";
import { startDelayedJobPoller } from "./queue/delayed";
import { startReaper } from "./queue/reaper";
import { enqueue } from "./queue/producer";

async function main() {
  // ISOLATED SHUTDOWN TEST — single hangs_forever job, worker only.
  // Poller and reaper are deliberately NOT started here, so the only thing
  // that can end this job's processing is either (a) the handler itself
  // finishing 60s later, or (b) our shutdown timeout forcing an exit. This
  // isolates exactly what we're testing: does Ctrl+C wait up to
  // SHUTDOWN_TIMEOUT_MS and then force-exit, rather than hanging forever
  // OR exiting instantly.
  const hangId = await enqueue({
    type: "hangs_forever",
    payload: {},
  });
  console.log("[producer] enqueued job:", hangId);
  console.log("[test] job enqueued, starting worker now...");

  startWorker().catch((err) => {
    console.error("[worker] fatal error:", err);
    process.exit(1);
  });

  // poller and reaper intentionally not started for this isolated test
}

const SHUTDOWN_TIMEOUT_MS = 15000; // give in-flight jobs up to 15s, then force-exit anyway

/**
 * Handles SIGINT (Ctrl+C) and SIGTERM (sent by process managers, Docker,
 * Kubernetes, etc. when they want a process to stop). The goal: never let
 * the process die while a handler is mid-execution — that's exactly the
 * "abandoned in-flight call" problem we saw with the hangsForever test.
 *
 * Sequence: stop the worker loop from claiming NEW jobs, then wait for
 * whatever job is currently running (if any) to actually finish, THEN
 * exit. Note this only protects against an orderly shutdown request — it
 * does nothing for a hard crash or `kill -9`, which is a separate concern
 * the reaper exists to cover.
 *
 * Crucially: we only wait up to SHUTDOWN_TIMEOUT_MS. If a handler is
 * unbounded (or just much slower than expected), waiting forever would
 * defeat the entire purpose of being able to deploy/restart promptly.
 * After the timeout, we force-exit anyway — the in-flight job becomes
 * exactly the same kind of abandoned call the reaper already knows how
 * to recover, once its visibility timeout elapses on the next worker.
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
      `[shutdown] timed out after ${SHUTDOWN_TIMEOUT_MS}ms waiting for in-flight work — forcing exit. Any abandoned job will be recovered by the reaper on its next sweep.`,
    );
  } else {
    console.log("[shutdown] clean shutdown complete, exiting");
  }

  process.exit(0);
}

process.on("SIGINT", () => handleShutdownSignal("SIGINT"));
process.on("SIGTERM", () => handleShutdownSignal("SIGTERM"));

main();
