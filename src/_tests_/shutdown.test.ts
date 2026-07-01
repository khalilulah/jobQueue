import { redis } from "../config/redis";
import { enqueue } from "../queue/producer";
import {
  startWorker,
  requestShutdown,
  waitForActiveJobs,
  resetWorkerState,
} from "../queue/worker";
import { jobRegistry } from "../jobs/registry";
import { flushQueues, sleep } from "./testHelpers";

describe("graceful shutdown", () => {
  let stopSlowJob = false;

  beforeEach(async () => {
    resetWorkerState();
    stopSlowJob = false;
    await flushQueues();
  });

  afterAll(async () => {
    // Signal the in-flight handler to stop on its next polling tick,
    // then give it a moment to actually exit before we disconnect Redis.
    stopSlowJob = true;
    await sleep(500);
    await flushQueues();
    redis.disconnect();
  });

  it("waits for an in-flight job, up to a timeout, rather than exiting instantly or hanging forever", async () => {
    const SHUTDOWN_TIMEOUT_MS = 5000;
    let jobStarted = false;

    jobRegistry["slow_test_job"] = async () => {
      jobStarted = true;
      // Poll the stop flag rather than sleeping a fixed long time —
      // this lets afterAll cleanly signal the handler to exit rather
      // than leaving it orphaned against a closed Redis connection.
      while (!stopSlowJob) {
        await sleep(100);
      }
    };

    await enqueue({ type: "slow_test_job", payload: {} });

    startWorker().catch(() => {});

    while (!jobStarted) {
      await sleep(50);
    }

    const startedAt = Date.now();
    requestShutdown();

    const timeout = new Promise<"timeout">((resolve) =>
      setTimeout(() => resolve("timeout"), SHUTDOWN_TIMEOUT_MS),
    );
    const finished = waitForActiveJobs().then(() => "finished" as const);

    const result = await Promise.race([finished, timeout]);
    const elapsed = Date.now() - startedAt;

    expect(result).toBe("timeout");
    expect(elapsed).toBeGreaterThanOrEqual(SHUTDOWN_TIMEOUT_MS - 200);
    expect(elapsed).toBeLessThan(SHUTDOWN_TIMEOUT_MS + 2000);
  });
});
