import { redis } from "../config/redis";
import { enqueue } from "../queue/producer";
import {
  startWorker,
  requestShutdown,
  waitForActiveJobs,
  resetWorkerState,
} from "../queue/worker";
import { startDelayedJobPoller } from "../queue/delayed";
import { jobRegistry } from "../jobs/registry";
import { QUEUE_KEYS } from "../queue/keys";
import { Job } from "../queue/types";
import { flushQueues, sleep } from "./testHelpers";

describe("retry and dead-letter queue (integration)", () => {
  beforeEach(async () => {
    resetWorkerState();
    await flushQueues();

    jobRegistry["always_fails_test"] = async () => {
      throw new Error("intentional test failure");
    };
  });

  afterEach(async () => {
    requestShutdown();
    await waitForActiveJobs();
    await flushQueues();
    delete jobRegistry["always_fails_test"];
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("moves a job to the dead-letter queue after exhausting maxRetries", async () => {
    const jobId = await enqueue({
      type: "always_fails_test",
      payload: {},
      maxRetries: 2, // low ceiling so this test runs quickly
    });

    startWorker().catch(() => {});
    startDelayedJobPoller().catch(() => {});

    // Backoff delays are small but real (jittered, up to a few seconds
    // per attempt at low attempt numbers) — give enough wall-clock time
    // for both retry cycles to actually play out.
    await sleep(8000);

    const deadEntries = await redis.lrange(QUEUE_KEYS.dead, 0, -1);
    expect(deadEntries).toHaveLength(1);

    const deadJob = JSON.parse(deadEntries[0]) as Job & { lastError: string };
    expect(deadJob.id).toBe(jobId);
    expect(deadJob.attempts).toBe(2);
    expect(deadJob.lastError).toBe("intentional test failure");

    // Confirm it's genuinely gone from every other queue, not duplicated.
    const pendingHigh = await redis.lrange(QUEUE_KEYS.pending.high, 0, -1);
    const pendingMedium = await redis.lrange(QUEUE_KEYS.pending.medium, 0, -1);
    const pendingLow = await redis.lrange(QUEUE_KEYS.pending.low, 0, -1);
    const processing = await redis.lrange(QUEUE_KEYS.processing, 0, -1);
    const delayed = await redis.zrange(QUEUE_KEYS.delayed, 0, -1);

    expect(pendingHigh).toHaveLength(0);
    expect(pendingMedium).toHaveLength(0);
    expect(pendingLow).toHaveLength(0);
    expect(processing).toHaveLength(0);
    expect(delayed).toHaveLength(0);
  });
});
