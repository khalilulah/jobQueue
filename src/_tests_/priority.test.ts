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

describe("priority ordering (integration)", () => {
  const processedOrder: string[] = [];

  beforeEach(async () => {
    resetWorkerState();
    await flushQueues();
    processedOrder.length = 0;

    // A throwaway handler registered for this test, so we don't need real
    // side effects (sending email, etc.) — just need to record the order
    // jobs were actually processed in.
    jobRegistry["record_order"] = async (payload: { label: string }) => {
      processedOrder.push(payload.label);
    };
  });

  afterEach(async () => {
    requestShutdown();
    await waitForActiveJobs();
    await flushQueues();
    delete jobRegistry["record_order"];
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("processes high priority jobs before medium, and medium before low", async () => {
    // Enqueue in the worst-case order for FIFO: low, medium, high.
    // All three MUST be fully enqueued before the worker starts — if the
    // worker starts concurrently, it could claim the low-priority job
    // before medium/high even exist in Redis, which would prove nothing
    // about priority ordering (this is the exact mistake made — and
    // caught — during manual testing of this feature).
    await enqueue({
      type: "record_order",
      payload: { label: "low" },
      priority: "low",
    });
    await enqueue({
      type: "record_order",
      payload: { label: "medium" },
      priority: "medium",
    });
    await enqueue({
      type: "record_order",
      payload: { label: "high" },
      priority: "high",
    });

    startWorker().catch(() => {
      // swallow — test teardown handles shutdown
    });

    // Wait until all three jobs have been processed, rather than sleeping
    // a fixed amount — fixed sleeps are inherently fragile against network
    // latency variance to Upstash. Poll with a timeout ceiling instead.
    const deadline = Date.now() + 10000;
    while (processedOrder.length < 3 && Date.now() < deadline) {
      await sleep(100);
    }

    expect(processedOrder).toEqual(["high", "medium", "low"]);
  });
});
