import { redis } from "../config/redis";
import { enqueue } from "../queue/producer";
import { QUEUE_KEYS } from "../queue/keys";
import { Job } from "../queue/types";
import { flushQueues } from "./testHelpers";

describe("enqueue", () => {
  beforeEach(async () => {
    await flushQueues();
  });

  afterAll(async () => {
    await flushQueues();
    redis.disconnect();
  });

  it("defaults to medium priority and 3 max retries when not specified", async () => {
    await enqueue({ type: "test_job", payload: { foo: "bar" } });

    const raw = await redis.lrange(QUEUE_KEYS.pending.medium, 0, -1);
    expect(raw).toHaveLength(1);

    const job: Job = JSON.parse(raw[0]);
    expect(job.priority).toBe("medium");
    expect(job.maxRetries).toBe(3);
    expect(job.attempts).toBe(0);
    expect(job.payload).toEqual({ foo: "bar" });
  });

  it("routes a job to the correct priority list when specified", async () => {
    await enqueue({ type: "test_job", payload: {}, priority: "high" });

    const highList = await redis.lrange(QUEUE_KEYS.pending.high, 0, -1);
    const mediumList = await redis.lrange(QUEUE_KEYS.pending.medium, 0, -1);
    const lowList = await redis.lrange(QUEUE_KEYS.pending.low, 0, -1);

    expect(highList).toHaveLength(1);
    expect(mediumList).toHaveLength(0);
    expect(lowList).toHaveLength(0);
  });

  it("respects a custom maxRetries value", async () => {
    await enqueue({ type: "test_job", payload: {}, maxRetries: 7 });

    const raw = await redis.lrange(QUEUE_KEYS.pending.medium, 0, -1);
    const job: Job = JSON.parse(raw[0]);

    expect(job.maxRetries).toBe(7);
  });

  it("generates a unique id for every job", async () => {
    const id1 = await enqueue({ type: "test_job", payload: {} });
    const id2 = await enqueue({ type: "test_job", payload: {} });

    expect(id1).not.toBe(id2);
  });
});
