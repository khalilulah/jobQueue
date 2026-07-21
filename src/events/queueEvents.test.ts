import { redis } from "../config/redis";
import { enqueue } from "../queue/producer";
import { queueEvents } from "./queueEvents";
import { Job } from "../queue/types";
import { flushQueues } from "../_tests_/testHelpers";

// This test proves the smallest meaningful thing about the event bus:
// that calling enqueue() causes a 'job:enqueued' event to arrive at a
// listener on queueEvents, with the correct job data attached.
//
// No HTTP server, no Socket.io, no worker running — just:
//   producer.ts  →  queueEvents (EventEmitter)  →  listener function
//
// If this passes, we know the bus works before layering WebSocket on top.

describe("queueEvents — job:enqueued", () => {
  beforeEach(async () => {
    await flushQueues();
  });

  afterAll(async () => {
    redis.disconnect();
  });

  it("emits job:enqueued with the correct job shape when enqueue() is called", async () => {
    // Promise-based listener pattern: we wrap the .on() call in a Promise
    // so we can await it in the test body rather than dealing with callbacks.
    // The Promise resolves with whatever the event carries as soon as the
    // event fires. If it never fires, the test times out — which is itself
    // a meaningful failure signal ("the event never arrived").
    const receivedJob = await new Promise<Job>((resolve) => {
      queueEvents.once("job:enqueued", (job) => {
        resolve(job);
      });

      // Trigger the event by calling enqueue() — producer.ts emits
      // 'job:enqueued' after pushing to Redis, so the listener above
      // fires as a direct result of this call.
      enqueue({
        type: "test_event_job",
        payload: { userId: 99 },
        priority: "high",
        maxRetries: 2,
      });
    });

    // The event arrived — now assert its shape is exactly what we expect.
    // This proves producer.ts is emitting the full Job object, not just
    // an ID or a partial snapshot.
    expect(receivedJob.type).toBe("test_event_job");
    expect(receivedJob.payload).toEqual({ userId: 99 });
    expect(receivedJob.priority).toBe("high");
    expect(receivedJob.maxRetries).toBe(2);
    expect(receivedJob.attempts).toBe(0);
    expect(receivedJob.id).toBeDefined();
    expect(typeof receivedJob.id).toBe("string");
    expect(receivedJob.createdAt).toBeDefined();
  });

  it("emits a separate event for each enqueue() call", async () => {
    const received: Job[] = [];

    // Collect events into an array rather than resolving immediately,
    // so we can verify two separate calls produce two separate events.
    queueEvents.on("job:enqueued", (job) => {
      received.push(job);
    });

    await enqueue({ type: "job_one", payload: {} });
    await enqueue({ type: "job_two", payload: {} });

    // Brief pause to let any async event delivery settle — EventEmitter
    // is synchronous in Node, so in practice the events arrive immediately,
    // but the explicit wait makes the test's intent unambiguous.
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Remove the listener we added so it doesn't leak into other tests.
    queueEvents.removeAllListeners("job:enqueued");

    expect(received).toHaveLength(2);
    expect(received[0].type).toBe("job_one");
    expect(received[1].type).toBe("job_two");

    // IDs must be unique — not the same job reported twice.
    expect(received[0].id).not.toBe(received[1].id);
  });
});
