import { calculateBackoffDelay } from "../queue/retry";
import { redis } from "../config/redis";

afterAll(() => {
  // retry.ts imports `redis` at the module level, which opens a connection
  // eagerly even though these tests never use Redis directly. We must close
  // it explicitly or Jest will hang waiting for the open socket to close.
  redis.disconnect();
});

describe("calculateBackoffDelay", () => {
  it("never exceeds the theoretical max for a given attempt number", () => {
    for (let attempt = 1; attempt <= 6; attempt++) {
      const maxPossible = 1000 * Math.pow(2, attempt);

      // Sample several times since this is randomized — a single sample
      // proves nothing about the ceiling being respected consistently.
      for (let sample = 0; sample < 20; sample++) {
        const delay = calculateBackoffDelay(attempt);
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThanOrEqual(maxPossible);
      }
    }
  });

  it("increases the ceiling roughly exponentially as attempts increase", () => {
    // We can't assert individual samples are larger (that's the whole
    // point of jitter — they're random), but the average across many
    // samples should trend upward with the attempt number.
    const avgAt = (attempt: number) => {
      const samples = Array.from({ length: 200 }, () =>
        calculateBackoffDelay(attempt),
      );
      return samples.reduce((sum, value) => sum + value, 0) / samples.length;
    };

    const avg1 = avgAt(1);
    const avg3 = avgAt(3);
    const avg5 = avgAt(5);

    expect(avg3).toBeGreaterThan(avg1);
    expect(avg5).toBeGreaterThan(avg3);
  });

  it("produces varied delays across calls with the same attempt number (jitter)", () => {
    const samples = Array.from({ length: 10 }, () => calculateBackoffDelay(4));
    const uniqueValues = new Set(samples);

    // Extremely unlikely all 10 random samples land on the exact same
    // value if jitter is actually working — this would only fail if
    // jitter were somehow broken (e.g. accidentally returning a fixed
    // delay instead of a random one).
    expect(uniqueValues.size).toBeGreaterThan(1);
  });
});
