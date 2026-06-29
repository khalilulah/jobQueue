import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const rawRedisUrl = process.env.REDIS_URL;

if (!rawRedisUrl) {
  throw new Error("REDIS_URL is not set. Check your .env file.");
}

// From this point on, REDIS_URL is a plain `string`, guaranteed — every
// usage below references this constant, not process.env.REDIS_URL directly,
// so there's no scope where TypeScript loses track of the narrowing and
// forces us to repeat `as string` assertions.
const REDIS_URL: string = rawRedisUrl;

// A single shared connection, reused across the whole app for fast,
// non-blocking commands (RPUSH, LREM, GET, SET, ZADD, etc.).
// We deliberately do NOT create a new Redis() instance per file/module —
// each instance is a real TCP connection, and we have no reason to open more than one
// for this kind of usage.
export const redis = new Redis(REDIS_URL);

redis.on("connect", () => {
  console.log("[redis] connected");
});

redis.on("error", (err) => {
  console.error("[redis] connection error:", err.message);
});

/**
 * Creates a NEW, separate Redis connection, intended only for blocking commands
 * (BRPOPLPUSH, BLPOP, etc.).
 *
 * Why a function, not a single shared export like `redis` above?
 * Each worker process should own its own dedicated blocking connection —
 * if we ran multiple workers inside one Node process for some reason, sharing
 * a single blocking connection between them would mean only one worker's
 * BRPOPLPUSH could ever be "in flight" at a time, defeating the purpose.
 * Calling this function gives each worker a fresh, independent connection.
 */
export function createBlockingConnection(): Redis {
  const connection = new Redis(REDIS_URL);

  connection.on("error", (err) => {
    console.error("[redis:blocking] connection error:", err.message);
  });

  return connection;
}
