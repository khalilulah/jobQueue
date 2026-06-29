import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL is not set. Check your .env file.");
}

// A single shared connection, reused across the whole app.
// We deliberately do NOT create a new Redis() instance per file/module —
// each instance is a real TCP connection, and we have no reason to open more than one
// for this project's scale.
export const redis = new Redis(redisUrl);

redis.on("connect", () => {
  console.log("[redis] connected");
});

redis.on("error", (err) => {
  console.error("[redis] connection error:", err.message);
});
