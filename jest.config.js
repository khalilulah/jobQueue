/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",

  // Our tests hit a real Upstash instance and wait on real backoff/reaper
  // timers (10s visibility timeout, etc.) — Jest's default 5s timeout is
  // too short for that. This isn't unit-testing in the classic isolated
  // sense; it's integration testing against real queue mechanics, which
  // is the more meaningful thing to verify for this project.
  testTimeout: 30000,

  // Run test files sequentially, not in parallel. Our tests share one
  // Redis instance and flush/inspect specific keys — running them
  // concurrently would mean one test's flush could wipe out another
  // test's in-progress job, exactly the kind of race condition this whole
  // project has been about avoiding.
  maxWorkers: 1,

  setupFiles: ["dotenv/config"],
};
