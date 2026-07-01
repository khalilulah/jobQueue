# job-queue

A job queue and background worker system built from scratch in Node.js and TypeScript, using Redis as the underlying data store — without BullMQ or any queue library.

Built to deeply understand how background processing systems work at a fundamental level: how queues function as data structures, how workers coordinate without stepping on each other, and what the real tradeoffs are between different retry and failure strategies.

---

## Why this exists

Most Node.js job queue tutorials hand you BullMQ and tell you to call `.add()`. This project builds the same core mechanics by hand, so the design decisions are visible rather than hidden inside a library:

- Why Redis Lists and not a database table?
- Why does a worker need a *separate* connection for blocking commands?
- What actually happens to a job if its worker crashes mid-execution?
- Why does exponential backoff use jitter, and what problem does that solve?
- How does "at least once" delivery interact with idempotency?

The answers are in the code, with inline comments explaining the tradeoff at each decision point.

---

## Features

- **Job enqueueing** — producers push jobs with a type, payload, and optional configuration
- **Background workers** — a continuous worker loop claims and processes jobs atomically via `BRPOPLPUSH`, so no two workers ever process the same job
- **Priority queues** — three priority tiers (high / medium / low) backed by separate Redis Lists; the worker sweeps high before medium before low on every iteration
- **Delayed jobs** — jobs scheduled for a future time are held in a Redis Sorted Set scored by their `runAt` timestamp; a poller moves due jobs into the pending queue
- **Retry with exponential backoff** — failed jobs are re-scheduled via the same Sorted Set used for delayed jobs, with a delay of `random(0, base * 2^attempt)` (full jitter)
- **Dead-letter queue** — jobs that exhaust their retry budget are moved to `queue:dead` with their last error and failure timestamp attached, rather than silently dropped
- **Visibility timeout / crash recovery** — a reaper sweeps `queue:processing` every 5 seconds; jobs sitting there longer than 10 seconds (worker likely crashed) are reclaimed and fed back through the retry/DLQ logic
- **Graceful shutdown** — `SIGINT`/`SIGTERM` signals stop the worker from claiming new jobs, then wait up to 15 seconds for any in-flight job to finish before exiting

---

## Architecture

```
Producer
  └─► RPUSH  queue:pending:high / :medium / :low
               │
               │  (worker sweep: high → medium → low)
               ▼
Worker ◄─── RPOPLPUSH ──► queue:processing
  │                              │
  │  success                     │  crash / timeout
  │  LREM :processing            │  (reaper reclaims)
  │                              ▼
  │                       handleFailure()
  │                         │           │
  │              retries    │           │  exhausted
  │              remaining  ▼           ▼
  │              ZADD queue:delayed   RPUSH queue:dead
  │                   (scored by runAt)
  │                         │
  │                  poller (every 1s)
  │                  ZRANGEBYSCORE 0 now
  │                  ZREM + RPUSH → pending:<priority>
  │                         │
  └─────────────────────────┘
```

### Redis data structures in use

| Key | Type | Purpose |
|-----|------|---------|
| `queue:pending:high` | List | Ready jobs, high priority |
| `queue:pending:medium` | List | Ready jobs, medium priority |
| `queue:pending:low` | List | Ready jobs, low priority |
| `queue:processing` | List | Jobs currently being worked on |
| `queue:delayed` | Sorted Set | Future/backoff jobs, scored by `runAt` timestamp |
| `queue:dead` | List | Jobs that exhausted all retries |

---

## Project structure

```
job-queue/
├── src/
│   ├── index.ts                    ← Entry point: starts worker, poller, reaper, signal handlers
│   ├── config/
│   │   └── redis.ts                ← ioredis singleton + blocking connection factory
│   ├── queue/
│   │   ├── types.ts                ← Job and EnqueueOptions interfaces
│   │   ├── keys.ts                 ← Centralised Redis key names and Priority type
│   │   ├── producer.ts             ← enqueue() — creates and pushes a job
│   │   ├── worker.ts               ← Worker loop, claim logic, success/failure handling
│   │   ├── retry.ts                ← Backoff calculation, retry vs. DLQ decision
│   │   ├── delayed.ts              ← Sorted Set poller: moves due jobs into pending
│   │   └── reaper.ts               ← Visibility timeout sweep: reclaims stuck jobs
│   ├── jobs/
│   │   ├── registry.ts             ← Maps job type strings to handler functions
│   │   └── handlers/
│   │       ├── sendWelcomeEmail.ts ← Example real handler
│   │       ├── alwaysFails.ts      ← Test handler (always throws)
│   │       └── hangsForever.ts     ← Test handler (simulates a slow/stuck job)
│   ├── flush.ts                    ← Dev utility: clears all queue keys in Redis
│   └── __tests__/
│       ├── testHelpers.ts          ← flushQueues() and sleep() shared by all tests
│       ├── retry.test.ts           ← Unit tests for backoff calculation
│       ├── producer.test.ts        ← Integration tests for enqueue()
│       ├── priority.test.ts        ← Integration test for priority ordering
│       ├── retryDlq.test.ts        ← Integration test for retry → DLQ lifecycle
│       └── shutdown.test.ts        ← Integration test for graceful shutdown timeout
├── .env                            ← REDIS_URL (not committed)
├── .env.example                    ← Documents required env vars
├── jest.config.js
├── tsconfig.json
└── package.json
```

---

## Getting started

### Prerequisites

- Node.js 20+
- A Redis instance — [Upstash](https://upstash.com) free tier works well (no local Redis required)

### Setup

```bash
git clone https://github.com/your-username/job-queue.git
cd job-queue
npm install
```

Create a `.env` file at the project root:

```
REDIS_URL=rediss://default:PASSWORD@HOST.upstash.io:PORT
```

Use the `rediss://` (TLS) URL from your Upstash dashboard, not the plain `redis://` one.

### Run the worker

```bash
npm run dev
```

This starts the worker loop, the delayed-job poller, and the reaper together in one process. To enqueue jobs, import `enqueue()` from `src/queue/producer.ts` in your own producer code (an Express route, a cron job, a script, etc.).

### Enqueue a job

```typescript
import { enqueue } from './queue/producer';

// Defaults: priority 'medium', maxRetries 3
await enqueue({
  type: 'send_welcome_email',
  payload: { userId: 42, email: 'user@example.com' },
});

// Override priority and retry budget
await enqueue({
  type: 'send_welcome_email',
  payload: { userId: 42, email: 'user@example.com' },
  priority: 'high',
  maxRetries: 5,
});
```

### Add a new job type

1. Create a handler in `src/jobs/handlers/`:

```typescript
// src/jobs/handlers/sendPasswordReset.ts
export async function sendPasswordResetHandler(payload: {
  userId: number;
  email: string;
}): Promise<void> {
  // call your email provider here
}
```

2. Register it in `src/jobs/registry.ts`:

```typescript
import { sendPasswordResetHandler } from './handlers/sendPasswordReset';

export const jobRegistry = {
  send_welcome_email: sendWelcomeEmailHandler,
  send_password_reset: sendPasswordResetHandler, // ← add this
};
```

3. Enqueue it from anywhere:

```typescript
await enqueue({ type: 'send_password_reset', payload: { userId: 1, email: 'user@example.com' } });
```

### Run the test suite

```bash
npm test
```

Tests run against your real Upstash instance (not a mock). The suite flushes and restores queue state around each test, so it is safe to run against a shared dev Redis. It takes around 40–45 seconds due to real timing in the retry/reaper/shutdown tests.

### Clear Redis queue state (dev utility)

```bash
npx ts-node --transpile-only src/flush.ts
```

---

## Key design decisions and tradeoffs

### Atomic job handoff via `RPOPLPUSH`

When a worker claims a job, it must both remove it from `pending` and add it to `processing` as a single, indivisible step. Redis's `RPOPLPUSH` (and its blocking variant `BRPOPLPUSH`) provides this atomicity. A naive "read then delete" approach would allow two workers to both see and claim the same job — a race condition that `RPOPLPUSH` eliminates by fusing both steps into one command on Redis's single-threaded command processor.

### Why a separate connection for blocking commands

`BRPOPLPUSH` holds a Redis connection open until a job is available, which can be indefinitely long. A connection that is blocking cannot send or receive any other command while it waits. The worker therefore uses a dedicated connection exclusively for this blocking call, leaving the shared `redis` singleton free for all other quick operations (timestamping, LREM, ZADD, etc.).

### At-least-once delivery and idempotency

This system guarantees every job will be attempted at least once, even across process crashes. It does not guarantee exactly once — a worker can crash after completing the real work but before removing the job from `queue:processing`, causing the reaper to retry it. Handlers for operations with side effects (charging a card, sending an email) must be written to be **idempotent**: checking whether the operation already succeeded before performing it again.

### Visibility timeout tradeoff

The reaper reclaims jobs that have been in `queue:processing` for longer than `VISIBILITY_TIMEOUT_MS` (10 seconds). Setting this too short risks reclaiming a legitimately slow job while its original worker is still running, causing duplicate processing. Setting it too long delays recovery after a genuine crash. The right value for a production system depends on your worst-case realistic job duration, with margin.

### Exponential backoff with full jitter

Failed jobs wait `random(0, base * 2^attempt)` milliseconds before retrying. The exponential ceiling gives failing systems time to recover. The jitter (randomising within that ceiling rather than using it directly) spreads retries from a batch of jobs that all failed together, preventing a "retry storm" where hundreds of jobs hammer a recovering dependency in lockstep.

### Priority and starvation

The three-tier priority system checks `high` before `medium` before `low` on every worker sweep. This is a strict priority model: sustained high-priority traffic can starve lower tiers indefinitely. This is an inherent tradeoff of any priority system, not a bug — it is the correct behaviour when high-priority work genuinely matters more. If starvation becomes a real problem in production, weighted-fair scheduling (process N low-priority jobs per M high-priority jobs) is the natural next step.

---

## Acknowledged gaps and known limitations

These are design decisions consciously deferred for scope, not oversights:

- **The job stamp swap is not atomic.** When a worker claims a job, it does an `LREM` then `RPUSH` to add a `processingStartedAt` timestamp. These are two separate commands. If the process crashes between them, the job is in neither list. The fix would be a Lua script that makes both operations atomic. The gap is extremely narrow (two sequential in-memory lines) and the reaper covers the failure mode regardless.
- **`jobRegistry` uses `any` for payloads.** Each handler knows its own payload shape, but the registry cannot express this without a discriminated union keyed by job type. The current looseness is an accepted tradeoff for a portfolio project; a production system would want full type safety here.
- **No built-in observability.** There is no dashboard, metrics endpoint, or alerting for DLQ growth. In production, monitoring `LLEN queue:dead` and alerting when it grows is the minimum viable observability layer for this system.
- **Single-process architecture.** The worker, poller, and reaper run together in one Node process. In production, these would typically be separate deployments scaled independently — multiple worker processes on separate machines, all sharing one Redis instance, with the poller and reaper running as single instances.

---

## Concepts covered

If you are reading this to learn rather than use it, the implementation touches:

- Redis Lists (`LPUSH`, `RPUSH`, `RPOPLPUSH`, `BRPOPLPUSH`, `LREM`, `LRANGE`)
- Redis Sorted Sets (`ZADD`, `ZRANGEBYSCORE`, `ZREM`, `ZRANGE`)
- Atomic operations and why they matter under concurrency
- The "at least once" delivery guarantee and what it actually means in practice
- Exponential backoff with full jitter
- Visibility timeouts for crash recovery
- Graceful shutdown with a bounded timeout using `Promise.race`
- Integration testing against real infrastructure (no mocks)
- TypeScript strict mode, generic interfaces, and the difference between "optional in input" vs "required in stored data"
