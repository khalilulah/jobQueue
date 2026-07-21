// All Redis keys used by the queue system, defined once.
// Every file that needs to reference a list/set name imports from here —
// a typo'd literal string in one file would silently create a queue
// nothing else ever reads from, which is a nasty bug to track down.

export type Priority = "high" | "medium" | "low";

// Checked in this exact order by the worker — high drains before medium,
// medium before low. This IS the starvation tradeoff we discussed:
// sustained high-priority traffic can delay low-priority jobs indefinitely.
export const PRIORITY_ORDER: Priority[] = ["high", "medium", "low"];

export const QUEUE_KEYS = {
  pending: {
    high: "queue:pending:high",
    medium: "queue:pending:medium",
    low: "queue:pending:low",
  },
  processing: "queue:processing",
  delayed: "queue:delayed",
  dead: "queue:dead",

  // Added: a capped log of recently completed jobs. The worker RPUSH-es
  // here on success and immediately LTRIM-s to the last 500 entries so
  // this list never grows unbounded. Without this, there's no record of
  // successful jobs — they're LREM'd from processing and simply vanish.
  completed: "queue:completed",
} as const;

// How many completed jobs to keep in the log. Older entries are trimmed
// automatically by the worker after every successful job.
export const COMPLETED_LOG_MAX = 500;
