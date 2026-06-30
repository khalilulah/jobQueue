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
} as const;
