// All Redis keys used by the queue system, defined once.
// Every file that needs to reference a list/set name imports from here —
// a typo'd literal string in one file would silently create a queue
// nothing else ever reads from, which is a nasty bug to track down.
export const QUEUE_KEYS = {
  pending: "queue:pending",
  processing: "queue:processing",
  delayed: "queue:delayed",
  dead: "queue:dead",
} as const;
