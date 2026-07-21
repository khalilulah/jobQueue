import { EventEmitter } from "events";
import { Job } from "../queue/types";
import { Priority } from "../queue/keys";

// TypeScript's way of telling an EventEmitter exactly which event names
// are valid and what data shape each one carries. Without this, .emit()
// and .on() accept any string with any data — helpful autocomplete and
// compile-time checks go away entirely.
//
// Each key is an event name. Each value is a tuple representing the
// arguments passed to the listener function for that event.
export interface QueueEvents {
  "job:enqueued": [job: Job];
  "job:started": [
    data: {
      jobId: string;
      type: string;
      priority: Priority;
      startedAt: number;
    },
  ];
  "job:completed": [data: { jobId: string; type: string; completedAt: number }];
  "job:failed": [
    data: {
      jobId: string;
      type: string;
      attempt: number;
      maxRetries: number;
      nextRetryAt: number;
      error: string;
    },
  ];
  "job:dlq": [
    data: {
      jobId: string;
      type: string;
      attempts: number;
      lastError: string;
      failedAt: number;
    },
  ];
  "job:requeued": [data: { jobId: string }];
  "reaper:reclaimed": [data: { jobId: string; type: string; age: number }];
}

// TypedEventEmitter wraps Node's EventEmitter with our QueueEvents
// interface so that .emit() and .on() are both fully type-checked.
// The actual runtime object is still a plain EventEmitter — no magic,
// just type-level information layered on top.
declare interface TypedEventEmitter {
  emit<K extends keyof QueueEvents>(event: K, ...args: QueueEvents[K]): boolean;
  on<K extends keyof QueueEvents>(
    event: K,
    listener: (...args: QueueEvents[K]) => void,
  ): this;
  off<K extends keyof QueueEvents>(
    event: K,
    listener: (...args: QueueEvents[K]) => void,
  ): this;
}

class TypedEventEmitter extends EventEmitter {}

// Single shared instance. Both the queue code and the API layer import
// this same object — queue code calls .emit(), API layer calls .on().
// Neither side imports the other directly; this bus is the only bridge.
export const queueEvents = new TypedEventEmitter();
