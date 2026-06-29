// The full shape of a job as it's stored, serialized, in Redis.
// Every list/set in this system (pending, processing, delayed, dead) stores jobs in exactly this shape, JSON-stringified.
export interface Job<TPayload = unknown> {
  id: string;
  type: string;
  payload: TPayload;
  attempts: number;
  maxRetries: number;
  createdAt: number; // unix ms timestamp
}

// What the caller of enqueue() actually has to provide.
// id/attempts/createdAt are filled in by the producer — the caller shouldn't have to think about queue bookkeeping, just "what job, what data".
export interface EnqueueOptions<TPayload = unknown> {
  type: string;
  payload: TPayload;
  maxRetries?: number; // defaults applied in producer.ts
}
