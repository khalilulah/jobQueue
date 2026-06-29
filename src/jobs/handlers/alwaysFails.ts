// Deliberately always fails — used to exercise the retry/backoff/DLQ path
// during development. Not something you'd ship, just a controlled way to
// watch failure handling actually run.
export async function alwaysFailsHandler(): Promise<void> {
  throw new Error("simulated failure for testing retry logic");
}
