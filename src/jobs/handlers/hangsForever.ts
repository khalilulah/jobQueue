// Simulates a slow job. Combined with manually killing the process (Ctrl+C)
// while this is running, this lets us observe the reaper reclaiming a job
// whose worker "died" mid-processing — there's no other realistic way to
// trigger that condition on demand.
export async function hangsForeverHandler(): Promise<void> {
  console.log(
    "[handler] starting a long task that will never get to finish...",
  );
  await new Promise((resolve) => setTimeout(resolve, 60000));
  console.log(
    "[handler] (you should never see this if you killed the process)",
  );
}
