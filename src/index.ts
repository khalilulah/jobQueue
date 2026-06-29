import { startWorker } from "./queue/worker";

startWorker().catch((err) => {
  console.error("[worker] fatal error:", err);
  process.exit(1);
});
