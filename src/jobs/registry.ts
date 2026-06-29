import { alwaysFailsHandler } from "./handlers/alwaysFails";
import { sendWelcomeEmailHandler } from "./handlers/sendWelcomeEmail";

// Maps a job's `type` string to the function that knows how to actually do it.
// Adding a new kind of job later means: write a handler, register it here.
// The worker itself never needs to change.
export const jobRegistry: Record<string, (payload: any) => Promise<void>> = {
  send_welcome_email: sendWelcomeEmailHandler,
  always_fails: alwaysFailsHandler,
};
