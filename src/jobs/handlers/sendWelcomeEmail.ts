// A stand-in for real work. In a real system this might call SendGrid, etc.
// For now it just simulates doing something and logs it — enough to prove
// the worker loop actually invokes the right handler for a given job type.
export async function sendWelcomeEmailHandler(payload: {
  userId: number;
  email: string;
}): Promise<void> {
  console.log(
    `[handler] sending welcome email to ${payload.email} (user ${payload.userId})`,
  );
  // simulate some work taking a moment, like a real API call would
  await new Promise((resolve) => setTimeout(resolve, 200));
  console.log(`[handler] welcome email sent to ${payload.email}`);
}
