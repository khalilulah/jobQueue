import express from "express";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";
import { setupWebSocket } from "./websocket";

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3000;

/**
 * Creates and starts the Express + Socket.io server.
 *
 * Why createServer(app) instead of app.listen()?
 * Socket.io needs direct access to the raw Node HTTP server, not Express's
 * wrapper around it. createServer(app) gives us that raw server while still
 * routing HTTP requests through Express — then we pass the same server to
 * both app.listen() (via server.listen()) and Socket.io.
 */
export function startApiServer(): void {
  const app = express();
  app.use(express.json());

  // Raw Node HTTP server — Socket.io attaches here.
  const httpServer = createServer(app);

  // Socket.io server, attached to the same HTTP server so WebSocket
  // upgrade requests on the same port are handled automatically.
  const io = new SocketIOServer(httpServer, {
    cors: {
      // Allow the React dev server (typically port 5173 with Vite) to
      // connect during development. Tighten this to your production domain
      // before deploying.
      origin: ["http://localhost:5173", "http://localhost:3000"],
      methods: ["GET", "POST"],
    },
  });

  // Wire up all WebSocket event forwarding.
  setupWebSocket(io);

  // Health check — useful for confirming the server is alive from a
  // browser or curl before the frontend exists.
  app.get("/health", (_req, res) => {
    res.json({ status: "ok", timestamp: Date.now() });
  });

  // REST routes will be mounted here in the next step.
  // app.use('/api/stats', statsRouter);
  // app.use('/api/jobs', jobsRouter);
  // app.use('/api/dlq', dlqRouter);

  httpServer.listen(PORT, () => {
    console.log(`[api] server running on http://localhost:${PORT}`);
    console.log(`[api] websocket ready on ws://localhost:${PORT}`);
  });
}
