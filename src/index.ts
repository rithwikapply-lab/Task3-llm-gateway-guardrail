import http from "node:http";
import { loadConfigOrExit } from "./config.js";
import { defaultLogger } from "./logger.js";
import { createApp } from "./app.js";

const config = loadConfigOrExit();
const app = createApp(config, defaultLogger);
const server = http.createServer(app);

server.listen(config.port, () => {
  process.stdout.write(`LLM Gateway Guardrail listening on port ${config.port}\n`);
});

function shutdown(signal: string) {
  process.stdout.write(`Received ${signal}, closing server...\n`);
  server.close(() => {
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
