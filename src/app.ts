import express, { type Request, type Response, type NextFunction } from "express";
import crypto from "node:crypto";
import type { GatewayConfig } from "./config.js";
import { Logger, defaultLogger } from "./logger.js";
import { forwardChatCompletion } from "./forward.js";

export function createApp(config: GatewayConfig, logger: Logger = defaultLogger): express.Express {
  const app = express();

  // Disable x-powered-by
  app.disable("x-powered-by");

  // Health check endpoint
  app.get("/healthz", (_req: Request, res: Response) => {
    res.status(200).json({ status: "ok" });
  });

  // Request ID middleware
  app.use((req: Request, res: Response, next: NextFunction) => {
    const headerReqId = req.headers["x-request-id"];
    const reqId = typeof headerReqId === "string" && headerReqId.trim().length > 0
      ? headerReqId.trim()
      : crypto.randomUUID();

    res.setHeader("X-Request-Id", reqId);
    (req as any).reqId = reqId;
    next();
  });

  // Body parser with 1MB limit and error handling
  app.use(express.json({ limit: "1mb" }));

  // Chat completions route
  app.post("/v1/chat/completions", async (req: Request, res: Response, next: NextFunction) => {
    const reqId = (req as any).reqId;
    const body = req.body;

    if (!body || typeof body !== "object") {
      res.status(400).json({
        error: {
          message: "Request body must be a JSON object",
          type: "invalid_request_error"
        }
      });
      return;
    }

    if (!Array.isArray(body.messages)) {
      res.status(400).json({
        error: {
          message: "Missing or invalid messages field: expected an array",
          type: "invalid_request_error"
        }
      });
      return;
    }

    if (typeof body.model !== "string" || body.model.trim() === "") {
      res.status(400).json({
        error: {
          message: "Missing or invalid model field: expected a non-empty string",
          type: "invalid_request_error"
        }
      });
      return;
    }

    try {
      await forwardChatCompletion(req, res, config, logger, reqId);
    } catch (err) {
      next(err);
    }
  });

  // Method Not Allowed for known routes with wrong HTTP method
  app.all("/v1/chat/completions", (_req: Request, res: Response) => {
    res.status(405).json({
      error: {
        message: "Method Not Allowed: only POST is supported on /v1/chat/completions",
        type: "method_not_allowed"
      }
    });
  });

  app.all("/healthz", (_req: Request, res: Response) => {
    res.status(405).json({
      error: {
        message: "Method Not Allowed: only GET is supported on /healthz",
        type: "method_not_allowed"
      }
    });
  });

  // 404 for unknown routes
  app.use((_req: Request, res: Response) => {
    res.status(404).json({
      error: {
        message: "Not Found",
        type: "not_found"
      }
    });
  });

  // Global error handler
  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    if (res.headersSent) {
      return;
    }
    if (err instanceof SyntaxError && "body" in err) {
      res.status(400).json({
        error: {
          message: "Invalid JSON payload in request body",
          type: "invalid_request_error"
        }
      });
      return;
    }
    if (err.type === "entity.too.large") {
      res.status(413).json({
        error: {
          message: "Request payload too large (limit is 1MB)",
          type: "payload_too_large"
        }
      });
      return;
    }

    res.status(500).json({
      error: {
        message: "Internal Server Error",
        type: "internal_server_error"
      }
    });
  });

  return app;
}
