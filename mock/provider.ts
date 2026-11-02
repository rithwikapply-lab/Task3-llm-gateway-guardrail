import express, { type Request, type Response } from "express";
import http from "node:http";

export interface MockScript {
  text?: string;
  choices?: string[];
  toolCalls?: Array<{
    name: string;
    arguments: string;
  }>;
  chunkSizes?: number[];
  gate?: boolean;
  status?: number;
  dropAfter?: number;
  stallAfter?: number;
}

export interface MockStats {
  requestCount: number;
  clientDisconnects: number;
  recordedHeaderNames: string[][];
}

export class MockProviderServer {
  public app: express.Express;
  public server: http.Server | null = null;
  public port: number;

  public stats: MockStats = {
    requestCount: 0,
    clientDisconnects: 0,
    recordedHeaderNames: []
  };

  private releaseResolvers = new Map<number, () => void>();
  private releasedSteps = new Set<number>();

  constructor(port = 4001) {
    this.port = port;
    this.app = express();
    this.app.use(express.json());
    this.setupRoutes();
  }

  reset(): void {
    this.stats = {
      requestCount: 0,
      clientDisconnects: 0,
      recordedHeaderNames: []
    };
    this.releaseResolvers.clear();
    this.releasedSteps.clear();
  }

  release(step: number): void {
    this.releasedSteps.add(step);
    const resolver = this.releaseResolvers.get(step);
    if (resolver) {
      resolver();
      this.releaseResolvers.delete(step);
    }
  }

  private async waitForRelease(step: number): Promise<void> {
    if (this.releasedSteps.has(step)) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.releaseResolvers.set(step, resolve);
    });
  }

  private setupRoutes(): void {
    // Stats endpoint
    this.app.get("/__stats", (_req: Request, res: Response) => {
      res.json(this.stats);
    });

    // Reset endpoint
    this.app.post("/__reset", (_req: Request, res: Response) => {
      this.reset();
      res.json({ ok: true });
    });

    // Release gate endpoint
    this.app.post("/__release/:step", (req: Request, res: Response) => {
      const step = parseInt(String(req.params.step), 10);
      this.release(step);
      res.json({ released: step });
    });

    // Chat completions endpoint
    this.app.post("/v1/chat/completions", async (req: Request, res: Response) => {
      this.stats.requestCount++;
      this.stats.recordedHeaderNames.push(Object.keys(req.headers).map((h) => h.toLowerCase()));

      let completed = false;
      res.on("close", () => {
        if (!completed) {
          this.stats.clientDisconnects++;
        }
      });

      const body = req.body || {};
      const mock: MockScript = body.mock || {};

      // Error status before streaming
      if (mock.status && mock.status !== 200) {
        completed = true;
        res.status(mock.status).json({
          error: {
            message: `Mock error with status ${mock.status}`,
            type: "mock_error",
            code: mock.status
          }
        });
        return;
      }

      const isStream = body.stream === true;
      const model = body.model || "mock-model";
      const id = "chatcmpl-mock-" + Date.now();
      const created = Math.floor(Date.now() / 1000);

      // Non-streaming response
      if (!isStream) {
        completed = true;
        const text = mock.text ?? "Mock non-stream response";
        res.status(200).json({
          id,
          object: "chat.completion",
          created,
          model,
          choices: [
            {
              index: 0,
              message: {
                role: "assistant",
                content: text
              },
              finish_reason: "stop"
            }
          ]
        });
        return;
      }

      // Streaming SSE response
      res.writeHead(200, {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
      });

      const chunkSizes = mock.chunkSizes && mock.chunkSizes.length > 0 ? mock.chunkSizes : [4];
      let step = 1;

      const sendChunk = async (data: string): Promise<boolean> => {
        if (mock.dropAfter !== undefined && step > mock.dropAfter) {
          res.socket?.destroy();
          return false;
        }

        if (mock.stallAfter !== undefined && step > mock.stallAfter) {
          // Cease sending and keep socket open
          return false;
        }

        if (mock.gate) {
          await this.waitForRelease(step);
        }

        res.write(`data: ${data}\n\n`);
        step++;
        return true;
      };

      // 1. Multi-choice support
      if (Array.isArray(mock.choices)) {
        // Leading role chunks
        for (let c = 0; c < mock.choices.length; c++) {
          const ok = await sendChunk(
            JSON.stringify({
              id,
              object: "chat.completion.chunk",
              created,
              model,
              choices: [{ index: c, delta: { role: "assistant" }, finish_reason: null }]
            })
          );
          if (!ok) return;
        }

        // Content chunks for each choice
        for (let c = 0; c < mock.choices.length; c++) {
          const text = mock.choices[c];
          let offset = 0;
          let sizeIdx = 0;
          while (offset < text.length) {
            const size = chunkSizes[sizeIdx % chunkSizes.length];
            const piece = text.slice(offset, offset + size);
            offset += size;
            sizeIdx++;

            const ok = await sendChunk(
              JSON.stringify({
                id,
                object: "chat.completion.chunk",
                created,
                model,
                choices: [{ index: c, delta: { content: piece }, finish_reason: null }]
              })
            );
            if (!ok) return;
          }

          // Finish chunk for choice c
          const ok = await sendChunk(
            JSON.stringify({
              id,
              object: "chat.completion.chunk",
              created,
              model,
              choices: [{ index: c, delta: {}, finish_reason: "stop" }]
            })
          );
          if (!ok) return;
        }

        await sendChunk("[DONE]");
        completed = true;
        res.end();
        return;
      }

      // 2. Tool calls support
      if (Array.isArray(mock.toolCalls)) {
        // Leading chunk with tool definition
        for (let t = 0; t < mock.toolCalls.length; t++) {
          const tc = mock.toolCalls[t];
          const ok = await sendChunk(
            JSON.stringify({
              id,
              object: "chat.completion.chunk",
              created,
              model,
              choices: [
                {
                  index: 0,
                  delta: {
                    role: "assistant",
                    tool_calls: [
                      {
                        index: t,
                        id: `call_${t}`,
                        type: "function",
                        function: { name: tc.name, arguments: "" }
                      }
                    ]
                  },
                  finish_reason: null
                }
              ]
            })
          );
          if (!ok) return;

          let offset = 0;
          let sizeIdx = 0;
          while (offset < tc.arguments.length) {
            const size = chunkSizes[sizeIdx % chunkSizes.length];
            const piece = tc.arguments.slice(offset, offset + size);
            offset += size;
            sizeIdx++;

            const ok2 = await sendChunk(
              JSON.stringify({
                id,
                object: "chat.completion.chunk",
                created,
                model,
                choices: [
                  {
                    index: 0,
                    delta: {
                      tool_calls: [
                        {
                          index: t,
                          function: { arguments: piece }
                        }
                      ]
                    },
                    finish_reason: null
                  }
                ]
              })
            );
            if (!ok2) return;
          }
        }

        // Finish chunk for tool calls
        const ok = await sendChunk(
          JSON.stringify({
            id,
            object: "chat.completion.chunk",
            created,
            model,
            choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }]
          })
        );
        if (!ok) return;

        await sendChunk("[DONE]");
        completed = true;
        res.end();
        return;
      }

      // 3. Standard single-choice text streaming
      const text = mock.text ?? "Hello from mock provider";

      // Step 1: Leading role-only chunk
      const okRole = await sendChunk(
        JSON.stringify({
          id,
          object: "chat.completion.chunk",
          created,
          model,
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }]
        })
      );
      if (!okRole) return;

      // Text chunks split by chunkSizes
      let offset = 0;
      let sizeIdx = 0;
      while (offset < text.length) {
        const size = chunkSizes[sizeIdx % chunkSizes.length];
        const piece = text.slice(offset, offset + size);
        offset += size;
        sizeIdx++;

        const ok = await sendChunk(
          JSON.stringify({
            id,
            object: "chat.completion.chunk",
            created,
            model,
            choices: [{ index: 0, delta: { content: piece }, finish_reason: null }]
          })
        );
        if (!ok) return;
      }

      // Finish chunk
      const okFinish = await sendChunk(
        JSON.stringify({
          id,
          object: "chat.completion.chunk",
          created,
          model,
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }]
        })
      );
      if (!okFinish) return;

      // [DONE]
      await sendChunk("[DONE]");
      completed = true;
      res.end();
    });
  }

  async start(): Promise<number> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.port, () => {
        const addr = this.server?.address();
        if (addr && typeof addr === "object") {
          this.port = addr.port;
        }
        resolve(this.port);
      });
    });
  }

  async close(): Promise<void> {
    if (this.server) {
      await new Promise<void>((resolve) => {
        this.server?.close(() => resolve());
      });
      this.server = null;
    }
  }
}

export function createMockProvider(port = 4001): MockProviderServer {
  return new MockProviderServer(port);
}

// Standalone execution
if (process.argv[1] && process.argv[1].endsWith("provider.js")) {
  const provider = createMockProvider(4001);
  provider.start().then((port) => {
    process.stdout.write(`Mock provider listening on port ${port}\n`);
  });
}
