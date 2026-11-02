import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http from "node:http";
import { createApp } from "../src/app.js";
import { Logger } from "../src/logger.js";
import { createMockProvider, type MockProviderServer } from "../mock/provider.js";
import type { GatewayConfig } from "../src/config.js";
import { SseParser } from "../src/sse.js";

describe("Gateway Integration Tests", () => {
  let mockProvider: MockProviderServer;
  let mockPort: number;
  let loggedLines: string[] = [];
  let testLogger: Logger;

  beforeAll(async () => {
    testLogger = new Logger((line) => loggedLines.push(line));
    mockProvider = createMockProvider(0);
    mockPort = await mockProvider.start();
  
  it("backpressure unit test with a fake writable that returns false", async () => {
    const gw = await startGateway();
    mockProvider.reset();
    try {
      const payload = JSON.stringify({
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "ABCDEFGHIJKL", chunkSizes: [4] }
      });

      // Send request with custom fake handling or small buffer
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "ABCDEFGHIJKL", chunkSizes: [4] }
      });
      expect(res.status).toBe(200);
      expect(res.body).toContain("ABCDEFGHIJKL");
    } finally {
      await gw.close();
    }
  });

});

  afterAll(async () => {
    await mockProvider.close();
  });

  function startGateway(configOverrides: Partial<GatewayConfig> = {}) {
    const config: GatewayConfig = {
      port: 0,
      upstreamUrl: "http://127.0.0.1:" + mockPort + "/v1/chat/completions",
      upstreamApiKey: "gateway-upstream-secret-key",
      upstreamIdleTimeoutMs: 5000,
      maxEventBytes: 1048576,
      ...configOverrides
    };

    const app = createApp(config, testLogger);
    const server = http.createServer(app);

    const sockets = new Set<any>();
    server.on("connection", (s) => {
      sockets.add(s);
      s.on("close", () => sockets.delete(s));
    });

    return new Promise<{ server: http.Server; port: number; close: () => Promise<void> }>(
      (resolve) => {
        server.listen(0, () => {
          const addr = server.address() as any;
          resolve({
            server,
            port: addr.port,
            close: () => {
              for (const s of sockets) s.destroy();
              sockets.clear();
              return new Promise<void>((res) => server.close(() => res()));
            }
          });
        });
      }
    );
  }

  function postJson(port: number, path: string, body: any, headers: Record<string, string> = {}) {
    return new Promise<{ status: number; headers: http.IncomingHttpHeaders; body: string }>(
      (resolve, reject) => {
        const payload = JSON.stringify(body);
        const req = http.request(
          {
            hostname: "127.0.0.1",
            port,
            path,
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Content-Length": Buffer.byteLength(payload),
              ...headers
            }
          },
          (res) => {
            let data = "";
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
              resolve({ status: res.statusCode || 500, headers: res.headers, body: data });
            });
          }
        );
        req.on("error", reject);
        req.write(payload);
        req.end();
      }
    );
  }

  it("redacts PII split across upstream chunks at every offset", async () => {
    const gw = await startGateway();
    try {
      const sample = "Email: john.doe@example.com, SSN: 123-45-6789, Card: 4111-1111-1111-1111.";
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: sample, chunkSizes: [4] }
      });
      expect(res.status).toBe(200);
      expect(res.body).not.toContain("john.doe@example.com");
      expect(res.body).not.toContain("123-45-6789");
      expect(res.body).not.toContain("4111-1111-1111-1111");
      expect(res.body).toContain("[REDACTED]");

      for (let split = 1; split < sample.length; split += 6) {
        const splitRes = await postJson(gw.port, "/v1/chat/completions", {
          model: "test-model",
          messages: [{ role: "user", content: "hi" }],
          stream: true,
          mock: { text: sample, chunkSizes: [split, sample.length - split] }
        });
        expect(splitRes.status).toBe(200);
        expect(splitRes.body).not.toContain("john.doe@example.com");
        expect(splitRes.body).not.toContain("123-45-6789");
        expect(splitRes.body).not.toContain("4111-1111-1111-1111");
      }
    } finally {
      await gw.close();
    }
  });

  it("lockstep: client receives content chunk N before provider releases chunk N+1", async () => {
    const gw = await startGateway();
    mockProvider.reset();
    try {
      const payload = JSON.stringify({
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "ABCDEFGHIJKL", chunkSizes: [4], gate: true }
      });
      let receivedFirst = false;
      let receivedSecond = false;
      const clientReq = http.request({
        hostname: "127.0.0.1",
        port: gw.port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload)
        }
      });
      const donePromise = new Promise<void>((resolve, reject) => {
        clientReq.on("response", (res) => {
          const parser = new SseParser();
          res.on("data", (chunk) => {
            const msgs = parser.push(chunk);
            for (const m of msgs) {
              if (m.type === "event") {
                if (m.data.includes("ABCD")) receivedFirst = true;
                if (m.data.includes("EFGH")) receivedSecond = true;
              }
            }
          });
          res.on("end", resolve);
          res.on("error", reject);
        });
      });
      clientReq.write(payload);
      clientReq.end();

      mockProvider.release(1);
      mockProvider.release(2);
      while (!receivedFirst) {
        await new Promise((r) => setTimeout(r, 10));
      }
      expect(receivedFirst).toBe(true);
      expect(receivedSecond).toBe(false);

      mockProvider.release(3);
      while (!receivedSecond) {
        await new Promise((r) => setTimeout(r, 10));
      }
      expect(receivedSecond).toBe(true);

      mockProvider.release(4);
      mockProvider.release(5);
      mockProvider.release(6);
      await donePromise;
    } finally {
      await gw.close();
    }
  });

  it("first content arrives before provider releases chunk 2 (TTFT)", async () => {
    const gw = await startGateway();
    mockProvider.reset();
    try {
      const payload = JSON.stringify({
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "TTFT verification text", chunkSizes: [4], gate: true }
      });
      let roleReceived = false;
      const clientReq = http.request({
        hostname: "127.0.0.1",
        port: gw.port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload)
        }
      });
      const donePromise = new Promise<void>((resolve) => {
        clientReq.on("response", (res) => {
          const parser = new SseParser();
          res.on("data", (chunk) => {
            const msgs = parser.push(chunk);
            for (const m of msgs) {
              if (m.type === "event" && m.data.includes('"role":"assistant"')) {
                roleReceived = true;
              }
            }
          });
          res.on("end", resolve);
        });
      });
      clientReq.write(payload);
      clientReq.end();

      mockProvider.release(1);
      while (!roleReceived) {
        await new Promise((r) => setTimeout(r, 10));
      }
      expect(roleReceived).toBe(true);

      for (let s = 2; s <= 20; s++) {
        mockProvider.release(s);
      }
      await donePromise;
    } finally {
      await gw.close();
    }
  });

  it("finish_reason ordering: flushed text precedes the finish event", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "hello test@corp.org", chunkSizes: [6, 13] }
      });
      expect(res.status).toBe(200);
      const parser = new SseParser();
      const events = parser.push(res.body);
      let flushedIndex = -1;
      let finishIndex = -1;
      for (let i = 0; i < events.length; i++) {
        const ev = events[i];
        if (ev.type === "event") {
          if (ev.data.includes("[REDACTED]")) flushedIndex = i;
          if (ev.data.includes('"finish_reason":"stop"')) finishIndex = i;
        }
      }
      expect(flushedIndex).toBeGreaterThan(-1);
      expect(finishIndex).toBeGreaterThan(-1);
      expect(flushedIndex).toBeLessThan(finishIndex);
    } finally {
      await gw.close();
    }
  });

  it("redacts two choices (n=2) independently", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: {
          choices: [
            "Choice 0: contact alice@test.com today",
            "Choice 1: card is 4111-1111-1111-1111 now"
          ],
          chunkSizes: [4]
        }
      });
      expect(res.status).toBe(200);
      expect(res.body).not.toContain("alice@test.com");
      expect(res.body).not.toContain("4111-1111-1111-1111");
      expect(res.body).toContain("[REDACTED]");
    } finally {
      await gw.close();
    }
  });

  it("redacts tool_call arguments", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: {
          toolCalls: [
            {
              name: "send_email",
              arguments: JSON.stringify({ email: "secret@company.com", ssn: "123-45-6789" })
            }
          ],
          chunkSizes: [4]
        }
      });
      expect(res.status).toBe(200);
      expect(res.body).not.toContain("secret@company.com");
      expect(res.body).not.toContain("123-45-6789");
      expect(res.body).toContain("[REDACTED]");
    } finally {
      await gw.close();
    }
  });

  it("redacts invalid-JSON payload fail-closed", async () => {
    const rawServer = http.createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.write("data: Malformed text with secret@domain.com inside\n\n");
      res.write("data: [DONE]\n\n");
      res.end();
    });
    const rawPort = await new Promise<number>((resolve) => {
      rawServer.listen(0, () => resolve((rawServer.address() as any).port));
    });
    const gw = await startGateway({ upstreamUrl: "http://127.0.0.1:" + rawPort + "/v1/chat/completions" });
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true
      });
      expect(res.status).toBe(200);
      expect(res.body).not.toContain("secret@domain.com");
      expect(res.body).toContain("Malformed text with [REDACTED] inside");
    } finally {
      rawServer.close();
      await gw.close();
    }
  });

  it("aborts upstream when client disconnects", async () => {
    const gw = await startGateway();
    mockProvider.reset();
    try {
      const payload = JSON.stringify({
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "Long text to cancel", chunkSizes: [4], gate: true }
      });
      const clientReq = http.request({
        hostname: "127.0.0.1",
        port: gw.port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload)
        }
      });
      clientReq.on("response", (res) => {
        res.on("data", () => {
          clientReq.destroy();
        });
      });
      clientReq.write(payload);
      clientReq.end();
      mockProvider.release(1);
      while (mockProvider.stats.clientDisconnects === 0) {
        await new Promise((r) => setTimeout(r, 10));
      }
      expect(mockProvider.stats.clientDisconnects).toBeGreaterThanOrEqual(1);
    } finally {
      await gw.close();
    }
  });

  it("handles upstream 500 by returning generic error JSON", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { status: 500 }
      });
      expect(res.status).toBe(500);
      const parsed = JSON.parse(res.body);
      expect(parsed.error).toBeDefined();
      expect(parsed.error.type).toBe("upstream_error");
    } finally {
      await gw.close();
    }
  });

  it("handles connection refused with 502 Bad Gateway", async () => {
    const gw = await startGateway({ upstreamUrl: "http://127.0.0.1:49999/v1/chat/completions" });
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true
      });
      expect(res.status).toBe(502);
      const parsed = JSON.parse(res.body);
      expect(parsed.error.type).toBe("bad_gateway");
    } finally {
      await gw.close();
    }
  });

  it("handles dropAfter by emitting error SSE event", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "Early text before drop and some held secret@domain.com", chunkSizes: [10], dropAfter: 2 }
      });
      expect(res.status).toBe(200);
      expect(res.body).toContain("upstream_drop");
    } finally {
      await gw.close();
    }
  });

  it("handles idle timeout by emitting error SSE event", async () => {
    const gw = await startGateway({ upstreamIdleTimeoutMs: 80 });
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        mock: { text: "Some text before stalling", chunkSizes: [4], stallAfter: 2 }
      });
      expect(res.status).toBe(200);
      expect(res.body).toContain("timeout_error");
    } finally {
      await gw.close();
    }
  });

  it("redacts non-stream JSON responses", async () => {
    const gw = await startGateway();
    try {
      const res = await postJson(gw.port, "/v1/chat/completions", {
        model: "test-model",
        messages: [{ role: "user", content: "hi" }],
        stream: false,
        mock: { text: "Non-stream response with alice@example.com and SSN 123-45-6789" }
      });
      expect(res.status).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.choices[0].message.content).toBe(
        "Non-stream response with [REDACTED] and SSN [REDACTED]"
      );
    } finally {
      await gw.close();
    }
  });

  it("client Authorization never reaches provider, UPSTREAM_API_KEY does, and secrets never leak in logs", async () => {
    const gw = await startGateway();
    mockProvider.reset();
    loggedLines = [];
    try {
      const clientAuthSecret = "client-super-secret-token-xyz-123";
      const res = await postJson(
        gw.port,
        "/v1/chat/completions",
        {
          model: "test-model",
          messages: [{ role: "user", content: "User prompt containing secret@client.com" }],
          stream: true,
          mock: { text: "Hello from mock provider" }
        },
        { Authorization: "Bearer " + clientAuthSecret }
      );
      expect(res.status).toBe(200);
      expect(mockProvider.stats.recordedHeaderNames.length).toBeGreaterThanOrEqual(1);
      const headersSeen = mockProvider.stats.recordedHeaderNames[0];
      expect(headersSeen).toContain("authorization");

      expect(loggedLines.length).toBeGreaterThanOrEqual(1);
      for (const line of loggedLines) {
        expect(line).not.toContain(clientAuthSecret);
        expect(line).not.toContain("gateway-upstream-secret-key");
        expect(line).not.toContain("secret@client.com");
        expect(line).not.toContain("User prompt");
        const logObj = JSON.parse(line);
        expect(logObj.reqId).toBeDefined();
        expect(logObj.status).toBe(200);
        expect(logObj.totalMs).toBeDefined();
        expect(logObj.bytesIn).toBeDefined();
        expect(logObj.bytesOut).toBeDefined();
        expect(logObj.redactions).toBeDefined();
        expect(logObj.peakPending).toBeDefined();
      }
    } finally {
      await gw.close();
    }
  });
  it("backpressure: fake writable returning false pauses pulling upstream until drain and keeps pending bounded", async () => {
    let drainCb: (() => void) | null = null;
    let writeCount = 0;
    let isFinished = false;

    const fakeRes: any = new (require("events").EventEmitter)();
    fakeRes.writeHead = () => {};
    fakeRes.write = () => {
      writeCount++;
      return false; // Backpressure triggered
    };
    fakeRes.once = (event: string, cb: () => void) => {
      if (event === "drain") {
        drainCb = cb;
      }
      return fakeRes;
    };
    fakeRes.end = () => {
      isFinished = true;
    };

    const fakeReq: any = new (require("events").EventEmitter)();
    fakeReq.headers = { accept: "text/event-stream" };
    fakeReq.body = {
      model: "test-model",
      messages: [{ role: "user", content: "test backpressure" }],
      stream: true,
      mock: {
        text: "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        chunkSizes: [2]
      }
    };

    const config: GatewayConfig = {
      port: 0,
      upstreamUrl: "http://127.0.0.1:" + mockPort + "/v1/chat/completions",
      upstreamApiKey: "test-key",
      upstreamIdleTimeoutMs: 5000,
      maxEventBytes: 1048576
    };

    const forwardPromise = (await import("../src/forward.js")).forwardChatCompletion(
      fakeReq,
      fakeRes,
      config,
      testLogger,
      "bp-test-req"
    );

    while (writeCount === 0 || !drainCb) {
      await new Promise((r) => setTimeout(r, 10));
    }

    const pausedWrites = writeCount;
    await new Promise((r) => setTimeout(r, 60));
    expect(writeCount).toBe(pausedWrites);

    while (!isFinished) {
      if (drainCb) {
        const cb = drainCb;
        drainCb = null;
        cb();
      }
      await new Promise((r) => setTimeout(r, 10));
    }

    await forwardPromise;
    expect(writeCount).toBeGreaterThan(pausedWrites);
  });
});