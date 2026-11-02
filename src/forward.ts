import type { Request, Response } from "express";
import type { GatewayConfig } from "./config.js";
import { Logger } from "./logger.js";
import { SseParser, formatSseEvent, type SseMessage } from "./sse.js";
import { StreamTransformer } from "./transform.js";
import { redactWhole, findMatches } from "./redactor.js";

export async function forwardChatCompletion(
  req: Request,
  res: Response,
  config: GatewayConfig,
  logger: Logger,
  reqId: string
): Promise<void> {
  const startTime = performance.now();
  let ttftMs: number | null = null;
  let bytesIn = 0;
  let bytesOut = 0;
  let status = 200;

  // Build upstream headers: forward only content-type and accept.
  // The client Authorization is stripped; upstream API key is injected if present.
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "accept": "text/event-stream"
  };

  if (typeof req.headers["accept"] === "string") {
    headers["accept"] = req.headers["accept"];
  }

  if (config.upstreamApiKey) {
    headers["authorization"] = `Bearer ${config.upstreamApiKey}`;
  }

  const isStream = req.body && req.body.stream === true;
  const abortController = new AbortController();
  let clientDisconnected = false;
  let upstreamFinished = false;

  const onClientClose = () => {
    if (!upstreamFinished) {
      clientDisconnected = true;
      abortController.abort();
    }
  };
  res.on("close", onClientClose);

  // 1. Non-streaming handling
  if (!isStream) {
    try {
      const upstreamRes = await fetch(config.upstreamUrl, {
        method: "POST",
        headers,
        body: JSON.stringify(req.body),
        signal: abortController.signal
      });

      status = upstreamRes.status;
      const resText = await upstreamRes.text();
      bytesIn += Buffer.byteLength(resText);

      if (!upstreamRes.ok) {
        const totalMs = Math.round(performance.now() - startTime);
        res.status(status).json({
          error: {
            message: "Upstream service error",
            type: "upstream_error",
            code: status
          }
        });
        logger.logRequest({
          reqId,
          status,
          ttftMs: null,
          totalMs,
          bytesIn,
          bytesOut: 0,
          redactions: { email: 0, ssn: 0, card: 0, total: 0 },
          peakPending: 0
        });
        return;
      }

      let parsed: any;
      try {
        parsed = JSON.parse(resText);
      } catch {
        parsed = null;
      }

      const redactionCounts = { email: 0, ssn: 0, card: 0, total: 0 };

      if (parsed && Array.isArray(parsed.choices)) {
        for (const choice of parsed.choices) {
          if (choice.message) {
            if (typeof choice.message.content === "string") {
              const matches = findMatches(choice.message.content);
              for (const m of matches) {
                redactionCounts[m.type]++;
                redactionCounts.total++;
              }
              choice.message.content = redactWhole(choice.message.content);
            }
            if (Array.isArray(choice.message.tool_calls)) {
              for (const tc of choice.message.tool_calls) {
                if (tc.function && typeof tc.function.arguments === "string") {
                  const matches = findMatches(tc.function.arguments);
                  for (const m of matches) {
                    redactionCounts[m.type]++;
                    redactionCounts.total++;
                  }
                  tc.function.arguments = redactWhole(tc.function.arguments);
                }
              }
            }
          }
        }
      }

      const bodyOut = parsed ? JSON.stringify(parsed) : redactWhole(resText);
      bytesOut = Buffer.byteLength(bodyOut);
      const totalMs = Math.round(performance.now() - startTime);

      res.status(200).type("application/json").send(bodyOut);

      logger.logRequest({
        reqId,
        status: 200,
        ttftMs: totalMs,
        totalMs,
        bytesIn,
        bytesOut,
        redactions: redactionCounts,
        peakPending: 0
      });
      return;
    } catch (err: any) {
      if (clientDisconnected) return;
      const totalMs = Math.round(performance.now() - startTime);
      status = 502;
      res.status(502).json({
        error: {
          message: "Bad Gateway: unable to connect to upstream provider",
          type: "bad_gateway"
        }
      });
      logger.logRequest({
        reqId,
        status: 502,
        ttftMs: null,
        totalMs,
        bytesIn,
        bytesOut: 0,
        redactions: { email: 0, ssn: 0, card: 0, total: 0 },
        peakPending: 0
      });
      return;
    }
  }

  // 2. Streaming handling
  let upstreamRes: globalThis.Response;
  try {
    upstreamRes = await fetch(config.upstreamUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(req.body),
      signal: abortController.signal
    });
  } catch (err: any) {
    if (clientDisconnected) return;
    const totalMs = Math.round(performance.now() - startTime);
    status = 502;
    res.status(502).json({
      error: {
        message: "Bad Gateway: unable to connect to upstream provider",
        type: "bad_gateway"
      }
    });
    logger.logRequest({
      reqId,
      status: 502,
      ttftMs: null,
      totalMs,
      bytesIn: 0,
      bytesOut: 0,
      redactions: { email: 0, ssn: 0, card: 0, total: 0 },
      peakPending: 0
    });
    return;
  }

  status = upstreamRes.status;
  if (!upstreamRes.ok) {
    const totalMs = Math.round(performance.now() - startTime);
    res.status(status).json({
      error: {
        message: "Upstream service error",
        type: "upstream_error",
        code: status
      }
    });
    logger.logRequest({
      reqId,
      status,
      ttftMs: null,
      totalMs,
      bytesIn: 0,
      bytesOut: 0,
      redactions: { email: 0, ssn: 0, card: 0, total: 0 },
      peakPending: 0
    });
    return;
  }

  // Response headers for SSE
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no"
  });

  const parser = new SseParser(config.maxEventBytes);
  const transformer = new StreamTransformer();
  const reader = upstreamRes.body?.getReader();

  if (!reader) {
    upstreamFinished = true;
    res.end();
    return;
  }

  const writeEventWithBackpressure = async (wire: string): Promise<void> => {
    if (ttftMs === null) {
      ttftMs = Math.round(performance.now() - startTime);
    }
    bytesOut += Buffer.byteLength(wire);
    const ok = res.write(wire);
    if (!ok && !clientDisconnected) {
      await new Promise<void>((resolve) => res.once("drain", resolve));
    }
  };

  try {
    while (true) {
      let timer: NodeJS.Timeout | null = null;
      const timeoutPromise = new Promise<{ value: undefined; done: true; timeout: boolean }>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error("Upstream idle timeout"));
        }, config.upstreamIdleTimeoutMs);
      });

      let readResult: ReadableStreamReadResult<Uint8Array>;
      try {
        readResult = await Promise.race([reader.read(), timeoutPromise]);
      } finally {
        if (timer) clearTimeout(timer);
      }

      if (readResult.done) {
        break;
      }

      const chunk = readResult.value;
      if (chunk) {
        bytesIn += chunk.byteLength;
        const messages = parser.push(chunk);
        if (messages.length > 0) {
          for await (const outMsg of transformer.transform(messages)) {
            await writeEventWithBackpressure(outMsg.raw);
          }
        }
      }
    }

    // Upstream stream completed cleanly
    upstreamFinished = true;
    const flushed = parser.flush();
    if (flushed.length > 0) {
      for await (const outMsg of transformer.transform(flushed)) {
        await writeEventWithBackpressure(outMsg.raw);
      }
    } else {
      // Ensure transformer flushes any remaining held text
      for await (const outMsg of transformer.transform([])) {
        await writeEventWithBackpressure(outMsg.raw);
      }
    }

    res.end();
  } catch (err: any) {
    upstreamFinished = true;
    if (!clientDisconnected) {
      // Flush held text first before sending error
      try {
        const remaining = parser.flush();
        for await (const outMsg of transformer.transform(remaining)) {
          await writeEventWithBackpressure(outMsg.raw);
        }
      } catch {}

      const isTimeout = err?.message === "Upstream idle timeout";
      const errMsg = isTimeout ? "Upstream idle timeout" : "Upstream connection dropped";
      const errType = isTimeout ? "timeout_error" : "upstream_drop";
      const errWire = formatSseEvent(
        JSON.stringify({ error: { message: errMsg, type: errType } }),
        "error"
      );
      await writeEventWithBackpressure(errWire);
      res.end();
    }
  } finally {
    const totalMs = Math.round(performance.now() - startTime);
    const stats = transformer.getStats();
    logger.logRequest({
      reqId,
      status: 200,
      ttftMs: ttftMs ?? totalMs,
      totalMs,
      bytesIn,
      bytesOut,
      redactions: stats.counts,
      peakPending: stats.peakPending
    });
  }
}
