import { type SseMessage, type SseEvent, formatSseEvent } from "./sse.js";
import { StreamRedactor, redactWhole, type RedactorStats } from "./redactor.js";

export interface TransformStats {
  peakPending: number;
  charsIn: number;
  charsOut: number;
  counts: {
    email: number;
    ssn: number;
    card: number;
    total: number;
  };
}

function makeSseEvent(data: string, eventName?: string, id?: string): SseEvent {
  return {
    type: "event",
    event: eventName,
    id,
    data,
    isDone: data.trim() === "[DONE]",
    raw: formatSseEvent(data, eventName, id)
  };
}

export class StreamTransformer {
  private redactors = new Map<string, StreamRedactor>();
  private peakPending = 0;
  private lastId = "";
  private lastCreated = 0;
  private lastModel = "";

  private getRedactor(key: string): StreamRedactor {
    let r = this.redactors.get(key);
    if (!r) {
      r = new StreamRedactor();
      this.redactors.set(key, r);
    }
    return r;
  }

  private updatePeakPending(): void {
    let currentTotal = 0;
    for (const r of this.redactors.values()) {
      currentTotal += r.pendingLength;
    }
    if (currentTotal > this.peakPending) {
      this.peakPending = currentTotal;
    }
  }

  getStats(): TransformStats {
    let charsIn = 0;
    let charsOut = 0;
    const counts = { email: 0, ssn: 0, card: 0, total: 0 };

    for (const r of this.redactors.values()) {
      const s = r.stats;
      charsIn += s.charsIn;
      charsOut += s.charsOut;
      counts.email += s.counts.email;
      counts.ssn += s.counts.ssn;
      counts.card += s.counts.card;
      counts.total += s.counts.total;
    }

    return {
      peakPending: this.peakPending,
      charsIn,
      charsOut,
      counts
    };
  }

  async *transform(source: AsyncIterable<SseMessage>): AsyncGenerator<SseMessage> {
    for await (const msg of source) {
      // Pass through comments directly
      if (msg.type === "comment") {
        yield msg;
        continue;
      }

      const event = msg as SseEvent;

      // Handle [DONE]
      if (event.data === "[DONE]") {
        yield* this.flushAllPending();
        yield event;
        return;
      }

      // Try parsing JSON payload
      let parsed: any;
      try {
        parsed = JSON.parse(event.data);
      } catch {
        // Fail-closed: invalid JSON is redacted over the whole data string
        yield makeSseEvent(redactWhole(event.data), event.event, event.id);
        continue;
      }

      // Store metadata for synthetic flushes
      if (parsed.id) this.lastId = parsed.id;
      if (parsed.created) this.lastCreated = parsed.created;
      if (parsed.model) this.lastModel = parsed.model;

      // If choices is not an array, pass through untouched
      if (!Array.isArray(parsed.choices)) {
        yield event;
        continue;
      }

      let dropEntireEvent = true;
      const choices = parsed.choices;
      const preFinishChunks: SseMessage[] = [];

      for (let i = 0; i < choices.length; i++) {
        const choice = choices[i];
        const cIdx = choice.index ?? i;
        const delta = choice.delta;

        if (!delta) {
          if (choice.finish_reason) {
            dropEntireEvent = false;
          }
          continue;
        }

        // 1. Role-only first chunk: pass through immediately for TTFT
        const hasRole = typeof delta.role === "string";
        const hasContent = typeof delta.content === "string";
        const hasTools = Array.isArray(delta.tool_calls);

        if (hasRole && !hasContent && !hasTools) {
          dropEntireEvent = false;
          continue;
        }

        // 2. Content redaction
        if (hasContent) {
          const redactorKey = `${cIdx}:content`;
          const redactor = this.getRedactor(redactorKey);
          const emitted = redactor.push(delta.content);
          this.updatePeakPending();

          delta.content = emitted;
          if (emitted.length > 0) {
            dropEntireEvent = false;
          }
        }

        // 3. Tool call arguments redaction
        if (hasTools) {
          let anyToolEmitted = false;
          for (let t = 0; t < delta.tool_calls.length; t++) {
            const tc = delta.tool_calls[t];
            const tIdx = tc.index ?? t;
            if (tc.function && typeof tc.function.arguments === "string") {
              const redactorKey = `${cIdx}:tool:${tIdx}:args`;
              const redactor = this.getRedactor(redactorKey);
              const emitted = redactor.push(tc.function.arguments);
              this.updatePeakPending();

              tc.function.arguments = emitted;
              if (emitted.length > 0) {
                anyToolEmitted = true;
              }
            } else {
              // Metadata like id, type, name in tool call
              anyToolEmitted = true;
            }
          }
          if (anyToolEmitted) {
            dropEntireEvent = false;
          }
        }

        // 4. finish_reason ordering: flushed text MUST precede the finish event!
        if (choice.finish_reason) {
          dropEntireEvent = false;

          // Flush content redactor for this choice
          const contentKey = `${cIdx}:content`;
          const contentRedactor = this.redactors.get(contentKey);
          if (contentRedactor && contentRedactor.pendingLength > 0) {
            const flushed = contentRedactor.flush();
            this.updatePeakPending();
            if (flushed.length > 0) {
              preFinishChunks.push(
                makeSseEvent(
                  JSON.stringify({
                    id: this.lastId || parsed.id || "chatcmpl-gateway",
                    object: "chat.completion.chunk",
                    created: this.lastCreated || parsed.created || Math.floor(Date.now() / 1000),
                    model: this.lastModel || parsed.model || "unknown",
                    choices: [
                      {
                        index: cIdx,
                        delta: { content: flushed },
                        finish_reason: null
                      }
                    ]
                  }),
                  event.event
                )
              );
            }
          }

          // Flush tool redactors for this choice
          for (const [key, r] of this.redactors.entries()) {
            if (key.startsWith(`${cIdx}:tool:`) && r.pendingLength > 0) {
              const parts = key.split(":");
              const tIdx = parseInt(parts[2], 10) || 0;
              const flushed = r.flush();
              this.updatePeakPending();
              if (flushed.length > 0) {
                preFinishChunks.push(
                  makeSseEvent(
                    JSON.stringify({
                      id: this.lastId || parsed.id || "chatcmpl-gateway",
                      object: "chat.completion.chunk",
                      created: this.lastCreated || parsed.created || Math.floor(Date.now() / 1000),
                      model: this.lastModel || parsed.model || "unknown",
                      choices: [
                        {
                          index: cIdx,
                          delta: {
                            tool_calls: [
                              {
                                index: tIdx,
                                function: { arguments: flushed }
                              }
                            ]
                          },
                          finish_reason: null
                        }
                      ]
                    }),
                    event.event
                  )
                );
              }
            }
          }
        }
      }

      // Yield any pre-finish flushed chunks first
      for (const preChunk of preFinishChunks) {
        yield preChunk;
      }

      // If every choice delta resulted in empty content and no finish_reason, drop the event
      if (!dropEntireEvent) {
        yield makeSseEvent(JSON.stringify(parsed), event.event, event.id);
      }
    }

    // Upstream stream ended without [DONE]: flush any remaining held text
    yield* this.flushAllPending();
  }

  private *flushAllPending(): Generator<SseMessage> {
    for (const [key, r] of this.redactors.entries()) {
      if (r.pendingLength > 0) {
        const flushed = r.flush();
        this.updatePeakPending();
        if (flushed.length > 0) {
          if (key.includes(":tool:")) {
            const parts = key.split(":");
            const cIdx = parseInt(parts[0], 10) || 0;
            const tIdx = parseInt(parts[2], 10) || 0;
            yield makeSseEvent(
              JSON.stringify({
                id: this.lastId || "chatcmpl-gateway",
                object: "chat.completion.chunk",
                created: this.lastCreated || Math.floor(Date.now() / 1000),
                model: this.lastModel || "unknown",
                choices: [
                  {
                    index: cIdx,
                    delta: {
                      tool_calls: [
                        {
                          index: tIdx,
                          function: { arguments: flushed }
                        }
                      ]
                    },
                    finish_reason: null
                  }
                ]
              })
            );
          } else {
            const cIdx = parseInt(key.split(":")[0], 10) || 0;
            yield makeSseEvent(
              JSON.stringify({
                id: this.lastId || "chatcmpl-gateway",
                object: "chat.completion.chunk",
                created: this.lastCreated || Math.floor(Date.now() / 1000),
                model: this.lastModel || "unknown",
                choices: [
                  {
                    index: cIdx,
                    delta: { content: flushed },
                    finish_reason: null
                  }
                ]
              })
            );
          }
        }
      }
    }
  }
}
