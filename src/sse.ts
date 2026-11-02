export interface SseEvent {
  type: "event";
  event?: string;
  id?: string;
  data: string;
  isDone: boolean;
  raw: string;
}

export interface SseComment {
  type: "comment";
  comment: string;
  raw: string;
}

export type SseMessage = SseEvent | SseComment;

export class SseOversizedEventError extends Error {
  constructor(size: number, limit: number) {
    super(`SSE event size (${size} bytes) exceeded maximum allowed limit (${limit} bytes)`);
    this.name = "SseOversizedEventError";
  }
}

export class SseParser {
  private decoder = new TextDecoder("utf-8");
  private buffer = "";
  private scanOffset = 0;
  private maxEventBytes: number;

  private currentDataLines: string[] = [];
  private currentEventName?: string;
  private currentId?: string;
  private currentRawLines: string[] = [];
  private currentEventByteCount = 0;

  constructor(maxEventBytes = 1048576) {
    this.maxEventBytes = maxEventBytes;
  }

  /**
   * Pushes a chunk (Uint8Array or decoded string) into the parser
   * and returns any fully parsed SSE messages.
   */
  push(chunk: Uint8Array | string): SseMessage[] {
    let text: string;
    if (typeof chunk === "string") {
      text = chunk;
    } else {
      text = this.decoder.decode(chunk, { stream: true });
    }

    if (!text) {
      return [];
    }

    this.buffer += text;
    return this.parseBuffer();
  }

  /**
   * Flushes any remaining bytes in the decoder and buffer.
   */
  flush(): SseMessage[] {
    const remainingText = this.decoder.decode();
    if (remainingText) {
      this.buffer += remainingText;
    }

    const messages = this.parseBuffer();

    // If there is any trailing unterminated event with data, dispatch it
    if (this.currentDataLines.length > 0 || this.currentEventName || this.currentId) {
      const data = this.currentDataLines.join("\n");
      const raw = this.currentRawLines.join("") + (this.buffer ? this.buffer : "");
      messages.push({
        type: "event",
        event: this.currentEventName,
        id: this.currentId,
        data,
        isDone: data.trim() === "[DONE]",
        raw
      });
      this.resetEventState();
      this.buffer = "";
      this.scanOffset = 0;
    } else if (this.buffer.startsWith(":")) {
      const commentText = this.buffer.slice(1).replace(/^\s/, "");
      messages.push({
        type: "comment",
        comment: commentText,
        raw: this.buffer
      });
      this.buffer = "";
      this.scanOffset = 0;
    }

    return messages;
  }

  private resetEventState(): void {
    this.currentDataLines = [];
    this.currentEventName = undefined;
    this.currentId = undefined;
    this.currentRawLines = [];
    this.currentEventByteCount = 0;
  }

  private parseBuffer(): SseMessage[] {
    const messages: SseMessage[] = [];

    while (this.scanOffset < this.buffer.length) {
      let newlineIdx = -1;
      let delimLen = 1;

      for (let i = this.scanOffset; i < this.buffer.length; i++) {
        const code = this.buffer.charCodeAt(i);
        if (code === 10 /* \n */) {
          newlineIdx = i;
          delimLen = 1;
          break;
        } else if (code === 13 /* \r */) {
          if (i === this.buffer.length - 1) {
            // \r at buffer boundary: wait for potential \n in next chunk
            this.scanOffset = i;
            return messages;
          }
          if (this.buffer.charCodeAt(i + 1) === 10 /* \n */) {
            newlineIdx = i;
            delimLen = 2;
          } else {
            newlineIdx = i;
            delimLen = 1;
          }
          break;
        }
      }

      if (newlineIdx === -1) {
        // No more newlines in buffer
        this.scanOffset = this.buffer.length;
        if (this.currentEventByteCount + this.buffer.length > this.maxEventBytes) {
          throw new SseOversizedEventError(
            this.currentEventByteCount + this.buffer.length,
            this.maxEventBytes
          );
        }
        break;
      }

      const line = this.buffer.slice(0, newlineIdx);
      const rawDelim = this.buffer.slice(newlineIdx, newlineIdx + delimLen);
      const rawLineWithDelim = line + rawDelim;

      // Advance buffer past this line
      this.buffer = this.buffer.slice(newlineIdx + delimLen);
      this.scanOffset = 0;

      // Track byte size for maxEventBytes limit check
      this.currentEventByteCount += Buffer.byteLength(rawLineWithDelim);
      if (this.currentEventByteCount > this.maxEventBytes) {
        throw new SseOversizedEventError(this.currentEventByteCount, this.maxEventBytes);
      }

      // Check line semantics
      if (line === "") {
        // Empty line indicates event boundary
        if (this.currentDataLines.length > 0 || this.currentEventName || this.currentId) {
          const data = this.currentDataLines.join("\n");
          this.currentRawLines.push(rawLineWithDelim);
          const raw = this.currentRawLines.join("");
          messages.push({
            type: "event",
            event: this.currentEventName,
            id: this.currentId,
            data,
            isDone: data.trim() === "[DONE]",
            raw
          });
        }
        this.resetEventState();
      } else if (line.startsWith(":")) {
        // Comment / keepalive
        const comment = line.slice(1).replace(/^\s/, "");
        messages.push({
          type: "comment",
          comment,
          raw: rawLineWithDelim
        });
      } else {
        this.currentRawLines.push(rawLineWithDelim);
        if (line.startsWith("data:")) {
          let dataVal = line.slice(5);
          if (dataVal.startsWith(" ")) {
            dataVal = dataVal.slice(1);
          }
          this.currentDataLines.push(dataVal);
        } else if (line.startsWith("event:")) {
          let eventVal = line.slice(6);
          if (eventVal.startsWith(" ")) {
            eventVal = eventVal.slice(1);
          }
          this.currentEventName = eventVal;
        } else if (line.startsWith("id:")) {
          let idVal = line.slice(3);
          if (idVal.startsWith(" ")) {
            idVal = idVal.slice(1);
          }
          this.currentId = idVal;
        }
      }
    }

    return messages;
  }
}

/**
 * Serializes an SSE event object into raw wire format.
 */
export function formatSseEvent(data: string, eventName?: string, id?: string): string {
  let out = "";
  if (id !== undefined) {
    out += `id: ${id}\n`;
  }
  if (eventName !== undefined) {
    out += `event: ${eventName}\n`;
  }
  const lines = data.split("\n");
  for (const l of lines) {
    out += `data: ${l}\n`;
  }
  out += "\n";
  return out;
}
