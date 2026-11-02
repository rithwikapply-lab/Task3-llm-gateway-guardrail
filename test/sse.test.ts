import { describe, it, expect } from "vitest";
import { SseParser, SseOversizedEventError, formatSseEvent, SseEvent, SseComment } from "../src/sse.js";

describe("SseParser", () => {
  it("parses single event with standard \n delimiter", () => {
    const parser = new SseParser();
    const msgs = parser.push("data: hello world\n\n");
    expect(msgs).toHaveLength(1);
    expect(msgs[0]).toEqual({
      type: "event",
      event: undefined,
      id: undefined,
      data: "hello world",
      isDone: false,
      raw: "data: hello world\n\n"
    });
  });

  it("handles \\r\\n and \\r line endings", () => {
    const parser = new SseParser();
    const msgs1 = parser.push("data: event1\r\n\r\n");
    expect(msgs1).toHaveLength(1);
    expect((msgs1[0] as SseEvent).data).toBe("event1");

    const msgs2 = parser.push("data: event2\r\rdata: event3\r\r");
    expect(msgs2.length).toBeGreaterThanOrEqual(1);
    expect((msgs2[0] as SseEvent).data).toBe("event2");
  });

  it("handles events split mid-line across chunks", () => {
    const parser = new SseParser();
    const msgs1 = parser.push("data: chunked ");
    expect(msgs1).toHaveLength(0);

    const msgs2 = parser.push("message\n\n");
    expect(msgs2).toHaveLength(1);
    expect((msgs2[0] as SseEvent).data).toBe("chunked message");
  });

  it("handles delimiter split across chunk boundaries (\\r then \\n)", () => {
    const parser = new SseParser();
    const msgs1 = parser.push("data: split delim\r\n\r");
    expect(msgs1).toHaveLength(0);

    const msgs2 = parser.push("\n");
    expect(msgs2).toHaveLength(1);
    expect((msgs2[0] as SseEvent).data).toBe("split delim");
  });

  it("handles multiple events per chunk", () => {
    const parser = new SseParser();
    const chunk = "data: first\n\ndata: second\n\ndata: third\n\n";
    const msgs = parser.push(chunk);
    expect(msgs).toHaveLength(3);
    expect(msgs.map((m) => m.type === "event" ? m.data : null)).toEqual([
      "first",
      "second",
      "third"
    ]);
  });

  it("passes comment lines through as comments without waiting for empty line", () => {
    const parser = new SseParser();
    const msgs1 = parser.push(": keepalive ping\n");
    expect(msgs1).toHaveLength(1);
    expect(msgs1[0]).toEqual({
      type: "comment",
      comment: "keepalive ping",
      raw: ": keepalive ping\n"
    });

    const msgs2 = parser.push("data: after comment\n\n");
    expect(msgs2).toHaveLength(1);
    expect((msgs2[0] as SseEvent).data).toBe("after comment");
  });

  it("concatenates multi-line data with newline per SSE spec", () => {
    const parser = new SseParser();
    const payload = "data: line 1\ndata: line 2\ndata: line 3\n\n";
    const msgs = parser.push(payload);
    expect(msgs).toHaveLength(1);
    expect((msgs[0] as SseEvent).data).toBe("line 1\nline 2\nline 3");
  });

  it("identifies data: [DONE] correctly", () => {
    const parser = new SseParser();
    const msgs = parser.push("data: [DONE]\n\n");
    expect(msgs).toHaveLength(1);
    expect((msgs[0] as SseEvent).data).toBe("[DONE]");
    expect((msgs[0] as any).isDone).toBe(true);
  });

  it("safely decodes multi-byte UTF-8 character split across network chunks", () => {
    const parser = new SseParser();
    // Rocket emoji '🚀' is 4 bytes in UTF-8: 0xF0, 0x9F, 0x9A, 0x80
    const prefix = Buffer.from("data: Hello ");
    const rocketPart1 = Buffer.from([0xf0, 0x9f]);
    const rocketPart2 = Buffer.from([0x9a, 0x80]);
    const suffix = Buffer.from(" world\n\n");

    const chunk1 = Buffer.concat([prefix, rocketPart1]);
    const chunk2 = Buffer.concat([rocketPart2, suffix]);

    const msgs1 = parser.push(new Uint8Array(chunk1));
    expect(msgs1).toHaveLength(0);

    const msgs2 = parser.push(new Uint8Array(chunk2));
    expect(msgs2).toHaveLength(1);
    expect((msgs2[0] as SseEvent).data).toBe("Hello 🚀 world");
  });

  it("throws SseOversizedEventError when an event exceeds MAX_EVENT_BYTES", () => {
    const limit = 100;
    const parser = new SseParser(limit);
    const bigData = "data: " + "x".repeat(120);

    expect(() => {
      parser.push(bigData);
    }).toThrow(SseOversizedEventError);
  });

  it("formats SSE events with formatSseEvent", () => {
    const formatted = formatSseEvent("hello\nworld", "custom_event", "id-42");
    expect(formatted).toBe("id: id-42\nevent: custom_event\ndata: hello\ndata: world\n\n");
  });

  it("flushes remaining data if stream terminates without trailing newline", () => {
    const parser = new SseParser();
    parser.push("data: unfinished");
    const msgs = parser.flush();
    expect(msgs).toHaveLength(1);
    expect((msgs[0] as SseEvent).data).toBe("unfinished");
  });
});
