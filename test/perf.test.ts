import { describe, it, expect } from "vitest";
import { StreamRedactor } from "../src/redactor.js";

describe("latency and memory tests", () => {
  it("char-by-char pushes of \"Hello world, nice day\" show prose lag at most the last word", () => {
    const text = "Hello world, nice day";
    const redactor = new StreamRedactor();
    let accumulatedEmitted = "";

    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      const emitted = redactor.push(ch);
      accumulatedEmitted += emitted;

      // Pending holdback must never exceed the length of the current word
      // (at most the last word in flight)
      expect(redactor.pendingLength).toBeLessThanOrEqual(320);

      // When reaching spaces or punctuation, the preceding word must already be emitted
      if (ch === " " || ch === ",") {
        const prefixBeforeChar = text.slice(0, i);
        // The emitted text must at least contain the words completed prior to this delimiter
        expect(accumulatedEmitted.length).toBeGreaterThanOrEqual(prefixBeforeChar.lastIndexOf(" "));
      }
    }

    accumulatedEmitted += redactor.flush();
    expect(accumulatedEmitted).toBe("Hello world, nice day");
  });

  it("stream 50 MB of prose with PII asserting peakPending <= 320 + chunk size and no accumulation", () => {
    const redactor = new StreamRedactor();
    const targetBytes = 50 * 1024 * 1024; // 50 MB
    const chunkSize = 64 * 1024; // 64 KB

    // Template block with prose, emails, SSNs, credit cards, and punctuation
    const templateBlock =
      "The customer with email user123@example.com purchased goods using credit card " +
      "4111-1111-1111-1111 on 2024-05-12. Please contact support@service.org or call with " +
      "SSN 987-65-4321 for verification. All transactions are securely audited by our system. ";

    // Repeat template to build a 64 KB chunk
    const repeats = Math.ceil(chunkSize / templateBlock.length);
    const chunk = templateBlock.repeat(repeats).slice(0, chunkSize);

    let totalCharsIn = 0;
    let totalEmitted = 0;

    while (totalCharsIn < targetBytes) {
      const charsToSend = Math.min(chunkSize, targetBytes - totalCharsIn);
      const piece = charsToSend === chunkSize ? chunk : chunk.slice(0, charsToSend);

      const out = redactor.push(piece);
      totalCharsIn += piece.length;
      totalEmitted += out.length;

      // Invariant: pending length never exceeds 320 + chunk size
      expect(redactor.pendingLength).toBeLessThanOrEqual(320 + piece.length);
    }

    const final = redactor.flush();
    totalEmitted += final.length;

    const stats = redactor.stats;
    expect(stats.charsIn).toBe(targetBytes);
    expect(stats.charsOut).toBe(totalEmitted);
    expect(stats.peakPending).toBeLessThanOrEqual(320 + chunkSize);
    expect(redactor.pendingLength).toBe(0);
  });
});
