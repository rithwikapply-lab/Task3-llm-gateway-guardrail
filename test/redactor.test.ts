import { describe, it, expect } from "vitest";
import {
  StreamRedactor,
  redactWhole,
  isValidLuhn,
  findMatches
} from "../src/redactor.js";

describe("redactor pattern tests", () => {
  it("redacts each pattern alone", () => {
    expect(redactWhole("user@example.com")).toBe("[REDACTED]");
    expect(redactWhole("123-45-6789")).toBe("[REDACTED]");
    expect(redactWhole("4111-1111-1111-1111")).toBe("[REDACTED]");
    expect(redactWhole("4111 1111 1111 1111")).toBe("[REDACTED]");
  });

  it("leaves near-misses untouched", () => {
    const nearMisses = [
      "123-45-678",
      "1234-56-7890",
      "123-45-67890",
      "2024-01-15",
      "version 1.2.3",
      "a@b",
      "@handle",
      "4111-1111-1111-1112" // fails Luhn
    ];

    for (const nm of nearMisses) {
      expect(redactWhole(nm)).toBe(nm);

      // Also test via StreamRedactor
      const r = new StreamRedactor();
      const streamed = r.push(nm) + r.flush();
      expect(streamed).toBe(nm);
    }
  });

  it("handles trailing punctuation properly", () => {
    expect(redactWhole("mail a@b.com.")).toBe("mail [REDACTED].");
    expect(redactWhole("contact: support@corp.org, please")).toBe("contact: [REDACTED], please");
    expect(redactWhole("SSN is 123-45-6789!")).toBe("SSN is [REDACTED]!");
  });
});

describe("exhaustive single and 3-way split tests", () => {
  const samples = [
    "user@domain.com is my email", // PII at start
    "Call 123-45-6789 today",      // PII at middle
    "Card is 4111-1111-1111-1111", // PII at end
    "user1@a.com user2@b.com 123-45-6789", // Several in a row
    "mail a@b.com."                // Trailing punctuation keeps .
  ];

  for (const sample of samples) {
    const expected = redactWhole(sample);

    it(`every single split point of "${sample}"`, () => {
      for (let i = 0; i <= sample.length; i++) {
        const r = new StreamRedactor();
        const p1 = sample.slice(0, i);
        const p2 = sample.slice(i);
        const out = r.push(p1) + r.push(p2) + r.flush();
        expect(out).toBe(expected);
      }
    });

    it(`every 3-way split of "${sample}"`, () => {
      for (let i = 0; i <= sample.length; i++) {
        for (let j = i; j <= sample.length; j++) {
          const r = new StreamRedactor();
          const p1 = sample.slice(0, i);
          const p2 = sample.slice(i, j);
          const p3 = sample.slice(j);
          const out = r.push(p1) + r.push(p2) + r.push(p3) + r.flush();
          expect(out).toBe(expected);
        }
      }
    });
  }
});

describe("adversarial input performance", () => {
  it("finishes 1 MB of a, 100k digits, and 100k dashes in under 1 second", () => {
    const start = performance.now();

    // 1 MB of "a" in 1 KB chunks
    const r1 = new StreamRedactor();
    const chunkA = "a".repeat(1024);
    for (let i = 0; i < 1024; i++) {
      r1.push(chunkA);
    }
    r1.flush();

    // 100k digits in 1 KB chunks
    const r2 = new StreamRedactor();
    const chunkD = "1234567890".repeat(100); // 1000 digits
    for (let i = 0; i < 100; i++) {
      r2.push(chunkD);
    }
    r2.flush();

    // 100k dashes in 1 KB chunks
    const r3 = new StreamRedactor();
    const chunkDash = "-".repeat(1000);
    for (let i = 0; i < 100; i++) {
      r3.push(chunkDash);
    }
    r3.flush();

    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(1000);
  });
});

describe("seeded fuzz test (2000 iterations)", () => {
  // Deterministic Mulberry32 PRNG
  function mulberry32(a: number) {
    return function () {
      let t = (a += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  const rng = mulberry32(133742);

  const fragments = [
    "The quick brown fox jumps over the lazy dog. ",
    "Contact us at hello@world.com for details. ",
    "My SSN is 987-65-4321 and her SSN is 123-45-6789. ",
    "Payment via 4111-1111-1111-1111 or 4111 1111 1111 1111. ",
    "Near misses: 123-45-678, 1234-56-7890, 2024-01-15, v1.2.3, a@b, @handle. ",
    "Failed card: 4111-1111-1111-1112. ",
    "00000000001111111111222222222233333333334444444444 ",
    "-------------------------------------------------- ",
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ",
    "Trailing punctuation test: email info@example.org. ",
    "End of sentence. "
  ];

  it("verifies invariants 1 to 3 across 2000 random chunkings", () => {
    for (let iter = 0; iter < 2000; iter++) {
      // Build random composite text from 2 to 6 fragments
      const numFrags = 2 + Math.floor(rng() * 5);
      let text = "";
      for (let f = 0; f < numFrags; f++) {
        text += fragments[Math.floor(rng() * fragments.length)];
      }

      // Expected output via reference redactWhole
      const expected = redactWhole(text);

      // Find all ground-truth PII substrings
      const matches = findMatches(text);
      const piiTexts = matches.map((m) => m.text!).filter(Boolean);

      // Stream through redactor with random chunk sizes
      const redactor = new StreamRedactor();
      let streamed = "";
      let offset = 0;

      while (offset < text.length) {
        // Chunk size between 1 and 25 (with occasional larger chunk)
        let chunkSize = 1 + Math.floor(rng() * 25);
        if (rng() < 0.05) chunkSize = 80;
        const chunk = text.slice(offset, offset + chunkSize);
        offset += chunkSize;

        const out = redactor.push(chunk);
        streamed += out;

        // Invariant 2: pending.length <= 320 + maxChunkSize
        expect(redactor.pendingLength).toBeLessThanOrEqual(320 + chunk.length);
      }

      streamed += redactor.flush();

      // Invariant 1: chunk-invariance
      expect(streamed).toBe(expected);

      // Invariant 3: no original PII substring appears in emitted output
      for (const pii of piiTexts) {
        expect(streamed.includes(pii)).toBe(false);
      }
    }
  });
});
