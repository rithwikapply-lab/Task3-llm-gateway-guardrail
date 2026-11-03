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
  it("regression: long digit/dash runs preceding valid card with left-context", () => {
    const text = "084253-272931126665012-9 718-3 9 0927906429351 6-9 ";
    const expected = redactWhole(text);
    const redactor = new StreamRedactor();
    let streamed = "";
    for (let i = 0; i < text.length; i++) {
      streamed += redactor.push(text[i]);
      expect(redactor.pendingLength).toBeLessThanOrEqual(320 + 1);
    }
    streamed += redactor.flush();
    expect(streamed).toBe(expected);
  });

  function mulberry32(a: number) {
    return function () {
      let t = (a += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  const FUZZ_SEED = 439041101;
  const rng = mulberry32(FUZZ_SEED);

  function makeLuhnCard(valid: boolean): string {
    const len = 15;
    let prefix = "";
    for (let i = 0; i < len; i++) prefix += Math.floor(rng() * 10);
    for (let d = 0; d <= 9; d++) {
      const candidate = prefix + d;
      if (isValidLuhn(candidate) === valid) {
        if (rng() < 0.3) return candidate.slice(0, 4) + "-" + candidate.slice(4, 8) + "-" + candidate.slice(8, 12) + "-" + candidate.slice(12);
        if (rng() < 0.6) return candidate.slice(0, 4) + " " + candidate.slice(4, 8) + " " + candidate.slice(8, 12) + " " + candidate.slice(12);
        return candidate;
      }
    }
    return "4111111111111111";
  }

  const proseWords = ["the", "quick", "brown", "fox", "meeting", "invoice", "payment", "customer", "order", "status", "report", "system", "client", "service"];
  const nearMisses = ["1234-56-7890", "123-45-67890", "a@b", "v1.2.3", "2024-05-18", "1999-12-31", "@handle", "user@.com", "12-34-5678", "user@-domain.com"];
  const unicodeSamples = ["café ☕", "日本語テスト", "🚀🎉", "über", "naïve", "Москва", "hello世界"];

  function generateFuzzText(): string {
    const parts: string[] = [];
    const count = 3 + Math.floor(rng() * 5);
    for (let i = 0; i < count; i++) {
      const kind = Math.floor(rng() * 10);
      switch (kind) {
        case 0: parts.push(proseWords[Math.floor(rng() * proseWords.length)]); break;
        case 1: parts.push(`user${Math.floor(rng() * 1000)}@example.com`); break;
        case 2: parts.push(`987-${String(Math.floor(rng() * 90) + 10)}-${String(Math.floor(rng() * 9000) + 1000)}`); break;
        case 3: parts.push(makeLuhnCard(true)); break;
        case 4: parts.push(makeLuhnCard(false)); break;
        case 5: parts.push(nearMisses[Math.floor(rng() * nearMisses.length)]); break;
        case 6: {
          const len = 50 + Math.floor(rng() * 651);
          let s = "";
          for (let l = 0; l < len; l++) s += String.fromCharCode(97 + Math.floor(rng() * 26));
          parts.push(s);
          break;
        }
        case 7: {
          const len = 30 + Math.floor(rng() * 71);
          const chars = "0123456789 -";
          let s = "";
          for (let l = 0; l < len; l++) s += chars[Math.floor(rng() * chars.length)];
          parts.push(s);
          break;
        }
        case 8: {
          parts.push(`(user${Math.floor(rng() * 100)}@test.com,${makeLuhnCard(true)}):987-65-4321.`);
          break;
        }
        case 9: parts.push(unicodeSamples[Math.floor(rng() * unicodeSamples.length)]); break;
      }
    }
    return parts.join(rng() < 0.5 ? " " : ", ");
  }

  it("verifies invariants 1 to 3 across 5000 random texts and chunkings", () => {
    for (let iter = 0; iter < 5000; iter++) {
      const text = generateFuzzText();
      const expected = redactWhole(text);
      const matches = findMatches(text);
      const piiTexts = matches.map((m) => m.text!).filter(Boolean);

      const chunkStrategies = [
        () => [1], // 1-character chunks
        () => [text.length + 100], // chunk larger than text
        () => [1 + Math.floor(rng() * 15), 1 + Math.floor(rng() * 35)] // random variable chunks
      ];

      for (const getSizes of chunkStrategies) {
        const chunkSizes = getSizes();
        const redactor = new StreamRedactor();
        let streamed = "";
        let offset = 0;
        let sizeIdx = 0;

        while (offset < text.length) {
          const size = chunkSizes[sizeIdx % chunkSizes.length];
          sizeIdx++;
          const chunk = text.slice(offset, offset + size);
          offset += size;

          const out = redactor.push(chunk);
          streamed += out;

          if (redactor.pendingLength > 320 + chunk.length) {
            expect.unreachable(
              `Invariant 2 failed: pending length ${redactor.pendingLength} > 320 + ${chunk.length} on seed ${FUZZ_SEED}, iter ${iter}\nText: ${text}`
            );
          }
        }

        streamed += redactor.flush();

        if (streamed !== expected) {
          expect.unreachable(
            `Invariant 1 failed on seed ${FUZZ_SEED}, iter ${iter}\nText: ${text}\nExpected: ${expected}\nStreamed: ${streamed}`
          );
        }

        for (const pii of piiTexts) {
          if (streamed.includes(pii)) {
            expect.unreachable(
              `Invariant 3 failed: leaked PII ${pii} on seed ${FUZZ_SEED}, iter ${iter}\nText: ${text}`
            );
          }
        }
      }
    }
  });
});
