export type PiiType = "email" | "card" | "ssn";

export interface PiiMatch {
  start: number;
  end: number;
  type: PiiType;
  text?: string;
}

export interface RedactorStats {
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

export const EMAIL_PATTERN = /(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]{1,64}@(?!-)(?:[A-Za-z0-9-]{1,63}\.)+[A-Za-z]{2,63}(?![A-Za-z0-9-])/g;
export const SSN_PATTERN = /(?<![\d-])\d{3}-\d{2}-\d{4}(?![\d-])/g;
export const CARD_PATTERN = /(?<![\d-])\d(?:[ -]?\d){12,18}(?![\d-])/g;

const EMAIL_CHAR_RE = /[A-Za-z0-9._%+@-]/;
const DIGIT_CHAR_RE = /[0-9 -]/;

/**
 * Validates credit card number digits using the Luhn checksum algorithm (Mod 10).
 */
export function isValidLuhn(digitsStr: string): boolean {
  const cleanDigits = digitsStr.replace(/[ -]/g, "");
  if (cleanDigits.length < 13 || cleanDigits.length > 19) {
    return false;
  }
  let sum = 0;
  let shouldDouble = false;
  for (let i = cleanDigits.length - 1; i >= 0; i--) {
    const digit = cleanDigits.charCodeAt(i) - 48;
    if (digit < 0 || digit > 9) return false;
    let val = digit;
    if (shouldDouble) {
      val *= 2;
      if (val > 9) val -= 9;
    }
    sum += val;
    shouldDouble = !shouldDouble;
  }
  return sum % 10 === 0;
}

/**
 * Finds all non-overlapping PII matches in text, prioritizing start position,
 * longer matches first, and type tie-breaking (email > card > ssn).
 */
export function findMatches(text: string): PiiMatch[] {
  const matches: PiiMatch[] = [];

  // 1. Email matches
  EMAIL_PATTERN.lastIndex = 0;
  let em: RegExpExecArray | null;
  while ((em = EMAIL_PATTERN.exec(text)) !== null) {
    if (em[0].length <= 320) {
      matches.push({ start: em.index, end: em.index + em[0].length, type: "email", text: em[0] });
    }
  }

  // 2. SSN matches
  SSN_PATTERN.lastIndex = 0;
  let sm: RegExpExecArray | null;
  while ((sm = SSN_PATTERN.exec(text)) !== null) {
    matches.push({ start: sm.index, end: sm.index + sm[0].length, type: "ssn", text: sm[0] });
  }

  // 3. Card matches (Luhn verified)
  CARD_PATTERN.lastIndex = 0;
  let cm: RegExpExecArray | null;
  while ((cm = CARD_PATTERN.exec(text)) !== null) {
    if (isValidLuhn(cm[0])) {
      matches.push({ start: cm.index, end: cm.index + cm[0].length, type: "card", text: cm[0] });
    } else {
      CARD_PATTERN.lastIndex = cm.index + 1;
    }
  }

  // Sort matches: start asc, length desc, type priority (email, card, ssn)
  const typeOrder: Record<PiiType, number> = { email: 1, card: 2, ssn: 3 };
  matches.sort((a, b) => {
    if (a.start !== b.start) return a.start - b.start;
    const lenA = a.end - a.start;
    const lenB = b.end - b.start;
    if (lenB !== lenA) return lenB - lenA;
    return typeOrder[a.type] - typeOrder[b.type];
  });

  // Drop overlapping matches
  const nonOverlapping: PiiMatch[] = [];
  let lastEnd = -1;
  for (const m of matches) {
    if (m.start >= lastEnd) {
      nonOverlapping.push(m);
      lastEnd = m.end;
    }
  }

  return nonOverlapping;
}

/**
 * Synchronous reference implementation replacing all detected PII with [REDACTED].
 */
export function redactWhole(text: string): string {
  const matches = findMatches(text);
  if (matches.length === 0) return text;
  let result = "";
  let lastIdx = 0;
  for (const m of matches) {
    result += text.slice(lastIdx, m.start) + "[REDACTED]";
    lastIdx = m.end;
  }
  result += text.slice(lastIdx);
  return result;
}

/**
 * Streaming redactor with bounded holdback and left context.
 * Invariants:
 * 1. Chunk-invariance: concat(push(chunk)) + flush() === redactWhole(fullText)
 * 2. pending.length <= 320 + maxChunkSize
 * 3. No original PII substring appears in any emitted output.
 */
export class StreamRedactor {
  private pending = "";
  private ctx = "";
  private peakPendingCount = 0;
  private inCount = 0;
  private outCount = 0;
  private typeCounts = { email: 0, ssn: 0, card: 0, total: 0 };

  get stats(): RedactorStats {
    return {
      peakPending: this.peakPendingCount,
      charsIn: this.inCount,
      charsOut: this.outCount,
      counts: { ...this.typeCounts }
    };
  }

  get pendingLength(): number {
    return this.pending.length;
  }

  push(text: string): string {
    if (!text) return "";

    this.inCount += text.length;
    this.pending += text;
    if (this.pending.length > this.peakPendingCount) {
      this.peakPendingCount = this.pending.length;
    }

    // Compute holdStart at the tail
    let emailRun = 0;
    for (let i = this.pending.length - 1; i >= 0 && emailRun < 320; i--) {
      if (EMAIL_CHAR_RE.test(this.pending[i])) {
        emailRun++;
      } else {
        break;
      }
    }
    const emailHoldStart = emailRun > 0 ? this.pending.length - emailRun : this.pending.length;

    let digitRun = 0;
    for (let i = this.pending.length - 1; i >= 0 && digitRun < 40; i--) {
      if (DIGIT_CHAR_RE.test(this.pending[i])) {
        digitRun++;
      } else {
        break;
      }
    }
    const digitHoldStart = digitRun > 0 ? this.pending.length - digitRun : this.pending.length;

    const holdStart = Math.min(emailHoldStart, digitHoldStart);

    // Fast path: skip regexes when pending has no "@" and no digit
    if (!this.pending.includes("@") && !/\d/.test(this.pending)) {
      const cut = holdStart;
      const toEmit = this.pending.slice(0, cut);
      if (toEmit.length > 0) {
        this.ctx = toEmit.slice(-1);
      }
      this.pending = this.pending.slice(cut);
      this.outCount += toEmit.length;
      return toEmit;
    }

    // Find matches against ctx + pending
    const matches = findMatches(this.ctx + this.pending);
    const ctxLen = this.ctx.length;

    // Convert matches to pending coordinates.
    // Matches with start < ctxLen start inside ctx (which was already emitted);
    // ctx is only provided as left-context for lookbehind of matches starting in pending.
    const pendingMatches: PiiMatch[] = [];
    for (const m of matches) {
      if (m.start >= ctxLen) {
        pendingMatches.push({
          start: m.start - ctxLen,
          end: m.end - ctxLen,
          type: m.type,
          text: m.text
        });
      }
    }

    // Compute cut point:
    // Any match touching tail (end >= holdStart) or straddling cut is unresolved
    let cut = holdStart;
    for (const m of pendingMatches) {
      if (m.end >= holdStart) {
        cut = Math.min(cut, m.start);
      }
      if (m.start < cut && m.end > cut) {
        cut = Math.min(cut, m.start);
      }
    }

    // Resolved matches inside pending[0:cut]
    const resolved = pendingMatches.filter((m) => m.end <= cut);
    const sliceToEmit = this.pending.slice(0, cut);

    let emitted = "";
    let lastIdx = 0;
    for (const m of resolved) {
      emitted += sliceToEmit.slice(lastIdx, m.start) + "[REDACTED]";
      lastIdx = m.end;
      this.typeCounts[m.type]++;
      this.typeCounts.total++;
    }
    emitted += sliceToEmit.slice(lastIdx);

    if (sliceToEmit.length > 0) {
      this.ctx = sliceToEmit.slice(-1);
    }
    this.pending = this.pending.slice(cut);
    this.outCount += emitted.length;
    return emitted;
  }

  flush(): string {
    const matches = findMatches(this.ctx + this.pending);
    const ctxLen = this.ctx.length;

    const pendingMatches: PiiMatch[] = [];
    for (const m of matches) {
      if (m.start >= ctxLen) {
        pendingMatches.push({
          start: m.start - ctxLen,
          end: m.end - ctxLen,
          type: m.type,
          text: m.text
        });
      }
    }

    let emitted = "";
    let lastIdx = 0;
    for (const m of pendingMatches) {
      emitted += this.pending.slice(lastIdx, m.start) + "[REDACTED]";
      lastIdx = m.end;
      this.typeCounts[m.type]++;
      this.typeCounts.total++;
    }
    emitted += this.pending.slice(lastIdx);

    this.outCount += emitted.length;
    this.pending = "";
    this.ctx = "";
    return emitted;
  }
}
