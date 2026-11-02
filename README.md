# LLM Gateway Guardrail

A lightweight, zero-dependency streaming security gateway and reverse proxy for LLM chat completions (`POST /v1/chat/completions`). Built in TypeScript (strict ESM) on Node.js 18+ with Express 4, global `fetch`, and standard Server-Sent Events (SSE). It intercepts streaming responses from upstream LLM providers and redacts sensitive PII (emails, US Social Security Numbers, and Luhn-validated credit card numbers) in real time with minimal latency and strictly bounded memory.

---

## Run Steps

### Prerequisites
- Node.js 18.0.0 or higher (tested on Node v24.10.0)
- npm 9.0.0 or higher

### Installation
```bash
npm install
```

### Build
Compile TypeScript to `dist/`:
```bash
npm run build
```

### Test
Execute the automated test suite (SSE parser, redactor invariants, seeded fuzzing, gateway integration, and latency/memory tests):
```bash
npm test
```

### Start Mock Provider
Run the standalone mock LLM provider on port 4001:
```bash
npm run start:mock
```

### Start Gateway
Start the gateway reverse proxy (defaults to port 8080):
```bash
export UPSTREAM_URL="http://127.0.0.1:4001/v1/chat/completions"
export UPSTREAM_API_KEY="your-upstream-secret-key"
npm start
```

### Run Benchmark
Run the direct vs gateway comparative performance and memory benchmark:
```bash
npm run bench
```

---

## Configuration

The gateway is configured exclusively via environment variables:

| Variable | Type | Default | Description |
|---|---|---|---|
| `PORT` | Integer (1–65535) | `8080` | Port on which the gateway listens. |
| `UPSTREAM_URL` | URL (http/https) | *Required* | Complete upstream chat completions endpoint URL. |
| `UPSTREAM_API_KEY` | String | *Optional* | Upstream API key injected as `Authorization: Bearer <key>`. The client\'s Authorization header is stripped. |
| `UPSTREAM_IDLE_TIMEOUT_MS` | Number (> 0) | `30000` | Idle timeout in milliseconds while waiting for upstream chunks before emitting an error SSE event. |
| `MAX_EVENT_BYTES` | Number (> 0) | `1048576` (1 MB) | Maximum byte length of an individual SSE event buffer before rejecting as oversized. |

---

## Algorithm: Candidate-Suffix Holdback vs Sliding Window

### Why Candidate-Suffix Holdback?
Fixed sliding-window buffering introduces an unavoidable lag equal to the maximum possible entity length on every emitted token—even across ordinary prose with no PII.

Instead of a sliding window, this gateway implements a **candidate-suffix holdback** algorithm:
1. **No Buffering for Completed Text**: Whenever non-candidate characters (e.g. whitespace, commas, punctuation) are encountered, all preceding text is definitively resolved and emitted immediately.
2. **Dynamic Candidate Suffix**: The engine inspects only the tail of the buffer for characters that could potentially form the beginning or continuation of an entity:
   - **Emails**: Held back only for runs of valid email characters (`[A-Za-z0-9._%+@-]`) up to the RFC limit of 320 characters.
   - **Credit Cards & SSNs**: Held back only for runs of digits and digit-separators (`[0-9 -]`) up to 40 characters (credit cards are at most 19 digits; SSNs are 11 characters).
3. **Left-Context Character (`ctx`)**: Lookbehind assertions (such as `(?<![\d-])` and `(?<![A-Za-z0-9._%+-])`) require 1 character of preceding context. The redactor stores the single character immediately preceding the buffer as `ctx`. Any match evaluated over `ctx + pending` must start at or after index `ctx.length`, guaranteeing already-emitted characters are never matched while still satisfying the negative lookbehind.
4. **Cut Computation**: The cut point is determined by taking the earliest start index of any candidate run touching the tail, or any match straddling the tail. All fully resolved text before the cut point is redacted and emitted immediately.

---

## The Three Invariants

The streaming redactor strictly enforces three formal invariants:

1. **Chunk-Invariance**:
   ```
   concat(push(chunk_1), push(chunk_2), ..., push(chunk_N)) + flush() === redactWhole(fullText)
   ```
   Streaming the text through arbitrary chunk boundaries produces the exact same output as evaluating the complete text synchronously with reference regex matching.

2. **Bounded Buffer Space**:
   ```
   pending.length <= 320 + maxChunkSize
   ```
   The internal pending buffer is strictly bounded. Runs of characters exceeding the maximum candidate length (320 for emails, 40 for digits) are released, preventing memory accumulation even under adversarial inputs.

3. **PII Confidentiality**:
   No original PII substring appears in any emitted output chunk or stream under any chunking configuration.

---

## Complexity

- **Time Complexity**: **O(N)** amortized linear time with respect to the input character count. Regex evaluations only scan the bounded candidate tail and active chunk, rather than the entire history.
- **Space Complexity**: **O(1)** auxiliary memory. The buffer size depends strictly on the maximum entity length (320 bytes) and network chunk size, never on the total response length.

---

## Latency and Memory Behaviour

- **Immediate TTFT**: The initial role-only chunk (`{"role":"assistant"}`) is forwarded to the client immediately without waiting for content deltas.
- **Word-Level Prose Lag**: In plain prose without PII, characters are emitted as soon as a word completes upon hitting whitespace or punctuation. Prose lag is at most the last word currently in flight.
- **Strict Memory Bounding**: Full streams are never buffered in memory. 50 MB payloads stream with peak buffer sizes under 320 bytes plus chunk size.
- **Backpressure Handling**: If `res.write()` returns `false`, the gateway halts pulling from the upstream reader until the client response emits the `drain` event, keeping memory bounded across slow clients.

---

## Measured Benchmark Results

Measured from a real run on port 0 with Node.js **v24.10.0** on an **Apple M3 Pro (11 cores)** with **18.0 GB RAM**, streaming a 1.3 MB scripted response with 4-character tokens and PII over 20 runs:

| Metric | Direct (Mock Provider) | Gateway (Guardrail Proxy) | Overhead |
|---|---|---|---|
| **Median TTFT** | 447.07 ms | 455.02 ms | **7.95 ms** |
| **Median Throughput** | 98.98 MB/s | 17.54 MB/s | — |
| **Gateway Peak RSS** | — | 1392.59 MB | — |
| **Gateway Peak Heap** | — | 865.97 MB | — |

---

## Error Handling

| Scenario | HTTP Status / Event | Response Payload | Behavior |
|---|---|---|---|
| Invalid JSON body | `400 Bad Request` | `{"error":{"message":"...","type":"invalid_request_error"}}` | Request rejected before upstream. |
| Missing `messages` / `model` | `400 Bad Request` | `{"error":{"message":"...","type":"invalid_request_error"}}` | Request rejected before upstream. |
| Payload > 1 MB | `413 Payload Too Large` | `{"error":{"message":"...","type":"payload_too_large"}}` | Enforced by JSON body parser. |
| Unknown path | `404 Not Found` | `{"error":{"message":"Not Found","type":"not_found"}}` | Standard 404 response. |
| Invalid HTTP method | `405 Method Not Allowed` | `{"error":{"message":"...","type":"method_not_allowed"}}` | Explicit method restriction. |
| Connection Refused | `502 Bad Gateway` | `{"error":{"message":"...","type":"bad_gateway"}}` | Upstream unreachable. |
| Upstream non-2xx | `500` / Upstream code | `{"error":{"message":"Upstream service error","type":"upstream_error"}}` | Sanitized upstream error. |
| Upstream Socket Drop | `200 text/event-stream` | Flushed held text + `event: error` (`upstream_drop`) | Held text flushed before error event. |
| Upstream Idle Timeout | `200 text/event-stream` | Flushed held text + `event: error` (`timeout_error`) | Triggered after configured timeout ms. |

---

## What is NOT Redacted

Per the specification, the gateway strictly limits redaction to content payloads:
- **Non-Content Fields**: Object IDs (`id`), model names (`model`), roles (`role`), timestamps (`created`), and system fingerprints are forwarded unmodified.
- **Request Bodies**: The gateway inspects and sanitizes only outgoing response streams; incoming user prompts are forwarded directly to the upstream provider.
- **Other PII Types**: Only emails, US Social Security Numbers, and credit card numbers are redacted. Names, street addresses, phone numbers, and passport numbers are not redacted.
- **Non-ASCII Digits**: Only standard ASCII digits (`0-9`) are evaluated. Unicode fullwidth or non-Latin numerals are not matched.

---

## Limitations

- **Email Validation**: Adheres to RFC 5322 address syntax for common email formats up to 320 characters. Exotic quoted-string local parts with internal whitespace are not covered.
- **Credit Card Mod-10**: Validates numbers with 13 to 19 digits using the Luhn algorithm. Valid card numbers with incorrect test digits that happen to pass Mod 10 checksum will be redacted.
- **Fail-Closed on Malformed JSON**: If an upstream SSE event contains malformed JSON data, the entire raw data string is redacted with `redactWhole` before emitting to prevent data leakage.
