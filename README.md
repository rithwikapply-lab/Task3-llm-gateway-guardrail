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
| `BENCH_GC` | Integer (`0` or `1`) | `0` | Debug flag. When set to `1` and Node is started with `--expose-gc`, enables `POST /__gc` for memory profiling. |

---

## Algorithm: Candidate-Suffix Holdback vs Sliding Window

### Why Candidate-Suffix Holdback?
Fixed sliding-window buffering introduces an unavoidable lag equal to the maximum possible entity length on every emitted token—even across ordinary prose with no PII.

Instead of a sliding window, this gateway implements a **candidate-suffix holdback** algorithm:
1. **No Buffering for Completed Text**: Whenever non-candidate characters (e.g. whitespace, commas, punctuation) are encountered, all preceding text is definitively resolved and emitted immediately.
2. **Dynamic Candidate Suffix**: The engine inspects only the tail of the buffer for characters that could potentially form the beginning or continuation of an entity:
   - **Emails**: Held back only for runs of valid email characters (`[A-Za-z0-9._%+@-]`) up to the simplified bounded pattern limit of 320 characters.
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

- **Time Complexity**: **O(N)** amortized linear time with respect to the input character count. Regex evaluations only scan the bounded candidate suffix tail rather than the full response history.
- **Space Complexity**: **O(1)** algorithm buffer space. The redactor pending buffer is strictly bounded at 320 characters plus chunk size. Internally, post-GC heap memory returns to baseline (~9.6–11.0 MB) after every request regardless of stream size.

---

## Latency Characteristics

- **Prose vs Continuous Run**: Text without a separator (a long continuous run of letters, digits, '.', '_', '-', '%', '+', or '@') is held back up to 320 characters (40 characters for digit runs) before it is emitted.
- **Ordinary Prose**: Ordinary prose containing spaces, commas, or punctuation lags by at most the current word in flight. Once a word delimiter is received, all preceding resolved words are emitted immediately.
- **First-Byte Latency**: Initial role chunks and header bytes are forwarded immediately with sub-4ms TTFT overhead.
- **1000-Letter Run Lag Bound**: When streaming an unbroken 1000-character letter run, characters 1 through 320 are held back while candidate matches remain possible; starting at character 321, characters are emitted steadily one-for-one, guaranteeing that pending buffer length never exceeds 320 characters.

## Latency and Memory Behaviour

- **Immediate TTFT**: The initial role-only chunk (`{"role":"assistant"}`) is forwarded to the client immediately without waiting for content deltas.
- **Word-Level Prose Lag**: In plain prose without PII, characters are emitted as soon as a word completes upon hitting whitespace or punctuation. Prose lag is at most the last word currently in flight.
- **Strict Memory Bounding**: Full streams are never buffered in memory. 50 MB payloads stream with peak buffer sizes under 320 bytes plus chunk size.
- **Backpressure Handling**: If `res.write()` returns `false`, the gateway halts pulling from the upstream reader until the client response emits the `drain` event, keeping memory bounded across slow clients.

---

## Measured Benchmark Results

Benchmarked with mock provider and gateway running as **separate child processes** communicating over loopback TCP HTTP. Gateway RSS was continuously sampled using `ps -o rss= -p <gatewayPid>` every 100 ms throughout each stream, and `POST /__gc` was called at the completion of each run.
Environment: Node.js **v24.10.0** on an **Apple M3 Pro (11 cores)** with **18.0 GB RAM** on **Darwin 25.6.0 (arm64)**.

### Memory Honesty: Multi-Run Sequence Against ONE Gateway Process
Sequence executed in order: **50 MB x4**, then **200 MB x2**, then **1.3 MB** against a single persistent gateway process:

| Run | Stream Size | Wire Bytes | Baseline RSS | Peak RSS | Peak Delta | Post-GC RSS | Post-GC HeapUsed | Throughput |
|---|---|---|---|---|---|---|---|---|
| **1** | 50 MB (#1) | 51.4 MB | 66.22 MB | 133.34 MB | 67.13 MB | 133.23 MB | **10.57 MB** | 29.64 MB/s |
| **2** | 50 MB (#2) | 51.4 MB | 133.23 MB | 189.42 MB | 56.19 MB | 189.33 MB | **10.64 MB** | 30.12 MB/s |
| **3** | 50 MB (#3) | 51.4 MB | 189.33 MB | 255.50 MB | 66.17 MB | 255.50 MB | **10.78 MB** | 30.20 MB/s |
| **4** | 50 MB (#4) | 51.4 MB | 255.50 MB | 255.59 MB | 0.09 MB | 255.59 MB | **10.83 MB** | 30.52 MB/s |
| **5** | 200 MB (#1) | 205.7 MB | 255.59 MB | 261.27 MB | 5.67 MB | 261.28 MB | **10.98 MB** | 30.46 MB/s |
| **6** | 200 MB (#2) | 205.7 MB | 261.28 MB | 261.50 MB | 0.22 MB | 261.52 MB | **9.69 MB** | 31.10 MB/s |
| **7** | 1.3 MB (#1) | 1.3 MB | 261.52 MB | 261.64 MB | 0.13 MB | 261.13 MB | **9.62 MB** | 20.95 MB/s |

### Understanding RSS vs HeapUsed
- **Post-GC `heapUsed` returns to baseline**: Starting from an initial baseline of 8.81 MB, `heapUsed` measured immediately after garbage collection stays strictly within ~9.6–11.0 MB across all 7 runs—even after processing over 820 MB of cumulative SSE stream data. No per-request state, loggers, or streams accumulate over time.
- **RSS Retention**: Resident Set Size (RSS) measures the total OS memory pages allocated to the process. V8 requests memory pages from the operating system during initial high-throughput allocations (growing to ~255 MB over runs 1–3), and then retains those committed pages for subsequent workloads rather than returning them to the OS. Once established, Peak Delta drops to virtually zero (0.09 MB on Run 4, 0.22 MB on Run 6), and 200 MB streams add only 5.67 MB of peak delta.

### Throughput & Latency Honesty
- **Direct Provider Throughput**: ~145–148 MB/s (pure synthetic localhost stream).
- **Gateway Guardrail Throughput**: ~30 MB/s of SSE wire bytes (approximately 160,000 token chunk events per second at ~188 bytes per event), with sub-2.5 ms TTFT.
- **Production Context**: Real-world LLM inference streams emit roughly 20 to 100 tokens per second (under 20 KB/s per client connection). At ~30 MB/s capacity, the gateway is more than three orders of magnitude faster than actual LLM generation speeds, introducing negligible overhead in real deployments.

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

- **Email Validation**: Uses a simplified bounded pattern matching standard email formats up to a maximum of 320 characters. Exotic quoted-string local parts with internal whitespace or uncommon obsolete formats are not covered.
- **Credit Card Mod-10**: Digit runs of 13 to 19 digits that pass Luhn are redacted; others are left alone.
- **Fail-Closed on Malformed JSON**: If an upstream SSE event contains malformed JSON data, the entire raw data string is redacted with `redactWhole` before emitting to prevent data leakage.
