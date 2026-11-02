import http from "node:http";
import os from "node:os";
import { createMockProvider } from "../mock/provider.js";
import { createApp } from "../src/app.js";
import { Logger } from "../src/logger.js";
import type { GatewayConfig } from "../src/config.js";

interface RunResult {
  ttftMs: number;
  totalMs: number;
  bytes: number;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 !== 0) {
    return sorted[mid];
  }
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

async function runClientRequest(port: number, payload: string): Promise<RunResult> {
  const startTime = performance.now();
  let firstChunkTime: number | null = null;
  let totalBytes = 0;

  return new Promise<RunResult>((resolve, reject) => {
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload)
        }
      },
      (res) => {
        res.on("data", (chunk) => {
          totalBytes += chunk.length;
          if (firstChunkTime === null) {
            firstChunkTime = performance.now();
          }
        });

        res.on("end", () => {
          const endTime = performance.now();
          const ttftMs = (firstChunkTime ?? endTime) - startTime;
          const totalMs = endTime - startTime;
          resolve({ ttftMs, totalMs, bytes: totalBytes });
        });
      }
    );

    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  const cpus = os.cpus();
  const cpuModel = cpus.length > 0 ? cpus[0].model : "Unknown CPU";
  process.stdout.write("=========================================================\n");
  process.stdout.write("LLM Gateway Guardrail - Performance & Memory Benchmark\n");
  process.stdout.write(`Node: ${process.version} | OS: ${os.type()} ${os.release()} (${os.arch()})\n`);
  process.stdout.write(`CPU: ${cpuModel} (${cpus.length} cores) | RAM: ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(1)} GB\n`);
  process.stdout.write("=========================================================\n\n");

  // 1. Build test payload (~2 MB response with 4-char chunks and PII)
  const piiSentence =
    "Customer alice@example.com (SSN: 987-65-4321) charged card 4111-1111-1111-1111 on 2024-06-15. ";
  const targetRepeat = 15000; // ~1.5 MB text response
  const expectedBytes = piiSentence.length * targetRepeat;

  const requestBody = JSON.stringify({
    model: "benchmark-model",
    messages: [{ role: "user", content: "stream benchmark" }],
    stream: true,
    mock: {
      text: piiSentence,
      repeat: targetRepeat,
      chunkSizes: [4] // 4-char tokens per spec
    }
  });

  // 2. Start mock provider
  const mockProvider = createMockProvider(0);
  const mockPort = await mockProvider.start();

  // 3. Start gateway pointing to mock
  const nullLogger = new Logger(() => {});
  const config: GatewayConfig = {
    port: 0,
    upstreamUrl: `http://127.0.0.1:${mockPort}/v1/chat/completions`,
    upstreamApiKey: "bench-upstream-key",
    upstreamIdleTimeoutMs: 30000,
    maxEventBytes: 1048576
  };
  const app = createApp(config, nullLogger);
  const gatewayServer = http.createServer(app);
  const gatewaySockets = new Set<any>();
  gatewayServer.on("connection", (s) => {
    gatewaySockets.add(s);
    s.on("close", () => gatewaySockets.delete(s));
  });

  const gatewayPort = await new Promise<number>((resolve) => {
    gatewayServer.listen(0, () => {
      resolve((gatewayServer.address() as any).port);
    });
  });

  const numRuns = 20;

  try {
    process.stdout.write(`Running ${numRuns} direct iterations against mock provider (${(expectedBytes / 1024 / 1024).toFixed(1)} MB stream)...\n`);
    const directResults: RunResult[] = [];
    for (let i = 1; i <= numRuns; i++) {
      const res = await runClientRequest(mockPort, requestBody);
      directResults.push(res);
      process.stdout.write(`  [Direct ${i.toString().padStart(2, "0")}/${numRuns}] TTFT: ${res.ttftMs.toFixed(2)} ms | Total: ${res.totalMs.toFixed(2)} ms\n`);
    }

    process.stdout.write(`\nRunning ${numRuns} iterations through gateway with real-time PII redaction...\n`);
    const gatewayResults: RunResult[] = [];
    let peakRss = 0;
    let peakHeap = 0;

    for (let i = 1; i <= numRuns; i++) {
      const memBefore = process.memoryUsage();
      const res = await runClientRequest(gatewayPort, requestBody);
      const memAfter = process.memoryUsage();

      const runPeakRss = Math.max(memBefore.rss, memAfter.rss);
      const runPeakHeap = Math.max(memBefore.heapUsed, memAfter.heapUsed);
      if (runPeakRss > peakRss) peakRss = runPeakRss;
      if (runPeakHeap > peakHeap) peakHeap = runPeakHeap;

      gatewayResults.push(res);
      process.stdout.write(`  [Gateway ${i.toString().padStart(2, "0")}/${numRuns}] TTFT: ${res.ttftMs.toFixed(2)} ms | Total: ${res.totalMs.toFixed(2)} ms\n`);
    }

    // Calculations
    const directTtfts = directResults.map((r) => r.ttftMs);
    const gatewayTtfts = gatewayResults.map((r) => r.ttftMs);
    const medianDirectTtft = median(directTtfts);
    const medianGatewayTtft = median(gatewayTtfts);

    const directThroughputMBps = directResults.map((r) => (r.bytes / 1024 / 1024) / (r.totalMs / 1000));
    const gatewayThroughputMBps = gatewayResults.map((r) => (r.bytes / 1024 / 1024) / (r.totalMs / 1000));
    const medianDirectThroughput = median(directThroughputMBps);
    const medianGatewayThroughput = median(gatewayThroughputMBps);

    process.stdout.write("\n==================== BENCHMARK RESULTS ====================\n");
    process.stdout.write(`Metric                    Direct (Mock)        Gateway (Guardrail)\n`);
    process.stdout.write(`------------------------------------------------------------\n`);
    process.stdout.write(`Median TTFT:              ${medianDirectTtft.toFixed(2).padStart(8)} ms        ${medianGatewayTtft.toFixed(2).padStart(8)} ms\n`);
    process.stdout.write(`TTFT Overhead:            --------             ${(medianGatewayTtft - medianDirectTtft).toFixed(2).padStart(8)} ms\n`);
    process.stdout.write(`Median Throughput:        ${medianDirectThroughput.toFixed(2).padStart(8)} MB/s      ${medianGatewayThroughput.toFixed(2).padStart(8)} MB/s\n`);
    process.stdout.write(`Gateway Peak RSS:         --------             ${(peakRss / 1024 / 1024).toFixed(2).padStart(8)} MB\n`);
    process.stdout.write(`Gateway Peak Heap:        --------             ${(peakHeap / 1024 / 1024).toFixed(2).padStart(8)} MB\n`);
    process.stdout.write("============================================================\n");
  } finally {
    for (const s of gatewaySockets) s.destroy();
    gatewaySockets.clear();
    await new Promise<void>((r) => gatewayServer.close(() => r()));
    await mockProvider.close();
  }
}

main().catch((err) => {
  process.stderr.write(String(err) + "\n");
  process.exit(1);
});
