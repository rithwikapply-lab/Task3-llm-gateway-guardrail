import { spawn, exec } from "node:child_process";
import http from "node:http";
import os from "node:os";

interface GcStats {
  heapUsed: number;
  heapTotal: number;
  external: number;
  arrayBuffers: number;
  rss: number;
}

interface RunRecord {
  runIndex: number;
  label: string;
  wireBytes: number;
  baselineRssMB: number;
  peakRssMB: number;
  deltaRssMB: number;
  postGcRssMB: number;
  postGcHeapUsedMB: number;
  throughputMBps: number;
  ttftMs: number;
  totalMs: number;
}

function getRss(pid: number): Promise<number> {
  return new Promise((resolve) => {
    exec(`ps -o rss= -p ${pid}`, (err, stdout) => {
      if (err) return resolve(0);
      const kb = parseInt(stdout.trim(), 10);
      resolve(Number.isFinite(kb) ? kb * 1024 : 0);
    });
  });
}

function triggerGc(port: number): Promise<GcStats | null> {
  return new Promise((resolve) => {
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/__gc",
        method: "POST"
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(body));
          } catch {
            resolve(null);
          }
        });
      }
    );
    req.on("error", () => resolve(null));
    req.end();
  });
}

function runClientRequest(port: number, payload: string): Promise<{ bytes: number; ttftMs: number; totalMs: number }> {
  const startTime = performance.now();
  let firstChunkTime: number | null = null;
  let totalBytes = 0;

  return new Promise((resolve, reject) => {
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
          resolve({ bytes: totalBytes, ttftMs, totalMs });
        });
      }
    );

    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function waitForServer(proc: any, readyPattern: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Server start timeout")), 10000);
    proc.stdout.on("data", (chunk: Buffer) => {
      const text = chunk.toString();
      if (text.includes(readyPattern)) {
        clearTimeout(timeout);
        const match = text.match(/port (\\d+)/);
        resolve(match ? parseInt(match[1], 10) : 0);
      }
    });
    proc.on("error", (err: any) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

async function main() {
  const cpus = os.cpus();
  const cpuModel = cpus.length > 0 ? cpus[0].model : "Unknown CPU";
  process.stdout.write("=================================================================================\n");
  process.stdout.write("LLM Gateway Guardrail - Separate-Process Memory Honesty & Multi-Run Benchmark\n");
  process.stdout.write(`Node: ${process.version} | OS: ${os.type()} ${os.release()} (${os.arch()})\n`);
  process.stdout.write(`CPU: ${cpuModel} (${cpus.length} cores) | RAM: ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(1)} GB\n`);
  process.stdout.write("Sampling Method: ps -o rss= -p <gatewayPid> every 100 ms | GC: /__gc debug endpoint\n");
  process.stdout.write("=================================================================================\n\n");

  const mockPort = 4099;
  const gatewayPort = 8099;

  // 1. Spawn Mock Provider as separate child process
  process.stdout.write("Spawning mock provider process (node dist/mock/provider.js)...\n");
  const mockProc = spawn("node", ["dist/mock/provider.js"], {
    env: { ...process.env, PORT: String(mockPort) },
    stdio: ["ignore", "pipe", "inherit"]
  });
  await waitForServer(mockProc, "Mock provider listening");

  // 2. Spawn ONE Gateway process for the entire benchmark sequence
  process.stdout.write("Spawning ONE gateway process with BENCH_GC=1 (node --expose-gc dist/src/index.js)...\n");
  const gatewayProc = spawn("node", ["--expose-gc", "dist/src/index.js"], {
    env: {
      ...process.env,
      BENCH_GC: "1",
      PORT: String(gatewayPort),
      UPSTREAM_URL: `http://127.0.0.1:${mockPort}/v1/chat/completions`,
      UPSTREAM_API_KEY: "bench-key",
      UPSTREAM_IDLE_TIMEOUT_MS: "60000"
    },
    stdio: ["ignore", "pipe", "inherit"]
  });
  await waitForServer(gatewayProc, "LLM Gateway Guardrail listening");
  const gatewayPid = gatewayProc.pid!;
  process.stdout.write(`Gateway PID: ${gatewayPid}\n\n`);

  const piiSentence =
    "Customer alice@example.com (SSN: 987-65-4321) charged card 4111-1111-1111-1111 on 2024-06-15. ";

  // Sequence per spec: 50 MB x4, then 200 MB x2, then 1.3 MB
  const sequence = [
    { label: "50 MB (#1)", repeat: 27000 },
    { label: "50 MB (#2)", repeat: 27000 },
    { label: "50 MB (#3)", repeat: 27000 },
    { label: "50 MB (#4)", repeat: 27000 },
    { label: "200 MB (#1)", repeat: 108000 },
    { label: "200 MB (#2)", repeat: 108000 },
    { label: "1.3 MB (#1)", repeat: 700 }
  ];

  // Initial GC before benchmark sequence starts
  const initGc = await triggerGc(gatewayPort);
  const initRss = await getRss(gatewayPid);
  process.stdout.write(`Initial Baseline (before any runs): RSS=${(initRss / 1024 / 1024).toFixed(2)} MB | HeapUsed=${((initGc?.heapUsed ?? 0) / 1024 / 1024).toFixed(2)} MB\n\n`);

  const records: RunRecord[] = [];

  try {
    for (let i = 0; i < sequence.length; i++) {
      const cfg = sequence[i];
      const runNum = i + 1;
      process.stdout.write(`[Run ${runNum}/7] ${cfg.label} stream (scripted 4-char tokens with PII)...\n`);

      const requestBody = JSON.stringify({
        model: "benchmark-model",
        messages: [{ role: "user", content: "stream benchmark" }],
        stream: true,
        mock: {
          text: piiSentence,
          repeat: cfg.repeat,
          chunkSizes: [4]
        }
      });

      const baselineRssBytes = await getRss(gatewayPid);
      let peakRssBytes = baselineRssBytes;

      const sampler = setInterval(async () => {
        const curRss = await getRss(gatewayPid);
        if (curRss > peakRssBytes) peakRssBytes = curRss;
      }, 100);

      const gwRes = await runClientRequest(gatewayPort, requestBody);
      clearInterval(sampler);

      const endRssBytes = await getRss(gatewayPid);
      if (endRssBytes > peakRssBytes) peakRssBytes = endRssBytes;

      // Call /__gc and get exact post-GC stats
      const postGc = await triggerGc(gatewayPort);
      const postGcRss = await getRss(gatewayPid);

      const baselineRssMB = baselineRssBytes / 1024 / 1024;
      const peakRssMB = peakRssBytes / 1024 / 1024;
      const deltaRssMB = (peakRssBytes - baselineRssBytes) / 1024 / 1024;
      const postGcRssMB = postGcRss / 1024 / 1024;
      const postGcHeapUsedMB = (postGc?.heapUsed ?? 0) / 1024 / 1024;
      const throughputMBps = (gwRes.bytes / 1024 / 1024) / (gwRes.totalMs / 1000);

      records.push({
        runIndex: runNum,
        label: cfg.label,
        wireBytes: gwRes.bytes,
        baselineRssMB,
        peakRssMB,
        deltaRssMB,
        postGcRssMB,
        postGcHeapUsedMB,
        throughputMBps,
        ttftMs: gwRes.ttftMs,
        totalMs: gwRes.totalMs
      });

      process.stdout.write(`  Result: ${(gwRes.bytes / 1024 / 1024).toFixed(1)} MB | TTFT=${gwRes.ttftMs.toFixed(2)} ms | Total=${gwRes.totalMs.toFixed(0)} ms | Throughput=${throughputMBps.toFixed(2)} MB/s\n`);
      process.stdout.write(`  Memory: Baseline RSS=${baselineRssMB.toFixed(2)} MB | Peak RSS=${peakRssMB.toFixed(2)} MB | Delta=${deltaRssMB.toFixed(2)} MB | Post-GC RSS=${postGcRssMB.toFixed(2)} MB | Post-GC HeapUsed=${postGcHeapUsedMB.toFixed(2)} MB\n\n`);
    }

    // Direct comparison baseline run
    process.stdout.write("---------------------------------------------------------------------------------\n");
    process.stdout.write("Direct Baseline vs Gateway Comparison (50 MB stream)...\n");
    const direct50Req = JSON.stringify({
      model: "benchmark-model",
      messages: [{ role: "user", content: "stream benchmark" }],
      stream: true,
      mock: { text: piiSentence, repeat: 27000, chunkSizes: [4] }
    });
    const directRes = await runClientRequest(mockPort, direct50Req);
    const directThroughput = (directRes.bytes / 1024 / 1024) / (directRes.totalMs / 1000);
    process.stdout.write(`  Direct (Mock Provider): TTFT=${directRes.ttftMs.toFixed(2)} ms | Total=${directRes.totalMs.toFixed(0)} ms | Throughput=${directThroughput.toFixed(2)} MB/s\n\n`);

    // Output table
    process.stdout.write("========================================================================================================\n");
    process.stdout.write("POST-GC HEAP & RSS RETENTION OVER SEQUENCE (50MB x4 -> 200MB x2 -> 1.3MB against ONE process)\n");
    process.stdout.write("========================================================================================================\n");
    process.stdout.write("Run | Stream Size | Wire Bytes | Baseline RSS | Peak RSS   | Peak Delta | Post-GC RSS | Post-GC HeapUsed | Throughput\n");
    process.stdout.write("----+-------------+------------+--------------+------------+------------+-------------+------------------+------------\n");
    for (const r of records) {
      const idxCol = String(r.runIndex).padStart(2);
      const lblCol = r.label.padEnd(11);
      const wireCol = `${(r.wireBytes / 1024 / 1024).toFixed(1)} MB`.padStart(10);
      const baseCol = `${r.baselineRssMB.toFixed(2)} MB`.padStart(12);
      const peakCol = `${r.peakRssMB.toFixed(2)} MB`.padStart(10);
      const deltaCol = `${r.deltaRssMB.toFixed(2)} MB`.padStart(10);
      const postRssCol = `${r.postGcRssMB.toFixed(2)} MB`.padStart(11);
      const heapCol = `${r.postGcHeapUsedMB.toFixed(2)} MB`.padStart(16);
      const tpCol = `${r.throughputMBps.toFixed(2)} MB/s`.padStart(10);
      process.stdout.write(`${idxCol}  | ${lblCol} | ${wireCol} | ${baseCol} | ${peakCol} | ${deltaCol} | ${postRssCol} | ${heapCol} | ${tpCol}\n`);
    }
    process.stdout.write("========================================================================================================\n");
  } finally {
    process.stdout.write("\nCleaning up child processes...\n");
    gatewayProc.kill("SIGTERM");
    mockProc.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 500));
  }
}

main().catch((err) => {
  process.stderr.write(String(err) + "\n");
  process.exit(1);
});
