import { spawn, exec } from "node:child_process";
import http from "node:http";
import os from "node:os";

interface StreamResult {
  streamSizeLabel: string;
  wireBytes: number;
  ttftMs: number;
  totalMs: number;
  throughputMBps: number;
  baselineRssMB: number;
  peakRssMB: number;
  deltaRssMB: number;
  afterRssMB: number;
}

interface DirectResult {
  streamSizeLabel: string;
  ttftMs: number;
  totalMs: number;
  throughputMBps: number;
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

function triggerGc(port: number): Promise<void> {
  return new Promise((resolve) => {
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/__gc",
        method: "POST"
      },
      () => resolve()
    );
    req.on("error", () => resolve());
    req.end();
  });
}

function runClientRequest(port: number, payload: string, onProgress?: () => void): Promise<{ bytes: number; ttftMs: number; totalMs: number }> {
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
          if (onProgress) onProgress();
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
        const match = text.match(/port (\d+)/);
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
  process.stdout.write("=======================================================================\n");
  process.stdout.write("LLM Gateway Guardrail - Separate-Process Memory & Performance Benchmark\n");
  process.stdout.write(`Node: ${process.version} | OS: ${os.type()} ${os.release()} (${os.arch()})\n`);
  process.stdout.write(`CPU: ${cpuModel} (${cpus.length} cores) | RAM: ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(1)} GB\n`);
  process.stdout.write("Sampling Method: ps -o rss= -p <gatewayPid> every 100 ms\n");
  process.stdout.write("=======================================================================\n\n");

  const mockPort = 4099;
  const gatewayPort = 8099;

  // 1. Spawn Mock Provider as separate child process
  process.stdout.write("Spawning mock provider process (node dist/mock/provider.js)...\n");
  const mockProc = spawn("node", ["dist/mock/provider.js"], {
    env: { ...process.env, PORT: String(mockPort) },
    stdio: ["ignore", "pipe", "inherit"]
  });
  await waitForServer(mockProc, "Mock provider listening");

  // 2. Spawn Gateway Guardrail as separate child process
  process.stdout.write("Spawning gateway process (node --expose-gc dist/src/index.js)...\n");
  const gatewayProc = spawn("node", ["--expose-gc", "dist/src/index.js"], {
    env: {
      ...process.env,
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

  const testConfigs = [
    { label: "1.3 MB", repeat: 700 },
    { label: "50 MB", repeat: 27000 },
    { label: "200 MB", repeat: 108000 }
  ];

  const gatewayResults: StreamResult[] = [];
  const directResults: DirectResult[] = [];

  try {
    for (const cfg of testConfigs) {
      process.stdout.write(`-----------------------------------------------------------------------\n`);
      process.stdout.write(`Benchmarking ${cfg.label} stream (scripted 4-char tokens with PII)...\n`);

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

      // 1. Direct (Mock) benchmark
      process.stdout.write(`  Running direct request to mock provider...\n`);
      const direct = await runClientRequest(mockPort, requestBody);
      const directThroughput = (direct.bytes / 1024 / 1024) / (direct.totalMs / 1000);
      directResults.push({
        streamSizeLabel: cfg.label,
        ttftMs: direct.ttftMs,
        totalMs: direct.totalMs,
        throughputMBps: directThroughput
      });
      process.stdout.write(`    Direct: TTFT=${direct.ttftMs.toFixed(2)} ms | Total=${direct.totalMs.toFixed(0)} ms | Throughput=${directThroughput.toFixed(2)} MB/s\n`);

      // 2. Gateway benchmark with RSS sampling
      process.stdout.write(`  Running request through gateway with continuous RSS sampling...\n`);
      await triggerGc(gatewayPort);
      await new Promise((r) => setTimeout(r, 200));

      const baselineRssBytes = await getRss(gatewayPid);
      let peakRssBytes = baselineRssBytes;

      const sampler = setInterval(async () => {
        const curRss = await getRss(gatewayPid);
        if (curRss > peakRssBytes) peakRssBytes = curRss;
      }, 100);

      const gwRes = await runClientRequest(gatewayPort, requestBody);
      clearInterval(sampler);

      // Sample immediate post-stream RSS
      const endRssBytes = await getRss(gatewayPid);
      if (endRssBytes > peakRssBytes) peakRssBytes = endRssBytes;

      // Trigger GC and sample plateau / after RSS
      await triggerGc(gatewayPort);
      await new Promise((r) => setTimeout(r, 200));
      const afterRssBytes = await getRss(gatewayPid);

      const baselineRssMB = baselineRssBytes / 1024 / 1024;
      const peakRssMB = peakRssBytes / 1024 / 1024;
      const deltaRssMB = (peakRssBytes - baselineRssBytes) / 1024 / 1024;
      const afterRssMB = afterRssBytes / 1024 / 1024;
      const gwThroughput = (gwRes.bytes / 1024 / 1024) / (gwRes.totalMs / 1000);

      gatewayResults.push({
        streamSizeLabel: cfg.label,
        wireBytes: gwRes.bytes,
        ttftMs: gwRes.ttftMs,
        totalMs: gwRes.totalMs,
        throughputMBps: gwThroughput,
        baselineRssMB,
        peakRssMB,
        deltaRssMB,
        afterRssMB
      });

      process.stdout.write(`    Gateway: TTFT=${gwRes.ttftMs.toFixed(2)} ms | Total=${gwRes.totalMs.toFixed(0)} ms | Throughput=${gwThroughput.toFixed(2)} MB/s\n`);
      process.stdout.write(`    Memory: Baseline=${baselineRssMB.toFixed(2)} MB | Peak=${peakRssMB.toFixed(2)} MB | Delta=${deltaRssMB.toFixed(2)} MB | After=${afterRssMB.toFixed(2)} MB\n`);
    }

    // Print summary tables
    process.stdout.write(`\n=======================================================================\n`);
    process.stdout.write(`STREAM SIZE VS GATEWAY PEAK RSS DELTA (FLAT MEMORY VERIFICATION)\n`);
    process.stdout.write(`=======================================================================\n`);
    process.stdout.write(`Stream Size | Wire Bytes  | Baseline RSS | Peak RSS   | Peak Delta | Post-GC RSS\n`);
    process.stdout.write(`------------+-------------+--------------+------------+------------+------------\n`);
    for (const r of gatewayResults) {
      const sizeCol = r.streamSizeLabel.padEnd(11);
      const wireCol = `${(r.wireBytes / 1024 / 1024).toFixed(1)} MB`.padStart(11);
      const baseCol = `${r.baselineRssMB.toFixed(2)} MB`.padStart(12);
      const peakCol = `${r.peakRssMB.toFixed(2)} MB`.padStart(10);
      const deltaCol = `${r.deltaRssMB.toFixed(2)} MB`.padStart(10);
      const afterCol = `${r.afterRssMB.toFixed(2)} MB`.padStart(10);
      process.stdout.write(`${sizeCol} | ${wireCol} | ${baseCol} | ${peakCol} | ${deltaCol} | ${afterCol}\n`);
    }
    process.stdout.write(`=======================================================================\n\n`);

    process.stdout.write(`=======================================================================\n`);
    process.stdout.write(`LATENCY AND THROUGHPUT COMPARISON (DIRECT VS GATEWAY)\n`);
    process.stdout.write(`=======================================================================\n`);
    process.stdout.write(`Stream Size | Direct TTFT | Gateway TTFT | TTFT Overhead | Direct MB/s | Gateway MB/s\n`);
    process.stdout.write(`------------+-------------+--------------+---------------+-------------+-------------\n`);
    for (let i = 0; i < testConfigs.length; i++) {
      const d = directResults[i];
      const g = gatewayResults[i];
      const sizeCol = d.streamSizeLabel.padEnd(11);
      const dTtft = `${d.ttftMs.toFixed(2)} ms`.padStart(11);
      const gTtft = `${g.ttftMs.toFixed(2)} ms`.padStart(12);
      const ovh = `${(g.ttftMs - d.ttftMs).toFixed(2)} ms`.padStart(13);
      const dRate = `${d.throughputMBps.toFixed(2)} MB/s`.padStart(11);
      const gRate = `${g.throughputMBps.toFixed(2)} MB/s`.padStart(12);
      process.stdout.write(`${sizeCol} | ${dTtft} | ${gTtft} | ${ovh} | ${dRate} | ${gRate}\n`);
    }
    process.stdout.write(`=======================================================================\n`);
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
