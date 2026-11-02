export interface GatewayConfig {
  port: number;
  upstreamUrl: string;
  upstreamApiKey?: string;
  upstreamIdleTimeoutMs: number;
  maxEventBytes: number;
}

export function parseConfig(env: NodeJS.ProcessEnv = process.env): GatewayConfig {
  const errors: string[] = [];

  // PORT
  let port = 8080;
  if (env.PORT !== undefined && env.PORT !== "") {
    const parsedPort = Number(env.PORT);
    if (!Number.isInteger(parsedPort) || parsedPort < 1 || parsedPort > 65535) {
      errors.push(`Invalid PORT: must be an integer between 1 and 65535, received "${env.PORT}"`);
    } else {
      port = parsedPort;
    }
  }

  // UPSTREAM_URL
  const rawUpstreamUrl = env.UPSTREAM_URL;
  let upstreamUrl = "";
  if (!rawUpstreamUrl || rawUpstreamUrl.trim() === "") {
    errors.push("Missing required environment variable: UPSTREAM_URL");
  } else {
    try {
      const u = new URL(rawUpstreamUrl);
      if (u.protocol !== "http:" && u.protocol !== "https:") {
        errors.push(`Invalid UPSTREAM_URL protocol: expected http: or https:, received "${u.protocol}"`);
      } else {
        upstreamUrl = rawUpstreamUrl.trim();
      }
    } catch {
      errors.push(`Invalid UPSTREAM_URL: "${rawUpstreamUrl}" is not a valid URL`);
    }
  }

  // UPSTREAM_API_KEY
  const upstreamApiKey = env.UPSTREAM_API_KEY && env.UPSTREAM_API_KEY.trim() !== ""
    ? env.UPSTREAM_API_KEY.trim()
    : undefined;

  // UPSTREAM_IDLE_TIMEOUT_MS
  let upstreamIdleTimeoutMs = 30000;
  if (env.UPSTREAM_IDLE_TIMEOUT_MS !== undefined && env.UPSTREAM_IDLE_TIMEOUT_MS !== "") {
    const parsedTimeout = Number(env.UPSTREAM_IDLE_TIMEOUT_MS);
    if (!Number.isFinite(parsedTimeout) || parsedTimeout <= 0) {
      errors.push(
        `Invalid UPSTREAM_IDLE_TIMEOUT_MS: must be a positive number, received "${env.UPSTREAM_IDLE_TIMEOUT_MS}"`
      );
    } else {
      upstreamIdleTimeoutMs = parsedTimeout;
    }
  }

  // MAX_EVENT_BYTES
  let maxEventBytes = 1048576;
  if (env.MAX_EVENT_BYTES !== undefined && env.MAX_EVENT_BYTES !== "") {
    const parsedMax = Number(env.MAX_EVENT_BYTES);
    if (!Number.isFinite(parsedMax) || parsedMax <= 0) {
      errors.push(`Invalid MAX_EVENT_BYTES: must be a positive number, received "${env.MAX_EVENT_BYTES}"`);
    } else {
      maxEventBytes = parsedMax;
    }
  }

  if (errors.length > 0) {
    const msg = `Configuration Error:\n` + errors.map((e) => `  - ${e}`).join("\n");
    throw new Error(msg);
  }

  return {
    port,
    upstreamUrl,
    upstreamApiKey,
    upstreamIdleTimeoutMs,
    maxEventBytes
  };
}

export function loadConfigOrExit(env: NodeJS.ProcessEnv = process.env): GatewayConfig {
  try {
    return parseConfig(env);
  } catch (err: any) {
    process.stderr.write(err.message + "\n");
    process.exit(1);
  }
}
