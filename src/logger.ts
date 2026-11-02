export interface RedactionCounts {
  email: number;
  ssn: number;
  card: number;
  total: number;
}

export interface RequestLogRecord {
  reqId: string;
  status: number;
  ttftMs: number | null;
  totalMs: number;
  bytesIn: number;
  bytesOut: number;
  redactions: RedactionCounts;
  peakPending: number;
}

export type LogSink = (line: string) => void;

const defaultSink: LogSink = (line: string) => {
  process.stderr.write(line + "\n");
};

export class Logger {
  private sink: LogSink;

  constructor(sink: LogSink = defaultSink) {
    this.sink = sink;
  }

  logRequest(record: RequestLogRecord): void {
    // Sanitize and serialize exactly one JSON line
    const output = {
      reqId: record.reqId,
      status: record.status,
      ttftMs: record.ttftMs,
      totalMs: record.totalMs,
      bytesIn: record.bytesIn,
      bytesOut: record.bytesOut,
      redactions: {
        email: record.redactions.email,
        ssn: record.redactions.ssn,
        card: record.redactions.card,
        total: record.redactions.total
      },
      peakPending: record.peakPending
    };
    this.sink(JSON.stringify(output));
  }
}

export const defaultLogger = new Logger();
