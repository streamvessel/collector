/**
 * Turns one OpenSearch document into one NDJSON line in the shape the log
 * viewer reads: `timestamp` (RFC 3339, UTC), `level`, `message`, and every
 * other field under `attrs`.
 */

const TIMESTAMP_FIELDS = ["@timestamp", "timestamp", "time", "ts", "datetime", "date"];
const MESSAGE_FIELDS = ["message", "msg", "log", "text"];
const LEVEL_FIELDS = ["level", "severity", "log.level", "levelname", "loglevel", "severity_text"];

/** Longest line (UTF-8 bytes, without the newline) kept as is; longer messages are cut. */
export const MAX_LINE_BYTES = 100_000;
/** A level is a short word; anything longer is cut so it cannot crowd out the message. */
const MAX_LEVEL_LENGTH = 32;
/**
 * A record's own time is trusted only from 7 days before to 1 minute after it
 * arrived; anything else (durations, numbers like 20261020, a skewed clock)
 * falls back to the arrival time. sv-be refuses a whole batch, permanently,
 * whose window starts over 30 days back or ends over 5 minutes ahead of its
 * clock, so this keeps every batch inside that even after days of retries.
 */
const MAX_PAST_MS = 7 * 86_400_000;
const MAX_FUTURE_MS = 60_000;

const encoder = new TextEncoder();

export function utf8Length(s: string): number {
	return encoder.encode(s).byteLength;
}

function takeFirst(rest: Record<string, unknown>, keys: string[]): unknown {
	for (const k of keys) {
		if (rest[k] != null) {
			const v = rest[k];
			delete rest[k];
			return v;
		}
	}
	return undefined;
}

/** Epoch ms from a number in seconds, ms, µs or ns (picked by magnitude). */
function epochToMs(n: number): number {
	const abs = Math.abs(n);
	if (abs < 1e11) return n * 1000;
	if (abs < 1e14) return n;
	if (abs < 1e17) return n / 1e3;
	return n / 1e6;
}

export function parseTimestamp(value: unknown): number | null {
	let ms: number;
	if (typeof value === "number") ms = epochToMs(value);
	else if (typeof value === "string" && /^\d+(\.\d+)?$/.test(value.trim())) ms = epochToMs(Number(value));
	else if (typeof value === "string") ms = Date.parse(value);
	else return null;
	// Date can represent ±8.64e15 ms; anything outside is garbage.
	return Number.isFinite(ms) && Math.abs(ms) <= 8.64e15 ? Math.floor(ms) : null;
}

function truncateUtf8(s: string, maxBytes: number): string {
	const bytes = encoder.encode(s);
	if (bytes.byteLength <= maxBytes) return s;
	// Decoding a cut that lands mid-character yields U+FFFD; drop it.
	return new TextDecoder().decode(bytes.subarray(0, maxBytes)).replace(/\uFFFD$/, "");
}

/** Returns the NDJSON line, without the trailing newline. */
export function toLogLine(doc: Record<string, unknown>, index: string | undefined, receivedAt: number): string {
	const rest: Record<string, unknown> = { ...doc };
	const rawTs = takeFirst(rest, TIMESTAMP_FIELDS);
	const rawMessage = takeFirst(rest, MESSAGE_FIELDS);
	const rawLevel = takeFirst(rest, LEVEL_FIELDS);

	let ts = rawTs === undefined ? null : parseTimestamp(rawTs);
	if (ts !== null && (ts < receivedAt - MAX_PAST_MS || ts > receivedAt + MAX_FUTURE_MS)) ts = null;
	if (ts === null) {
		if (rawTs !== undefined) rest["timestamp_raw"] = rawTs;
		ts = receivedAt;
	}

	let message: string;
	if (typeof rawMessage === "string") message = rawMessage;
	else if (rawMessage !== undefined) message = JSON.stringify(rawMessage);
	else message = "";

	const out: Record<string, unknown> = { timestamp: new Date(ts).toISOString() };
	if (typeof rawLevel === "string" && rawLevel !== "") out.level = rawLevel.slice(0, MAX_LEVEL_LENGTH).toLowerCase();
	else if (typeof rawLevel === "number") out.level = String(rawLevel);
	out.message = message;
	if (index !== undefined) rest["opensearch.index"] = index;
	if (Object.keys(rest).length > 0) out.attrs = rest;

	let line = JSON.stringify(out);
	if (utf8Length(line) > MAX_LINE_BYTES) {
		// Keep timestamp, level and as much of the message as fits; attrs go
		// first since they are the least useful part of an oversized record.
		const slim: Record<string, unknown> = { timestamp: out.timestamp };
		if (out.level !== undefined) slim.level = out.level;
		slim.message = "";
		slim.attrs = { truncated: true };
		const budget = MAX_LINE_BYTES - utf8Length(JSON.stringify(slim));
		// JSON escaping can grow the message up to 6x (\u00XX); shrink until it fits.
		let cut = truncateUtf8(message, budget);
		for (;;) {
			slim.message = cut;
			line = JSON.stringify(slim);
			// The level is capped, so an empty message always fits; the cut === "" guard is a backstop.
			if (utf8Length(line) <= MAX_LINE_BYTES || cut === "") break;
			cut = truncateUtf8(cut, Math.floor(utf8Length(cut) * 0.8));
		}
	}
	return line;
}
