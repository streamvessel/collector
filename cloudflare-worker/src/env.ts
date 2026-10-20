import type { QueueBody } from "./batching";

export interface Env {
	/** Producer binding for the log queue (wrangler.toml `[[queues.producers]]`). */
	LOG_QUEUE: Queue<QueueBody>;

	/** sv-be origin, e.g. `https://app.example.com`; routes are under `/be/ingest/`. */
	SV_SERVER: string;
	/** Agent client id (`sva_…`). Secret. */
	SV_CLIENT_ID: string;
	/** Agent client secret (`svas_…`). Secret. */
	SV_CLIENT_SECRET: string;
	/** Instance name reported on every upload; `[A-Za-z0-9._-]{1,63}`. */
	SV_INSTANCE?: string;

	/** Basic auth user App Platform sends; any user is accepted when unset. */
	BULK_USER?: string;
	/** Basic auth password App Platform sends. Secret, at least 16 characters. */
	BULK_PASSWORD: string;

	/** Cap on the request body as sent (bytes). */
	MAX_BODY_BYTES?: string;
	/** Cap on the request body after Content-Encoding is removed (bytes). */
	MAX_DECOMPRESSED_BYTES?: string;
	/** "true" logs a summary of every request (headers redacted) for `wrangler tail`. */
	DEBUG_LOG_REQUESTS?: string;

	/** Per-IP limit on all requests. Optional: unbound means unlimited. */
	REQUEST_LIMITER?: RateLimit;
	/** Per-IP limit on failed Basic auth attempts. Optional. */
	AUTH_FAIL_LIMITER?: RateLimit;
}

export const DEFAULT_INSTANCE = "cloudflare-worker";
export const DEFAULT_MAX_BODY_BYTES = 10 * 1024 * 1024;
export const DEFAULT_MAX_DECOMPRESSED_BYTES = 32 * 1024 * 1024;
export const MIN_PASSWORD_LENGTH = 16;

export function intVar(value: string | undefined, fallback: number): number {
	if (value === undefined || value.trim() === "") return fallback;
	const n = Number(value);
	return Number.isSafeInteger(n) && n > 0 ? n : fallback;
}
