import type { QueueBody } from "./batching";

export interface Env {
	/** Producer binding for the log queue (wrangler.toml `[[queues.producers]]`). */
	LOG_QUEUE: Queue<QueueBody>;

	/** sv-be origin, e.g. `https://app.example.com`; routes are under `/be/ingest/`. */
	SV_SERVER: string;
	/** Agent client id (`sva_…`). Public, so a plain variable in wrangler.toml. */
	SV_CLIENT_ID: string;
	/** Agent client secret (`svas_…`). Secret. */
	SV_CLIENT_SECRET: string;
	/** Instance name reported on every upload; `[A-Za-z0-9._-]{1,63}`. */
	SV_INSTANCE?: string;

	/** Basic auth user App Platform sends; any user is accepted when unset. */
	COLLECTOR_USER?: string;
	/** Basic auth password App Platform sends. Secret, at least 16 characters. */
	COLLECTOR_PASSWORD: string;

	/** Cap on the request body as sent (bytes). */
	MAX_BODY_BYTES?: string;
	/** Cap on the request body after Content-Encoding is removed (bytes). */
	MAX_DECOMPRESSED_BYTES?: string;
	/** "true" logs a summary of every request (Authorization redacted) for `wrangler tail`. */
	DEBUG_LOG_REQUESTS?: string;

	/** Per-IP limit on all requests. Optional: unbound means unlimited. */
	REQUEST_LIMITER?: RateLimit;
	/** Per-IP limit on failed Basic auth attempts. Optional. */
	AUTH_FAIL_LIMITER?: RateLimit;
}

const DEFAULT_INSTANCE = "cf-worker";
// A request is held in memory several times over while it is parsed and
// packed (about 8x its decompressed size), so these stay well under the
// isolate's 128 MB.
export const DEFAULT_MAX_BODY_BYTES = 8 * 1024 * 1024;
export const DEFAULT_MAX_DECOMPRESSED_BYTES = 8 * 1024 * 1024;
const MIN_PASSWORD_LENGTH = 16;
/** Matches placeholder values such as REPLACE_ME or change-me, which must never be used as secrets. */
const PLACEHOLDER_RE = /replace[-_ ]?me|change[-_ ]?me/i;
const CLIENT_ID_RE = /^sva_[A-Za-z0-9_-]{22}$/;
const INSTANCE_RE = /^[A-Za-z0-9._-]{1,63}$/;

/** Why COLLECTOR_PASSWORD cannot be used, or null. The receiver fails closed on any of these. */
export function passwordProblem(env: Env): string | null {
	const pw = env.COLLECTOR_PASSWORD ?? "";
	if (pw === "") return "COLLECTOR_PASSWORD is not set";
	if (pw.trim().length < MIN_PASSWORD_LENGTH) return `COLLECTOR_PASSWORD is shorter than ${MIN_PASSWORD_LENGTH} characters`;
	if (PLACEHOLDER_RE.test(pw)) return "COLLECTOR_PASSWORD is still a placeholder";
	return null;
}

/** What is wrong with the settings the uploader needs; empty when they look usable. */
export function uploadConfigProblems(env: Env): string[] {
	const problems: string[] = [];
	if (!env.SV_SERVER) problems.push("SV_SERVER is not set");
	if (!env.SV_CLIENT_ID) problems.push("SV_CLIENT_ID is not set");
	else if (!CLIENT_ID_RE.test(env.SV_CLIENT_ID)) problems.push("SV_CLIENT_ID is not a StreamVessel client id (sva_ + 22 characters)");
	if (!env.SV_CLIENT_SECRET) problems.push("SV_CLIENT_SECRET is not set");
	else if (!env.SV_CLIENT_SECRET.startsWith("svas_")) problems.push("SV_CLIENT_SECRET is not a StreamVessel secret (svas_…)");
	const instance = instanceName(env);
	if (!INSTANCE_RE.test(instance) || instance === "." || instance === "..") {
		problems.push(`SV_INSTANCE ${JSON.stringify(instance)} must match [A-Za-z0-9._-]{1,63}`);
	}
	return problems;
}

export function instanceName(env: Env): string {
	return env.SV_INSTANCE?.trim() || DEFAULT_INSTANCE;
}

export function intVar(value: string | undefined, fallback: number): number {
	if (value === undefined || value.trim() === "") return fallback;
	const n = Number(value);
	return Number.isSafeInteger(n) && n > 0 ? n : fallback;
}
