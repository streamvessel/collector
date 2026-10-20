import { checkBasicAuth } from "./auth";
import { enqueueLines } from "./batching";
import { BodyError, readBody } from "./body";
import { BulkRequestError, bulkResponse, parseBulk } from "./bulk";
import { handleQueue } from "./consumer";
import {
	DEFAULT_MAX_BODY_BYTES,
	DEFAULT_MAX_DECOMPRESSED_BYTES,
	intVar,
	MIN_PASSWORD_LENGTH,
	type Env,
} from "./env";
import { errorResponse, json, versionDocument } from "./opensearch";

const BULK_PATH_RE = /^\/(?:([^/_][^/]*)\/)?_bulk\/?$/;
const WWW_AUTHENTICATE = { "www-authenticate": 'Basic realm="streamvessel", charset="UTF-8"' };

function logRequest(request: Request): void {
	const headers: Record<string, string> = {};
	for (const [k, v] of request.headers) headers[k] = k === "authorization" ? "[redacted]" : v;
	const url = new URL(request.url);
	console.log(JSON.stringify({ event: "request", method: request.method, path: url.pathname, query: url.search, headers }));
}

async function limited(limiter: RateLimit | undefined, key: string): Promise<boolean> {
	if (!limiter) return false;
	const { success } = await limiter.limit({ key });
	return !success;
}

async function handleBulk(request: Request, env: Env, pathIndex: string | undefined): Promise<Response> {
	const started = Date.now();
	let body: string;
	try {
		body = await readBody(
			request,
			intVar(env.MAX_BODY_BYTES, DEFAULT_MAX_BODY_BYTES),
			intVar(env.MAX_DECOMPRESSED_BYTES, DEFAULT_MAX_DECOMPRESSED_BYTES),
		);
	} catch (err) {
		if (err instanceof BodyError) return errorResponse(err.status, err.type, err.message);
		throw err;
	}

	let items;
	try {
		items = parseBulk(body, pathIndex, started);
	} catch (err) {
		if (err instanceof BulkRequestError) return errorResponse(400, err.type, err.message);
		throw err;
	}

	const lines = items.flatMap((it) => (it.line === undefined ? [] : [it.line]));
	if (lines.length > 0) {
		try {
			await enqueueLines(env.LOG_QUEUE, lines);
		} catch (err) {
			// Nothing is acknowledged unless everything is queued; the sender retries
			// the whole request, so lines from earlier sendBatch calls may repeat.
			console.error(JSON.stringify({ level: "error", event: "enqueue_failed", error: String(err), lines: lines.length }));
			return errorResponse(503, "unavailable_shards_exception", "could not queue the documents; retry", {
				"retry-after": "5",
			});
		}
	}
	if (env.DEBUG_LOG_REQUESTS === "true") {
		console.log(JSON.stringify({ event: "bulk", index: pathIndex, items: items.length, queued: lines.length, bytes: body.length }));
	}
	return json(bulkResponse(items, Date.now() - started));
}

export async function handleFetch(request: Request, env: Env): Promise<Response> {
	if (env.DEBUG_LOG_REQUESTS === "true") logRequest(request);
	const ip = request.headers.get("cf-connecting-ip") ?? "unknown";

	if (await limited(env.REQUEST_LIMITER, ip)) {
		return errorResponse(429, "es_rejected_execution_exception", "rate limit exceeded", { "retry-after": "10" });
	}
	if (!env.BULK_PASSWORD || env.BULK_PASSWORD.length < MIN_PASSWORD_LENGTH) {
		// Fail closed: never run open, or behind a guessable password.
		console.error(JSON.stringify({ level: "error", event: "misconfigured", error: `BULK_PASSWORD must be at least ${MIN_PASSWORD_LENGTH} characters` }));
		return errorResponse(500, "exception", "receiver is not configured");
	}
	if (!(await checkBasicAuth(request.headers.get("authorization"), env.BULK_USER, env.BULK_PASSWORD))) {
		if (await limited(env.AUTH_FAIL_LIMITER, ip)) {
			return errorResponse(429, "es_rejected_execution_exception", "too many failed authentication attempts", {
				"retry-after": "60",
			});
		}
		return errorResponse(401, "security_exception", "missing or invalid authentication credentials", WWW_AUTHENTICATE);
	}

	const { pathname } = new URL(request.url);
	if (pathname === "/" && (request.method === "GET" || request.method === "HEAD")) {
		return request.method === "HEAD" ? new Response(null, { status: 200 }) : json(versionDocument());
	}
	const bulk = BULK_PATH_RE.exec(pathname);
	if (bulk) {
		if (request.method !== "POST" && request.method !== "PUT") {
			return errorResponse(405, "illegal_argument_exception", `method ${request.method} not allowed for ${pathname}`, {
				allow: "POST, PUT",
			});
		}
		let index: string | undefined;
		try {
			index = bulk[1] === undefined ? undefined : decodeURIComponent(bulk[1]);
		} catch {
			return errorResponse(400, "invalid_index_name_exception", "index name is not valid percent-encoding");
		}
		return handleBulk(request, env, index);
	}
	return errorResponse(404, "resource_not_found_exception", `no handler for ${request.method} ${pathname}`);
}

export default {
	fetch: handleFetch,
	queue: (batch, env) => handleQueue(batch, env),
} satisfies ExportedHandler<Env, unknown>;
