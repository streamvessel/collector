/**
 * The agent side of the sv-be ingest API (streamvessel/sv-be#385):
 * client_credentials token, presigned upload request, and the PUT itself.
 */

/** sv-be refused us in a way retrying cannot fix (revoked agent, deleted secret, bad config). */
export class PermanentError extends Error {
	constructor(readonly code: string, message: string) {
		super(message);
	}
}

/** Worth retrying later; `retryAfterSeconds` comes from Retry-After when sv-be sent one. */
export class TransientError extends Error {
	constructor(message: string, readonly retryAfterSeconds?: number) {
		super(message);
	}
}

export interface SvConfig {
	server: string;
	clientId: string;
	clientSecret: string;
}

export interface UploadRequest {
	instance: string;
	startTs: string;
	endTs: string;
	sha256: string;
}

export interface UploadTarget {
	url: string;
	headers: Record<string, string>;
	key: string;
}

/** Refresh this long before the token expires. */
const TOKEN_REFRESH_MARGIN_MS = 60_000;

interface CachedToken {
	cacheKey: string;
	token: string;
	expiresAt: number;
}

// Per isolate: every cold isolate fetches its own token, which sv-be allows.
let cachedToken: CachedToken | null = null;

export function resetTokenCache(): void {
	cachedToken = null;
}

function endpoint(cfg: SvConfig, path: string): string {
	return cfg.server.replace(/\/+$/, "") + path;
}

function retryAfter(res: Response): number | undefined {
	const v = res.headers.get("retry-after");
	if (!v) return undefined;
	const secs = Number(v);
	if (Number.isFinite(secs) && secs >= 0) return secs;
	const date = Date.parse(v);
	return Number.isFinite(date) ? Math.max(0, Math.ceil((date - Date.now()) / 1000)) : undefined;
}

/** sv-be's `{error, error_description}`; the description is kept for the logs. */
async function errorBody(res: Response): Promise<{ code: string; description: string }> {
	try {
		const body = (await res.json()) as { error?: unknown; error_description?: unknown };
		return {
			code: typeof body.error === "string" ? body.error : `http_${res.status}`,
			description: typeof body.error_description === "string" ? body.error_description : "",
		};
	} catch {
		return { code: `http_${res.status}`, description: "" };
	}
}

/** Classifies an sv-be error response. */
async function failure(what: string, res: Response): Promise<Error> {
	const { code, description } = await errorBody(res);
	const message = `${what}: HTTP ${res.status} ${code}${description ? ` (${description})` : ""}`;
	if (res.status === 429 || res.status === 408 || res.status >= 500) {
		return new TransientError(message, retryAfter(res));
	}
	return new PermanentError(code, message);
}

async function send(what: string, input: string, init: RequestInit): Promise<Response> {
	try {
		return await fetch(input, init);
	} catch (err) {
		throw new TransientError(`${what}: ${String(err)}`);
	}
}

/** Asks sv-be for a new token, bypassing the cache; `/_check` uses it to test the credentials. */
export async function requestToken(cfg: SvConfig): Promise<{ token: string; expiresIn: number }> {
	// RFC 6749 §2.3.1: client_secret_basic, form-urlencoded id and secret.
	const basic = btoa(`${encodeURIComponent(cfg.clientId)}:${encodeURIComponent(cfg.clientSecret)}`);
	const res = await send("token request", endpoint(cfg, "/be/ingest/oauth/token"), {
		method: "POST",
		headers: {
			authorization: `Basic ${basic}`,
			"content-type": "application/x-www-form-urlencoded",
			accept: "application/json",
		},
		body: "grant_type=client_credentials",
	});
	if (!res.ok) throw await failure("token request", res);
	const body = (await res.json()) as { access_token?: unknown; expires_in?: unknown };
	if (typeof body.access_token !== "string" || body.access_token === "") {
		throw new TransientError("token request: response has no access_token");
	}
	const expiresIn = typeof body.expires_in === "number" && body.expires_in > 0 ? body.expires_in : 900;
	return { token: body.access_token, expiresIn };
}

async function getToken(cfg: SvConfig): Promise<string> {
	const now = Date.now();
	const cacheKey = `${cfg.server} ${cfg.clientId}`;
	if (cachedToken && cachedToken.cacheKey === cacheKey && cachedToken.expiresAt - TOKEN_REFRESH_MARGIN_MS > now) {
		return cachedToken.token;
	}
	const { token, expiresIn } = await requestToken(cfg);
	cachedToken = { cacheKey, token, expiresAt: now + expiresIn * 1000 };
	return token;
}

export async function requestUpload(cfg: SvConfig, req: UploadRequest): Promise<UploadTarget> {
	for (let attempt = 0; ; attempt++) {
		const token = await getToken(cfg);
		const res = await send("upload request", endpoint(cfg, "/be/ingest/uploads"), {
			method: "POST",
			headers: { authorization: `Bearer ${token}`, "content-type": "application/json", accept: "application/json" },
			body: JSON.stringify(req),
		});
		if (res.status === 401 && attempt === 0) {
			// invalid_token: expired or deleted with its secret; fetch a new one once.
			await res.body?.cancel();
			resetTokenCache();
			continue;
		}
		if (res.status === 401) throw new TransientError(`upload request: HTTP 401 ${(await errorBody(res)).code}`);
		if (!res.ok) throw await failure("upload request", res);
		const body = (await res.json()) as Partial<UploadTarget>;
		if (typeof body.url !== "string" || typeof body.key !== "string") {
			throw new TransientError("upload request: response has no url or key");
		}
		const headers: Record<string, string> = {};
		for (const [k, v] of Object.entries(body.headers ?? {})) {
			if (typeof v === "string") headers[k] = v;
		}
		return { url: body.url, headers, key: body.key };
	}
}

type PutOutcome = "stored" | "exists" | "url_refused";

/**
 * PUTs the object with exactly the headers sv-be signed, plus Content-Type,
 * and never an Authorization header: the bucket would refuse it beside the
 * signature, and it would hand our token to the storage provider.
 */
export async function putObject(target: UploadTarget, body: Uint8Array): Promise<PutOutcome> {
	// Not signed by sv-be, so ours to set. No Content-Encoding: the object is a
	// .log.gz file, and the log viewer gunzips it itself.
	const headers = new Headers({ "content-type": "application/gzip" });
	for (const [k, v] of Object.entries(target.headers)) {
		if (k.toLowerCase() !== "authorization") headers.set(k, v);
	}
	const res = await send("PUT", target.url, { method: "PUT", headers, body });
	await res.body?.cancel();
	if (res.ok) return "stored";
	// If-None-Match: * refused: the key (which carries the hash) already holds this batch.
	if (res.status === 412) return "exists";
	// Expired URL or a signature/clock problem: a new URL may help.
	if (res.status === 403) return "url_refused";
	throw new TransientError(`PUT: HTTP ${res.status}`, retryAfter(res));
}
