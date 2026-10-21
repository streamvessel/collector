import type { QueueBody } from "./batching";
import { DEFAULT_INSTANCE, type Env } from "./env";
import { log } from "./log";
import { PermanentError, putObject, requestUpload, TransientError, type SvConfig, type UploadRequest } from "./svclient";

const INSTANCE_RE = /^[A-Za-z0-9._-]{1,63}$/;
/** New URLs per batch after a 403 on the PUT, before the batch is retried later. */
const MAX_URL_REFUSALS = 3;
/** Backoff for transient failures: 30 s doubling per attempt, capped at an hour. */
const BASE_DELAY_SECONDS = 30;
const MAX_DELAY_SECONDS = 3600;
/** Retry delay after a permanent refusal: slow enough not to spin, fast enough to resume after a fix. */
const PERMANENT_RETRY_SECONDS = 3600;
/** Cloudflare's ceiling for a retry delay. */
const MAX_QUEUE_DELAY_SECONDS = 43_200;
/** After a permanent refusal, stop calling sv-be from this isolate for this long. */
const PERMANENT_COOLDOWN_MS = 10 * 60_000;

let blocked: { until: number; reason: string } | null = null;

export function resetConsumerState(): void {
	blocked = null;
}

const LINE_TS_RE = /^\{"timestamp":"([^"]+)"/;

export async function gzip(text: string): Promise<Uint8Array> {
	const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
	return new Uint8Array(await new Response(stream).arrayBuffer());
}

function hex(buf: ArrayBuffer): string {
	return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Joins the messages' lines, gzips and hashes them, and finds the time window. */
async function prepareUpload(
	bodies: QueueBody[],
	fallbackTs: number,
): Promise<{ gzip: Uint8Array; meta: Omit<UploadRequest, "instance"> }> {
	let min = Infinity;
	let max = -Infinity;
	for (const body of bodies) {
		for (const line of body.split("\n")) {
			if (line === "") continue;
			const m = LINE_TS_RE.exec(line);
			const ts = m ? Date.parse(m[1]) : NaN;
			if (Number.isFinite(ts)) {
				if (ts < min) min = ts;
				if (ts > max) max = ts;
			}
		}
	}
	if (!Number.isFinite(min)) min = max = fallbackTs;
	const data = await gzip(bodies.join(""));
	const sha = await crypto.subtle.digest("SHA-256", data);
	return {
		gzip: data,
		meta: {
			startTs: new Date(min).toISOString(),
			endTs: new Date(max).toISOString(),
			// Names the file (its first 8 characters end the key); sv-be does not check it.
			sha256: hex(sha),
		},
	};
}

function backoffSeconds(attempts: number, retryAfter?: number): number {
	const exp = Math.min(BASE_DELAY_SECONDS * 2 ** Math.max(0, attempts - 1), MAX_DELAY_SECONDS);
	return Math.min(Math.max(exp, retryAfter ?? 0), MAX_QUEUE_DELAY_SECONDS);
}

export async function handleQueue(batch: MessageBatch<unknown>, env: Env): Promise<void> {
	const valid: Message<unknown>[] = [];
	for (const msg of batch.messages) {
		if (typeof msg.body === "string" && msg.body !== "") valid.push(msg);
		else {
			// Nothing we produce looks like this; retrying cannot help.
			log("error", "malformed_message_dropped", { queue: batch.queue, id: msg.id });
			msg.ack();
		}
	}
	if (valid.length === 0) return;

	const attempts = Math.max(...valid.map((m) => m.attempts));
	const retry = (delaySeconds: number) => {
		for (const m of valid) m.retry({ delaySeconds });
	};

	const now = Date.now();
	if (blocked && blocked.until > now) {
		log("error", "upload_blocked", { queue: batch.queue, reason: blocked.reason, messages: valid.length, attempts });
		retry(PERMANENT_RETRY_SECONDS);
		return;
	}

	const instance = env.SV_INSTANCE?.trim() || DEFAULT_INSTANCE;
	const cfg: SvConfig = { server: env.SV_SERVER, clientId: env.SV_CLIENT_ID, clientSecret: env.SV_CLIENT_SECRET };
	try {
		if (!cfg.server || !cfg.clientId || !cfg.clientSecret) {
			throw new PermanentError("misconfigured", "SV_SERVER, SV_CLIENT_ID and SV_CLIENT_SECRET must be set");
		}
		if (!INSTANCE_RE.test(instance) || instance === "." || instance === "..") {
			throw new PermanentError("misconfigured", `SV_INSTANCE ${JSON.stringify(instance)} must match [A-Za-z0-9._-]{1,63}`);
		}
		const earliest = Math.min(...valid.map((m) => m.timestamp.getTime()));
		const upload = await prepareUpload(valid.map((m) => m.body as string), earliest);
		const request: UploadRequest = { instance, ...upload.meta };
		for (let refusals = 0; ; ) {
			const target = await requestUpload(cfg, request);
			const outcome = await putObject(target, upload.gzip);
			if (outcome !== "url_refused") {
				log("info", "uploaded", { key: target.key, outcome, bytes: upload.gzip.byteLength, messages: valid.length, attempts });
				break;
			}
			if (++refusals >= MAX_URL_REFUSALS) {
				throw new TransientError(`PUT refused with 403 ${refusals} times; check the bucket credentials and clock`);
			}
		}
		for (const m of valid) m.ack();
	} catch (err) {
		if (err instanceof PermanentError) {
			// invalid_client, agent_revoked, scope_changed, bad config: alert and
			// back off hard instead of spinning. The messages stay queued, then
			// land in the dead-letter queue once max_retries is spent.
			blocked = { until: now + PERMANENT_COOLDOWN_MS, reason: err.code };
			log("error", "upload_refused_permanently", { code: err.code, error: err.message, messages: valid.length, attempts });
			retry(PERMANENT_RETRY_SECONDS);
			return;
		}
		const delay = backoffSeconds(attempts, err instanceof TransientError ? err.retryAfterSeconds : undefined);
		log("warn", "upload_failed", { error: String(err instanceof Error ? err.message : err), retryInSeconds: delay, attempts });
		retry(delay);
	}
}
