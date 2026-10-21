import { createExecutionContext, createMessageBatch, getQueueResult } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetConsumerState } from "../src/consumer";
import type { Env } from "../src/env";
import worker from "../src/index";
import { resetTokenCache } from "../src/svclient";
import { FakeSvBe, gunzipText } from "./fake-svbe";

let sv: FakeSvBe;
let errorLog: { mock: { calls: unknown[][] } };

function testEnv(overrides: Partial<Env> = {}): Env {
	return {
		LOG_QUEUE: undefined as unknown as Queue<string>,
		SV_SERVER: sv.server,
		SV_CLIENT_ID: sv.clientId,
		SV_CLIENT_SECRET: sv.clientSecret,
		SV_INSTANCE: "cf-test",
		BULK_PASSWORD: "unused-in-consumer-tests",
		...overrides,
	};
}

function line(ts: string, message: string): string {
	return JSON.stringify({ timestamp: ts, message }) + "\n";
}

function batchOf(bodies: unknown[], attempts = 1) {
	return createMessageBatch("sv-logs", bodies.map((body, i) => ({ id: `m${i}`, timestamp: new Date("2026-10-20T12:00:00Z"), attempts, body })));
}

/**
 * Runs the consumer on a batch. getQueueResult() does not report retry delays,
 * so `retryMessages` here is rebuilt from spies on each message's retry().
 */
async function run(bodies: unknown[], env = testEnv(), attempts = 1) {
	const batch = batchOf(bodies, attempts);
	const retried: Array<{ msgId: string; delaySeconds?: number }> = [];
	for (const msg of batch.messages) {
		const original = msg.retry.bind(msg);
		msg.retry = (opts?: QueueRetryOptions) => {
			retried.push({ msgId: msg.id, delaySeconds: opts?.delaySeconds });
			original(opts);
		};
	}
	const ctx = createExecutionContext();
	await worker.queue(batch as MessageBatch<unknown>, env);
	const result = await getQueueResult(batch, ctx);
	expect(result.retryMessages.map((m) => m.msgId)).toEqual(retried.map((m) => m.msgId));
	return { ...result, retryMessages: retried };
}

beforeEach(() => {
	sv = new FakeSvBe();
	resetTokenCache();
	resetConsumerState();
	vi.stubGlobal("fetch", sv.fetch);
	vi.spyOn(console, "log").mockImplementation(() => {});
	vi.spyOn(console, "warn").mockImplementation(() => {});
	errorLog = vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

describe("queue consumer", () => {
	it("gzips the lines, presigns with sha256 and time window, PUTs without Authorization, and acks", async () => {
		const bodies = [
			line("2026-10-20T12:00:05.000Z", "b") + line("2026-10-20T12:00:01.000Z", "a"),
			line("2026-10-20T12:00:09.500Z", "c"),
		];
		const result = await run(bodies);

		expect(result.retryMessages).toEqual([]);
		expect(result.explicitAcks.sort()).toEqual(["m0", "m1"]);

		expect(sv.tokenCalls).toHaveLength(1);
		expect(sv.tokenCalls[0].body).toBe("grant_type=client_credentials");
		expect(sv.uploadCalls).toHaveLength(1);
		const up = sv.uploadCalls[0];
		expect(up.auth).toBe("Bearer svla_1");
		expect(up.body).toMatchObject({ instance: "cf-test", startTs: "2026-10-20T12:00:01.000Z", endTs: "2026-10-20T12:00:09.500Z" });

		expect(sv.putCalls).toHaveLength(1);
		expect(sv.putCalls[0].headers.authorization).toBeUndefined();
		expect(sv.putCalls[0].headers["if-none-match"]).toBe("*");

		const [obj] = [...sv.objects.values()];
		// sv-be no longer checks the hash, so check here that it is the SHA-256 of what was stored.
		const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", obj.body));
		expect(up.body.sha256).toBe([...digest].map((b) => b.toString(16).padStart(2, "0")).join(""));
		expect(Object.keys(up.body).sort()).toEqual(["endTs", "instance", "sha256", "startTs"]);
		expect(await gunzipText(obj.body)).toBe(bodies.join(""));
	});

	it("PUTs with no extra headers when sv-be signs none (non-AWS provider)", async () => {
		sv.provider = "other";
		const result = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(result.explicitAcks).toEqual(["m0"]);
		expect(sv.putCalls[0].headers["if-none-match"]).toBeUndefined();
		expect(sv.putCalls[0].headers.authorization).toBeUndefined();
		expect(sv.objects.size).toBe(1);
	});

	it("logs sv-be's error_description with a permanent refusal", async () => {
		sv.onUpload = () =>
			Response.json({ error: "invalid_request", error_description: "window starts more than 30 days ago" }, { status: 400 });
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(errorLog).toHaveBeenCalledWith(expect.stringContaining("window starts more than 30 days ago"));
	});

	it("treats 400 invalid_instance as permanent", async () => {
		sv.onUpload = () => Response.json({ error: "invalid_instance" }, { status: 400 });
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(errorLog).toHaveBeenCalledWith(expect.stringContaining("invalid_instance"));
	});

	it("sends Content-Type: application/gzip and no Authorization or Content-Encoding on every PUT", async () => {
		await run([line("2026-10-20T12:00:00.000Z", "aws")]);
		sv.provider = "other";
		await run([line("2026-10-20T12:00:01.000Z", "other")]);
		let refusals = 1;
		sv.onPut = () => (refusals-- > 0 ? new Response(null, { status: 403 }) : undefined);
		await run([line("2026-10-20T12:00:02.000Z", "retried")]);

		expect(sv.putCalls).toHaveLength(4);
		for (const put of sv.putCalls) {
			expect(put.headers["content-type"]).toBe("application/gzip");
			expect(put.headers.authorization).toBeUndefined();
			expect(put.headers["content-encoding"]).toBeUndefined();
		}
	});

	it("never forwards an Authorization header even if sv-be lists one", async () => {
		sv.onUpload = undefined;
		const orig = sv.fetch;
		vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
			const res = await orig(input, init);
			if (new URL(new Request(input, init).url).pathname === "/be/ingest/uploads" && res.ok) {
				const body = (await res.json()) as { headers: Record<string, string> };
				body.headers.Authorization = "Bearer leaked";
				return Response.json(body);
			}
			return res;
		});
		const result = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(result.explicitAcks).toEqual(["m0"]);
		expect(sv.putCalls[0].headers.authorization).toBeUndefined();
	});

	it("caches the token across batches in the isolate", async () => {
		await run([line("2026-10-20T12:00:00.000Z", "1")]);
		await run([line("2026-10-20T12:00:01.000Z", "2")]);
		expect(sv.tokenCalls).toHaveLength(1);
		expect(sv.uploadCalls).toHaveLength(2);
	});

	it("fetches a new token once on 401 invalid_token", async () => {
		await run([line("2026-10-20T12:00:00.000Z", "1")]);
		sv.issuedTokens.clear(); // e.g. secret deleted and recreated
		const result = await run([line("2026-10-20T12:00:01.000Z", "2")]);
		expect(result.explicitAcks).toEqual(["m0"]);
		expect(sv.tokenCalls).toHaveLength(2);
	});

	it("treats 412 on the PUT as already stored", async () => {
		sv.onPut = () => new Response(null, { status: 412 });
		const result = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(result.explicitAcks).toEqual(["m0"]);
	});

	it("asks for a new URL after a 403 on the PUT, at most 3 times, then retries later", async () => {
		let refusals = 1;
		sv.onPut = () => (refusals-- > 0 ? new Response(null, { status: 403 }) : undefined);
		const ok = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(ok.explicitAcks).toEqual(["m0"]);
		expect(sv.uploadCalls).toHaveLength(2);

		sv.uploadCalls = [];
		sv.onPut = () => new Response(null, { status: 403 });
		const failed = await run([line("2026-10-20T12:00:00.000Z", "y")]);
		expect(sv.uploadCalls).toHaveLength(3);
		expect(failed.explicitAcks).toEqual([]);
		expect(failed.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 30 }]);
	});

	it("retries transient failures with exponential backoff and honours Retry-After", async () => {
		sv.onUpload = () => new Response("down", { status: 503 });
		const r1 = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv(), 1);
		expect(r1.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 30 }]);
		const r4 = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv(), 4);
		expect(r4.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 240 }]);
		const r20 = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv(), 20);
		expect(r20.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);

		sv.onUpload = () => Response.json({ error: "rate_limited" }, { status: 429, headers: { "retry-after": "120" } });
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv(), 1);
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 120 }]);

		vi.stubGlobal("fetch", async () => {
			throw new Error("network down");
		});
		const n = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv(), 2);
		expect(n.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 60 }]);
	});

	it("treats invalid_client as permanent: backs off an hour and stops calling sv-be", async () => {
		const env = testEnv({ SV_CLIENT_SECRET: "svas_deleted" });
		const r1 = await run([line("2026-10-20T12:00:00.000Z", "x")], env);
		expect(r1.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(sv.tokenCalls).toHaveLength(1);
		expect(errorLog).toHaveBeenCalledWith(expect.stringContaining("invalid_client"));

		// next batch in the same isolate does not hit sv-be at all
		const r2 = await run([line("2026-10-20T12:00:00.000Z", "y")], env);
		expect(r2.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(sv.tokenCalls).toHaveLength(1);
	});

	it("treats 409 too_many_instances / connection_unreadable as permanent and 502 storage_unavailable as transient", async () => {
		for (const error of ["too_many_instances", "connection_unreadable"]) {
			resetConsumerState();
			sv.onUpload = () => Response.json({ error }, { status: 409 });
			const r = await run([line("2026-10-20T12:00:00.000Z", "x")]);
			expect(r.retryMessages, error).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		}
		resetConsumerState();
		sv.onUpload = () => Response.json({ error: "storage_unavailable" }, { status: 502 });
		const t = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(t.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 30 }]);
	});

	it("refuses a client id that cannot be one, as sv-be does (invalid_client, permanent)", async () => {
		// Id and secret match the fake's records, so only the shape check can refuse it.
		sv.clientId = "sva_short";
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv({ SV_CLIENT_ID: "sva_short" }));
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(errorLog).toHaveBeenCalledWith(expect.stringContaining("invalid_client"));
	});

	it("treats 403 agent_revoked from uploads as permanent", async () => {
		sv.onUpload = () => Response.json({ error: "agent_revoked" }, { status: 403 });
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")]);
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(sv.putCalls).toHaveLength(0);
		expect(errorLog).toHaveBeenCalledWith(expect.stringContaining("agent_revoked"));
		await run([line("2026-10-20T12:00:00.000Z", "y")]);
		expect(sv.uploadCalls).toHaveLength(1);
	});

	it("treats a bad SV_INSTANCE as permanent configuration error", async () => {
		const r = await run([line("2026-10-20T12:00:00.000Z", "x")], testEnv({ SV_INSTANCE: "../evil" }));
		expect(r.retryMessages).toEqual([{ msgId: "m0", delaySeconds: 3600 }]);
		expect(sv.tokenCalls).toHaveLength(0);
	});

	it("acks malformed messages and uploads the rest", async () => {
		const r = await run([{ not: "a string" }, line("2026-10-20T12:00:00.000Z", "x")]);
		expect(r.explicitAcks.sort()).toEqual(["m0", "m1"]);
		expect(sv.objects.size).toBe(1);
	});

	it("falls back to the message time when lines carry no parsable timestamp", async () => {
		await run(["plain text line\n"]);
		expect(sv.uploadCalls[0].body.startTs).toBe("2026-10-20T12:00:00.000Z");
		expect(sv.uploadCalls[0].body.endTs).toBe("2026-10-20T12:00:00.000Z");
	});
});
