import { env } from "cloudflare:test";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MAX_BATCH_BYTES, MAX_BATCH_MESSAGES, MAX_MESSAGE_BYTES, type QueueBody } from "../src/batching";
import { gzip } from "../src/consumer";
import type { Env } from "../src/env";
import worker, { handleFetch } from "../src/index";

const PASSWORD = "correct-horse-battery-staple";
const encoder = new TextEncoder();

class FakeQueue {
	calls: Array<Array<{ body: QueueBody; contentType?: string }>> = [];
	failOnCall?: number;
	async sendBatch(messages: Iterable<MessageSendRequest<QueueBody>>): Promise<void> {
		const list = [...messages] as Array<{ body: QueueBody; contentType?: string }>;
		if (this.failOnCall === this.calls.length) throw new Error("queue unavailable");
		this.calls.push(list);
	}
	get lines(): string[] {
		return this.calls.flat().flatMap((m) => m.body.split("\n").filter((l) => l !== ""));
	}
}

function makeEnv(overrides: Partial<Env> = {}): { env: Env; queue: FakeQueue } {
	const queue = new FakeQueue();
	return {
		queue,
		env: {
			LOG_QUEUE: queue as unknown as Queue<QueueBody>,
			SV_SERVER: "https://sv.test",
			SV_CLIENT_ID: "sva_ReceiverTest-012345678",
			SV_CLIENT_SECRET: "svas_x",
			BULK_USER: "streamvessel",
			BULK_PASSWORD: PASSWORD,
			...overrides,
		},
	};
}

function basic(user: string, password: string): string {
	return "Basic " + btoa(`${user}:${password}`);
}

function bulkRequest(body: BodyInit, init: { path?: string; headers?: Record<string, string>; auth?: string } = {}) {
	return new Request(`https://worker.test${init.path ?? "/_bulk"}`, {
		method: "POST",
		headers: {
			authorization: init.auth ?? basic("streamvessel", PASSWORD),
			"content-type": "application/x-ndjson",
			...init.headers,
		},
		body,
	});
}

function ndjson(docs: Array<Record<string, unknown>>, action: "index" | "create" = "index"): string {
	return docs.map((d) => `{"${action}":{}}\n${JSON.stringify(d)}\n`).join("");
}

describe("authentication", () => {
	it("rejects a missing or wrong password with 401", async () => {
		const { env } = makeEnv();
		const none = await handleFetch(new Request("https://worker.test/"), env);
		expect(none.status).toBe(401);
		expect(none.headers.get("www-authenticate")).toMatch(/^Basic/);
		const wrong = await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("streamvessel", "nope") } }), env);
		expect(wrong.status).toBe(401);
		const wrongUser = await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("admin", PASSWORD) } }), env);
		expect(wrongUser.status).toBe(401);
	});

	it("accepts any user when BULK_USER is unset", async () => {
		const { env } = makeEnv({ BULK_USER: undefined });
		const res = await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("whoever", PASSWORD) } }), env);
		expect(res.status).toBe(200);
	});

	it("compares in constant time via crypto.subtle.timingSafeEqual", async () => {
		const spy = vi.spyOn(crypto.subtle, "timingSafeEqual");
		const { env } = makeEnv();
		await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("streamvessel", "x") } }), env);
		// user and password, each over equal-length digests
		expect(spy).toHaveBeenCalledTimes(2);
		for (const [a, b] of spy.mock.calls) expect((a as ArrayBuffer).byteLength).toBe((b as ArrayBuffer).byteLength);
		spy.mockRestore();
	});

	it("fails closed when BULK_PASSWORD is unset, short or a placeholder", async () => {
		for (const pw of ["", "short", "REPLACE_ME", "REPLACE_ME-padded-to-be-long-enough", "change-me-to-a-long-random-value"]) {
			const { env } = makeEnv({ BULK_PASSWORD: pw });
			const res = await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("streamvessel", pw) } }), env);
			expect(res.status).toBe(500);
		}
	});

	it("rate-limits failed attempts and all requests per IP", async () => {
		const outcomes: string[] = [];
		const limiter = (name: string, allow: boolean): RateLimit => ({
			limit: async ({ key }) => {
				outcomes.push(`${name}:${key}`);
				return { success: allow };
			},
		});
		const headers = { "cf-connecting-ip": "203.0.113.9", authorization: basic("streamvessel", "bad") };

		const fail = makeEnv({ AUTH_FAIL_LIMITER: limiter("auth", false), REQUEST_LIMITER: limiter("req", true) });
		const res = await handleFetch(new Request("https://worker.test/", { headers }), fail.env);
		expect(res.status).toBe(429);
		expect(outcomes).toEqual(["req:203.0.113.9", "auth:203.0.113.9"]);

		const all = makeEnv({ REQUEST_LIMITER: limiter("req", false) });
		const res2 = await handleFetch(new Request("https://worker.test/", { headers }), all.env);
		expect(res2.status).toBe(429);
		expect(res2.headers.get("retry-after")).toBe("10");
	});
});

describe("GET /", () => {
	it("answers an OpenSearch version document", async () => {
		const { env } = makeEnv();
		const res = await handleFetch(new Request("https://worker.test/", { headers: { authorization: basic("streamvessel", PASSWORD) } }), env);
		expect(res.status).toBe(200);
		const body = (await res.json()) as { version: { distribution: string; number: string }; tagline: string };
		expect(body.version.distribution).toBe("opensearch");
		expect(body.version.number).toMatch(/^\d+\.\d+\.\d+$/);
		expect(body.tagline).toContain("OpenSearch");
	});

	it("answers HEAD / with 200 and unknown paths with an OpenSearch 404", async () => {
		const { env } = makeEnv();
		const auth = { authorization: basic("streamvessel", PASSWORD) };
		expect((await handleFetch(new Request("https://worker.test/", { method: "HEAD", headers: auth }), env)).status).toBe(200);
		const res = await handleFetch(new Request("https://worker.test/_cat/indices", { headers: auth }), env);
		expect(res.status).toBe(404);
		expect(((await res.json()) as { error: { type: string } }).error.type).toBe("resource_not_found_exception");
	});
});

describe("POST _bulk", () => {
	it("maps documents to NDJSON lines, queues them, then answers a bulk response", async () => {
		const { env, queue } = makeEnv();
		const body =
			`{"index":{"_index":"app-logs","_id":"a1"}}\n` +
			JSON.stringify({ "@timestamp": "2026-10-20T12:00:00.123456Z", message: "GET / 200", level: "INFO", app: "shop", component: "web" }) +
			"\n" +
			`{"create":{}}\n` +
			JSON.stringify({ timestamp: 1792497600, msg: "no level here" }) +
			"\n";
		const res = await handleFetch(bulkRequest(body, { path: "/fallback-index/_bulk" }), env);
		expect(res.status).toBe(200);
		const out = (await res.json()) as { errors: boolean; items: Array<Record<string, { status: number; _index: string; _id: string; result: string }>> };
		expect(out.errors).toBe(false);
		expect(out.items).toHaveLength(2);
		expect(out.items[0].index).toMatchObject({ status: 201, _index: "app-logs", _id: "a1", result: "created" });
		expect(out.items[1].create).toMatchObject({ status: 201, _index: "fallback-index" });

		expect(queue.calls).toHaveLength(1);
		expect(queue.calls[0][0].contentType).toBe("text");
		const [l1, l2] = queue.lines.map((l) => JSON.parse(l));
		expect(l1).toEqual({
			timestamp: "2026-10-20T12:00:00.123Z",
			level: "info",
			message: "GET / 200",
			attrs: { app: "shop", component: "web", "opensearch.index": "app-logs" },
		});
		expect(l2).toEqual({
			timestamp: "2026-10-20T12:00:00.000Z",
			message: "no level here",
			attrs: { "opensearch.index": "fallback-index" },
		});
		// the line starts with the timestamp, which the consumer relies on
		expect(queue.lines[0].startsWith('{"timestamp":"')).toBe(true);
	});

	it("does not answer before every sendBatch has resolved", async () => {
		const { env } = makeEnv();
		let release!: () => void;
		const gate = new Promise<void>((r) => (release = r));
		let sent = false;
		env.LOG_QUEUE = {
			sendBatch: async () => {
				await gate;
				sent = true;
			},
		} as unknown as Queue<QueueBody>;
		const pending = handleFetch(bulkRequest(ndjson([{ message: "x" }])), env);
		let settled = false;
		void pending.then(() => (settled = true));
		await new Promise((r) => setTimeout(r, 20));
		expect(settled).toBe(false);
		release();
		const res = await pending;
		expect(sent).toBe(true);
		expect(res.status).toBe(200);
	});

	it("answers 503 and no bulk response when queueing fails part-way", async () => {
		const { env, queue } = makeEnv();
		queue.failOnCall = 1;
		const big = "x".repeat(50_000);
		const res = await handleFetch(bulkRequest(ndjson(Array.from({ length: 20 }, () => ({ message: big })))), env);
		expect(res.status).toBe(503);
		expect(res.headers.get("retry-after")).toBe("5");
		expect(queue.calls).toHaveLength(1);
	});

	it("splits a large body into messages < 128 KB over several sendBatch calls within 100 messages / 256 KB", async () => {
		const { env, queue } = makeEnv();
		const docs = Array.from({ length: 3000 }, (_, i) => ({ message: `line ${i} ` + "é\"".repeat(200 + (i % 300)) }));
		const res = await handleFetch(bulkRequest(ndjson(docs)), env);
		expect(res.status).toBe(200);
		expect(queue.calls.length).toBeGreaterThan(1);
		for (const call of queue.calls) {
			expect(call.length).toBeLessThanOrEqual(MAX_BATCH_MESSAGES);
			const sizes = call.map((m) => encoder.encode(m.body).byteLength);
			for (const s of sizes) expect(s).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
			expect(sizes.reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(MAX_BATCH_BYTES);
		}
		const lines = queue.lines.map((l) => JSON.parse(l).message as string);
		expect(lines).toHaveLength(3000);
		expect(lines[2999].startsWith("line 2999 ")).toBe(true);
	});

	it("accepts gzip bodies", async () => {
		const { env, queue } = makeEnv();
		const res = await handleFetch(bulkRequest(await gzip(ndjson([{ message: "zipped" }])), { headers: { "content-encoding": "gzip" } }), env);
		expect(res.status).toBe(200);
		expect(JSON.parse(queue.lines[0]).message).toBe("zipped");
	});

	it("caps the body as sent (413) by Content-Length and by streamed bytes", async () => {
		const { env, queue } = makeEnv({ MAX_BODY_BYTES: "1000" });
		// workerd sets no Content-Length on a Request built from a string, so declare it
		// explicitly; the tiny body proves the declared length alone is refused.
		const declared = await handleFetch(bulkRequest("{}", { headers: { "content-length": "5000" } }), env);
		expect(declared.status).toBe(413);

		// no Content-Length: a stream is counted as it arrives
		const stream = new Blob([ndjson([{ message: "y".repeat(2000) }])]).stream();
		const res2 = await handleFetch(bulkRequest(stream), env);
		expect(res2.status).toBe(413);
		expect(queue.calls).toHaveLength(0);
	});

	it("caps the decompressed size (413) for a gzip bomb", async () => {
		const { env, queue } = makeEnv({ MAX_DECOMPRESSED_BYTES: "100000" });
		const bomb = await gzip(ndjson([{ message: "a".repeat(1_000_000) }]));
		expect(bomb.byteLength).toBeLessThan(10_000);
		const res = await handleFetch(bulkRequest(bomb, { headers: { "content-encoding": "gzip" } }), env);
		expect(res.status).toBe(413);
		expect(queue.calls).toHaveLength(0);
	});

	it("rejects broken gzip (400) and unknown encodings (415)", async () => {
		const { env } = makeEnv();
		expect((await handleFetch(bulkRequest("not gzip", { headers: { "content-encoding": "gzip" } }), env)).status).toBe(400);
		expect((await handleFetch(bulkRequest("x", { headers: { "content-encoding": "br" } }), env)).status).toBe(415);
	});

	it("fails the whole request on a malformed action line or an empty body", async () => {
		const { env, queue } = makeEnv();
		for (const body of ["garbage\n{}\n", '{"index":{}}\n', '{"frobnicate":{}}\n{}\n', "\n\n"]) {
			const res = await handleFetch(bulkRequest(body), env);
			expect(res.status, body).toBe(400);
		}
		expect(queue.calls).toHaveLength(0);
	});

	it("reports per-item errors for bad documents and unsupported ops", async () => {
		const { env, queue } = makeEnv();
		const body = `{"index":{}}\nnot json\n{"delete":{"_id":"1"}}\n{"update":{"_id":"2"}}\n{"doc":{}}\n{"index":{}}\n{"message":"ok"}\n`;
		const res = await handleFetch(bulkRequest(body), env);
		const out = (await res.json()) as { errors: boolean; items: Array<Record<string, { status: number }>> };
		expect(out.errors).toBe(true);
		expect(out.items.map((i) => Object.values(i)[0].status)).toEqual([400, 400, 400, 201]);
		expect(queue.lines).toHaveLength(1);
	});

	it("rejects GET on _bulk with 405", async () => {
		const { env } = makeEnv();
		const res = await handleFetch(new Request("https://worker.test/_bulk", { headers: { authorization: basic("streamvessel", PASSWORD) } }), env);
		expect(res.status).toBe(405);
	});
});

describe("against the real Queue binding (Miniflare)", () => {
	beforeEach(() => {
		// The consumer may fire during these tests; keep it off the network.
		vi.stubGlobal("fetch", async () => new Response(null, { status: 503 }));
		return () => vi.unstubAllGlobals();
	});

	it("queues a large body without exceeding Cloudflare's per-message and per-batch limits", async () => {
		const docs = Array.from({ length: 2500 }, (_, i) => ({ message: `${i} ` + "z".repeat(400) }));
		const res = await worker.fetch(bulkRequest(ndjson(docs)), env as unknown as Env);
		expect(res.status).toBe(200);
		expect(((await res.json()) as { errors: boolean }).errors).toBe(false);
	});
});
