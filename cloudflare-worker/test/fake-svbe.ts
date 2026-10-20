/**
 * A fake sv-be (token + uploads routes, per streamvessel/sv-be#385) and a fake
 * bucket that checks the PUT the way S3 would check a presigned request.
 */

export interface StoredObject {
	key: string;
	body: Uint8Array;
	headers: Record<string, string>;
}

export interface UploadCall {
	auth: string | null;
	body: { instance: string; startTs: string; endTs: string; size: number; sha256: string };
}

type Override = (req: Request) => Response | Promise<Response> | undefined;

async function sha256Base64(data: Uint8Array): Promise<string> {
	const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", data));
	return btoa(String.fromCharCode(...digest));
}

export class FakeSvBe {
	readonly server = "https://sv.test";
	readonly bucket = "https://bucket.test";
	clientId = "sva_testclient";
	clientSecret = "svas_testsecret";

	tokenCalls: Array<{ auth: string | null; body: string }> = [];
	uploadCalls: UploadCall[] = [];
	putCalls: Array<{ url: string; headers: Record<string, string> }> = [];
	objects = new Map<string, StoredObject>();
	issuedTokens = new Set<string>();

	/** Answers instead of the default handler when it returns a Response. */
	onToken?: Override;
	onUpload?: Override;
	onPut?: Override;

	private seq = 0;

	fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
		const req = new Request(input, init);
		const url = new URL(req.url);
		if (url.origin === this.server && url.pathname === "/be/ingest/oauth/token" && req.method === "POST") {
			return this.token(req);
		}
		if (url.origin === this.server && url.pathname === "/be/ingest/uploads" && req.method === "POST") {
			return this.upload(req);
		}
		if (url.origin === this.bucket && req.method === "PUT") return this.put(req);
		return new Response("unexpected request " + req.method + " " + req.url, { status: 599 });
	};

	private async token(req: Request): Promise<Response> {
		const body = await req.clone().text();
		this.tokenCalls.push({ auth: req.headers.get("authorization"), body });
		const custom = await this.onToken?.(req);
		if (custom) return custom;
		const expected = "Basic " + btoa(`${this.clientId}:${this.clientSecret}`);
		if (req.headers.get("authorization") !== expected || body !== "grant_type=client_credentials") {
			return Response.json({ error: "invalid_client" }, { status: 401 });
		}
		const token = `svla_${++this.seq}`;
		this.issuedTokens.add(token);
		return Response.json({ access_token: token, token_type: "Bearer", expires_in: 900 });
	}

	private async upload(req: Request): Promise<Response> {
		const body = (await req.clone().json()) as UploadCall["body"];
		this.uploadCalls.push({ auth: req.headers.get("authorization"), body });
		const custom = await this.onUpload?.(req);
		if (custom) return custom;
		const token = req.headers.get("authorization")?.replace(/^Bearer /, "");
		if (!token || !this.issuedTokens.has(token)) return Response.json({ error: "invalid_token" }, { status: 401 });
		const hexToB64 = btoa(String.fromCharCode(...body.sha256.match(/../g)!.map((h) => parseInt(h, 16))));
		const key = `logs/agent1/${body.instance}/${body.startTs.slice(0, 10).replaceAll("-", "")}/${body.sha256.slice(0, 8)}-${++this.seq}.log.gz`;
		return Response.json({
			url: `${this.bucket}/${key}?X-Amz-Signature=fake`,
			headers: {
				"Content-Length": String(body.size),
				"Content-Type": "application/x-ndjson",
				"x-amz-checksum-sha256": hexToB64,
				"If-None-Match": "*",
			},
			key,
			expiresAt: new Date(Date.now() + 300_000).toISOString(),
		});
	}

	private async put(req: Request): Promise<Response> {
		const headers: Record<string, string> = {};
		for (const [k, v] of req.headers) headers[k] = v;
		this.putCalls.push({ url: req.url, headers });
		const custom = await this.onPut?.(req);
		if (custom) return custom;
		if (req.headers.has("authorization")) return new Response("<Error><Code>InvalidArgument</Code></Error>", { status: 400 });
		const data = new Uint8Array(await req.arrayBuffer());
		if (headers["x-amz-checksum-sha256"] !== (await sha256Base64(data))) {
			return new Response("<Error><Code>BadDigest</Code></Error>", { status: 400 });
		}
		const key = new URL(req.url).pathname.slice(1);
		if (this.objects.has(key) && headers["if-none-match"] === "*") return new Response(null, { status: 412 });
		this.objects.set(key, { key, body: data, headers });
		return new Response(null, { status: 200 });
	}
}

export async function gunzipText(data: Uint8Array): Promise<string> {
	const stream = new Blob([data]).stream().pipeThrough(new DecompressionStream("gzip"));
	return new Response(stream).text();
}
