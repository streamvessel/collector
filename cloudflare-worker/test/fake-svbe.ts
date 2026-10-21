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
	body: { instance: string; startTs: string; endTs: string; sha256: string };
}

/**
 * sv-be's ProviderCapabilities: AWS signs If-None-Match: *, so a second PUT
 * to the same key answers 412; other providers sign no extra header and
 * overwrite.
 */
export type Provider = "aws" | "other";

type Override = (req: Request) => Response | Promise<Response> | undefined;

export class FakeSvBe {
	readonly server = "https://sv.test";
	readonly bucket = "https://bucket.test";
	clientId = "sva_testclient";
	clientSecret = "svas_testsecret";
	provider: Provider = "aws";

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
		const body = new TextDecoder().decode(await req.clone().arrayBuffer());
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
		if (!/^[0-9a-f]{64}$/.test(body.sha256) || Number.isNaN(Date.parse(body.startTs)) || Number.isNaN(Date.parse(body.endTs))) {
			return Response.json({ error: "invalid_request", error_description: "bad sha256 or timestamps" }, { status: 400 });
		}
		// What the SDK signed besides Host: nothing, or If-None-Match on AWS.
		const headers: Record<string, string> = this.provider === "aws" ? { "if-none-match": "*" } : {};
		// Same batch, same key (the key carries start, end and sha8), as on sv-be.
		const key = `logs/agent1/${body.instance}/${body.startTs.slice(0, 10).replaceAll("-", "")}/${body.startTs}_${body.endTs}_${body.sha256.slice(0, 8)}.log.gz`;
		return Response.json({
			url: `${this.bucket}/${key}?X-Amz-Signature=fake`,
			headers,
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
