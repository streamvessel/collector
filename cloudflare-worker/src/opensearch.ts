/** OpenSearch-shaped responses, so stock clients (App Platform, Fluent Bit, Vector) accept them. */

const VERSION_NUMBER = "2.11.0";

/** A request the receiver refuses as a whole, answered as an OpenSearch error. */
export class RequestError extends Error {
	constructor(
		readonly status: number,
		readonly type: string,
		message: string,
	) {
		super(message);
	}
}

export function json(body: unknown, status = 200, headers: HeadersInit = {}): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json; charset=UTF-8", ...headers },
	});
}

export function errorResponse(status: number, type: string, reason: string, headers: HeadersInit = {}): Response {
	return json({ error: { root_cause: [{ type, reason }], type, reason }, status }, status, headers);
}

export function versionDocument(): unknown {
	return {
		name: "streamvessel-log-worker",
		cluster_name: "streamvessel",
		cluster_uuid: "streamvessel-log-worker",
		version: {
			distribution: "opensearch",
			number: VERSION_NUMBER,
			build_type: "cloudflare-worker",
			build_hash: "unknown",
			build_date: "2023-10-13T00:00:00.000000Z",
			build_snapshot: false,
			lucene_version: "9.7.0",
			minimum_wire_compatibility_version: "7.10.0",
			minimum_index_compatibility_version: "7.0.0",
		},
		tagline: "The OpenSearch Project: https://opensearch.org/",
	};
}
