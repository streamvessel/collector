import { RequestError } from "./opensearch";

const FORMATS = new Map<string, "gzip" | "deflate" | null>([
	["", null],
	["identity", null],
	["gzip", "gzip"],
	["x-gzip", "gzip"],
	["deflate", "deflate"],
]);

/** Passes bytes through and fails the stream once more than `cap` have gone by. */
function capStream(cap: number, what: string): TransformStream<Uint8Array, Uint8Array> {
	let seen = 0;
	return new TransformStream({
		transform(chunk, controller) {
			seen += chunk.byteLength;
			if (seen > cap) {
				controller.error(new RequestError(413, "content_too_long_exception", `${what} exceeds ${cap} bytes`));
				return;
			}
			controller.enqueue(chunk);
		},
	});
}

/**
 * Reads the request body as text, enforcing a cap on the bytes as sent and a
 * second cap on the bytes after gzip/deflate Content-Encoding is removed (a
 * small gzip body can expand a thousandfold).
 */
export async function readBody(request: Request, maxBody: number, maxDecompressed: number): Promise<string> {
	const declared = Number(request.headers.get("content-length") ?? "NaN");
	if (Number.isFinite(declared) && declared > maxBody) {
		throw new RequestError(413, "content_too_long_exception", `request body exceeds ${maxBody} bytes`);
	}
	if (!request.body) return "";

	const encoding = (request.headers.get("content-encoding") ?? "identity").trim().toLowerCase();
	if (!FORMATS.has(encoding)) {
		throw new RequestError(415, "illegal_argument_exception", `unsupported Content-Encoding: ${encoding}`);
	}
	const format = FORMATS.get(encoding);

	let stream = request.body.pipeThrough(capStream(maxBody, "request body"));
	if (format) {
		stream = stream
			.pipeThrough(new DecompressionStream(format))
			.pipeThrough(capStream(maxDecompressed, "decompressed request body"));
	}

	try {
		return await new Response(stream).text();
	} catch (err) {
		if (err instanceof RequestError) throw err;
		throw new RequestError(400, "parse_exception", `cannot decode request body: ${String(err)}`);
	}
}
