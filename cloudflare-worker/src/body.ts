export class BodyError extends Error {
	constructor(
		readonly status: 400 | 413 | 415,
		readonly type: string,
		message: string,
	) {
		super(message);
	}
}

/** Passes bytes through and fails the stream once more than `cap` have gone by. */
function capStream(cap: number, what: string): TransformStream<Uint8Array, Uint8Array> {
	let seen = 0;
	return new TransformStream({
		transform(chunk, controller) {
			seen += chunk.byteLength;
			if (seen > cap) {
				controller.error(new BodyError(413, "content_too_long_exception", `${what} exceeds ${cap} bytes`));
				return;
			}
			controller.enqueue(chunk);
		},
	});
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
	const chunks: Uint8Array[] = [];
	let total = 0;
	const reader = stream.getReader();
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		chunks.push(value);
		total += value.byteLength;
	}
	const out = new Uint8Array(total);
	let offset = 0;
	for (const c of chunks) {
		out.set(c, offset);
		offset += c.byteLength;
	}
	return out;
}

/**
 * Reads the request body as text, enforcing a cap on the bytes as sent and a
 * second cap on the bytes after gzip/deflate Content-Encoding is removed (a
 * small gzip body can expand a thousandfold).
 */
export async function readBody(request: Request, maxBody: number, maxDecompressed: number): Promise<string> {
	const declared = Number(request.headers.get("content-length") ?? "NaN");
	if (Number.isFinite(declared) && declared > maxBody) {
		throw new BodyError(413, "content_too_long_exception", `request body exceeds ${maxBody} bytes`);
	}
	if (!request.body) return "";

	const encoding = (request.headers.get("content-encoding") ?? "identity").trim().toLowerCase();
	let format: "gzip" | "deflate" | null;
	switch (encoding) {
		case "":
		case "identity":
			format = null;
			break;
		case "gzip":
		case "x-gzip":
			format = "gzip";
			break;
		case "deflate":
			format = "deflate";
			break;
		default:
			throw new BodyError(415, "illegal_argument_exception", `unsupported Content-Encoding: ${encoding}`);
	}

	let stream = request.body.pipeThrough(capStream(maxBody, "request body"));
	if (format) {
		stream = stream
			.pipeThrough(new DecompressionStream(format))
			.pipeThrough(capStream(maxDecompressed, "decompressed request body"));
	}

	let bytes: Uint8Array;
	try {
		bytes = await collect(stream);
	} catch (err) {
		if (err instanceof BodyError) throw err;
		throw new BodyError(400, "parse_exception", `cannot decode request body: ${String(err)}`);
	}
	return new TextDecoder().decode(bytes);
}
