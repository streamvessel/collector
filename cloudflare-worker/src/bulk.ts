import { toLogLine } from "./logline";

export class BulkRequestError extends Error {
	constructor(
		readonly type: string,
		message: string,
	) {
		super(message);
	}
}

type Op = "index" | "create" | "update" | "delete";
const OPS: readonly Op[] = ["index", "create", "update", "delete"];

export interface BulkItemResult {
	op: Op;
	index: string | undefined;
	id: string;
	/** Present when the document is accepted. */
	line?: string;
	error?: { type: string; reason: string };
}

function isObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * Parses a `_bulk` NDJSON body: action lines, each followed by a document
 * line except for `delete`. A malformed action line fails the whole request,
 * as in OpenSearch; a malformed document fails only its item.
 */
export function parseBulk(body: string, pathIndex: string | undefined, receivedAt: number): BulkItemResult[] {
	const lines = body.split("\n").map((l) => (l.endsWith("\r") ? l.slice(0, -1) : l));
	const items: BulkItemResult[] = [];
	let i = 0;
	const next = (): string | undefined => {
		while (i < lines.length && lines[i].trim() === "") i++;
		return i < lines.length ? lines[i++] : undefined;
	};

	for (let actionLine = next(); actionLine !== undefined; actionLine = next()) {
		let action: unknown;
		try {
			action = JSON.parse(actionLine);
		} catch {
			throw new BulkRequestError("illegal_argument_exception", "Malformed action/metadata line, expected a JSON object");
		}
		const keys = isObject(action) ? Object.keys(action) : [];
		const op = keys.length === 1 ? OPS.find((o) => o === keys[0]) : undefined;
		if (!op || !isObject(action)) {
			throw new BulkRequestError(
				"illegal_argument_exception",
				`Malformed action/metadata line, expected one of [${OPS.join(", ")}]`,
			);
		}
		const meta = isObject(action[op]) ? (action[op] as Record<string, unknown>) : {};
		const index = typeof meta._index === "string" ? meta._index : pathIndex;
		const id = typeof meta._id === "string" || typeof meta._id === "number" ? String(meta._id) : crypto.randomUUID();

		if (op === "delete") {
			items.push({ op, index, id, error: { type: "illegal_argument_exception", reason: "delete is not supported" } });
			continue;
		}
		const docLine = next();
		if (docLine === undefined) {
			throw new BulkRequestError("action_request_validation_exception", `Validation Failed: 1: ${op} is missing a document line;`);
		}
		if (op === "update") {
			items.push({ op, index, id, error: { type: "illegal_argument_exception", reason: "update is not supported" } });
			continue;
		}
		let doc: unknown;
		try {
			doc = JSON.parse(docLine);
		} catch {
			items.push({ op, index, id, error: { type: "mapper_parsing_exception", reason: "failed to parse document" } });
			continue;
		}
		if (!isObject(doc)) {
			items.push({ op, index, id, error: { type: "mapper_parsing_exception", reason: "document is not a JSON object" } });
			continue;
		}
		items.push({ op, index, id, line: toLogLine(doc, index, receivedAt).line });
	}
	if (items.length === 0) {
		throw new BulkRequestError("action_request_validation_exception", "Validation Failed: 1: no requests added;");
	}
	return items;
}

export function bulkResponse(items: BulkItemResult[], tookMs: number): unknown {
	let seqNo = 0;
	return {
		took: tookMs,
		errors: items.some((it) => it.error),
		items: items.map((it) => {
			const base = { _index: it.index ?? "logs", _id: it.id };
			if (it.error) {
				return { [it.op]: { ...base, status: 400, error: it.error } };
			}
			return {
				[it.op]: {
					...base,
					_version: 1,
					result: "created",
					_shards: { total: 1, successful: 1, failed: 0 },
					_seq_no: seqNo++,
					_primary_term: 1,
					status: 201,
				},
			};
		}),
	};
}
