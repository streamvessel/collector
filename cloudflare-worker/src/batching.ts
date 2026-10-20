import { utf8Length } from "./logline";

/**
 * A queue message: NDJSON lines, each ending in "\n", sent with contentType
 * "text" so its size is exactly its UTF-8 length (JSON or v8 serialization
 * would add escaping or per-character overhead we cannot predict).
 */
export type QueueBody = string;

// Cloudflare Queues limits: 128 KB per message; per sendBatch call at most
// 100 messages and 256 KB in total. Stay well under both, since the limits
// count some per-message overhead we do not see.
export const MAX_MESSAGE_BYTES = 120_000;
export const MAX_BATCH_MESSAGES = 100;
export const MAX_BATCH_BYTES = 240_000;
/** Allowance per message for envelope overhead when summing a batch. */
const PER_MESSAGE_OVERHEAD = 256;

/** Packs lines (each at most MAX_MESSAGE_BYTES - 1 bytes) into messages. */
export function packMessages(lines: string[]): QueueBody[] {
	const messages: QueueBody[] = [];
	let current: string[] = [];
	let currentBytes = 0;
	for (const line of lines) {
		const bytes = utf8Length(line) + 1;
		if (bytes > MAX_MESSAGE_BYTES) throw new Error(`line of ${bytes} bytes exceeds the message limit`);
		if (currentBytes + bytes > MAX_MESSAGE_BYTES) {
			messages.push(current.join(""));
			current = [];
			currentBytes = 0;
		}
		current.push(line + "\n");
		currentBytes += bytes;
	}
	if (current.length > 0) messages.push(current.join(""));
	return messages;
}

/** Groups messages into sendBatch calls within the count and size limits. */
export function groupBatches(messages: QueueBody[]): QueueBody[][] {
	const batches: QueueBody[][] = [];
	let current: QueueBody[] = [];
	let currentBytes = 0;
	for (const m of messages) {
		const bytes = utf8Length(m) + PER_MESSAGE_OVERHEAD;
		if (current.length > 0 && (current.length >= MAX_BATCH_MESSAGES || currentBytes + bytes > MAX_BATCH_BYTES)) {
			batches.push(current);
			current = [];
			currentBytes = 0;
		}
		current.push(m);
		currentBytes += bytes;
	}
	if (current.length > 0) batches.push(current);
	return batches;
}

/** Sends lines to the queue; resolves only once every sendBatch has succeeded. */
export async function enqueueLines(queue: Queue<QueueBody>, lines: string[]): Promise<number> {
	const batches = groupBatches(packMessages(lines));
	for (const batch of batches) {
		await queue.sendBatch(batch.map((body) => ({ body, contentType: "text" as const })));
	}
	return batches.length;
}
