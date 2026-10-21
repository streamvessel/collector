import { describe, expect, it } from "vitest";
import { MAX_BATCH_BYTES, MAX_BATCH_MESSAGES, MAX_MESSAGE_BYTES, groupBatches, packMessages } from "../src/batching";
import { utf8Length } from "../src/logline";

describe("packing", () => {
	it("keeps every message and batch under the queue limits and loses no line", () => {
		const lines = Array.from({ length: 5000 }, (_, i) => `{"i":${i},"p":"${"ü".repeat(i % 2000)}"}`);
		const messages = packMessages(lines);
		for (const m of messages) expect(utf8Length(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
		expect(messages.join("")).toBe(lines.map((l) => l + "\n").join(""));
		for (const b of groupBatches(messages)) {
			expect(b.length).toBeLessThanOrEqual(MAX_BATCH_MESSAGES);
			expect(b.reduce((n, m) => n + utf8Length(m), 0)).toBeLessThanOrEqual(MAX_BATCH_BYTES);
		}
	});
});
