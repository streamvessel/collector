import { describe, expect, it } from "vitest";
import { MAX_BATCH_BYTES, MAX_MESSAGE_BYTES, groupBatches, packMessages } from "../src/batching";
import { MAX_LINE_BYTES, parseTimestamp, toLogLine, utf8Length } from "../src/logline";

const NOW = Date.parse("2026-10-20T12:00:00Z");

describe("toLogLine", () => {
	it("reads epoch seconds, ms, µs and ns", () => {
		const iso = "2026-10-20T12:00:00.000Z";
		for (const v of [1792497600, 1792497600000, 1792497600000000, 1792497600000000000, "1792497600"]) {
			expect(new Date(parseTimestamp(v)!).toISOString(), String(v)).toBe(iso);
		}
	});

	it("keeps an unparsable timestamp in attrs and uses the receive time", () => {
		const { line, ts } = toLogLine({ time: "yesterday-ish", message: "m" }, undefined, NOW);
		expect(ts).toBe(NOW);
		expect(JSON.parse(line)).toEqual({ timestamp: "2026-10-20T12:00:00.000Z", message: "m", attrs: { timestamp_raw: "yesterday-ish" } });
	});

	it("serializes a non-string message", () => {
		const { line } = toLogLine({ message: { nested: 1 } }, undefined, NOW);
		expect(JSON.parse(line).message).toBe('{"nested":1}');
	});

	it("cuts an oversized record to MAX_LINE_BYTES, keeping valid UTF-8", () => {
		const { line } = toLogLine({ message: "é".repeat(200_000), big: "x".repeat(50_000) }, "idx", NOW);
		expect(utf8Length(line)).toBeLessThanOrEqual(MAX_LINE_BYTES);
		const parsed = JSON.parse(line);
		expect(parsed.attrs).toEqual({ truncated: true });
		expect(parsed.message).toMatch(/^é+$/);
	});

	it("cuts a message that grows under JSON escaping", () => {
		const { line } = toLogLine({ message: "\u0001".repeat(MAX_LINE_BYTES) }, undefined, NOW);
		expect(utf8Length(line)).toBeLessThanOrEqual(MAX_LINE_BYTES);
	});
});

describe("packing", () => {
	it("keeps every message and batch under the queue limits and loses no line", () => {
		const lines = Array.from({ length: 5000 }, (_, i) => `{"i":${i},"p":"${"ü".repeat(i % 2000)}"}`);
		const messages = packMessages(lines);
		for (const m of messages) expect(utf8Length(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
		expect(messages.join("")).toBe(lines.map((l) => l + "\n").join(""));
		for (const b of groupBatches(messages)) {
			expect(b.length).toBeLessThanOrEqual(100);
			expect(b.reduce((n, m) => n + utf8Length(m), 0)).toBeLessThanOrEqual(MAX_BATCH_BYTES);
		}
	});
});
