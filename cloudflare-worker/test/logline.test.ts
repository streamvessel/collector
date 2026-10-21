import { describe, expect, it } from "vitest";
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
		const line = toLogLine({ time: "yesterday-ish", message: "m" }, undefined, NOW);
		expect(JSON.parse(line)).toEqual({ timestamp: "2026-10-20T12:00:00.000Z", message: "m", attrs: { timestamp_raw: "yesterday-ish" } });
	});

	it("ignores implausible timestamps (durations, YYYYMMDD numbers, far future)", () => {
		for (const time of ["0.0123", 20261020, "2099-01-01T00:00:00Z"]) {
			const parsed = JSON.parse(toLogLine({ time, message: "m" }, undefined, NOW));
			expect(parsed.timestamp, String(time)).toBe("2026-10-20T12:00:00.000Z");
			expect(parsed.attrs.timestamp_raw).toBe(time);
		}
	});

	it("caps a huge level so an oversized record still terminates and fits", () => {
		const line = toLogLine({ level: "E".repeat(100_500), message: "hi" }, undefined, NOW);
		expect(utf8Length(line)).toBeLessThanOrEqual(MAX_LINE_BYTES);
		expect(JSON.parse(line).level).toBe("e".repeat(32));
	});

	it("serializes a non-string message", () => {
		const line = toLogLine({ message: { nested: 1 } }, undefined, NOW);
		expect(JSON.parse(line).message).toBe('{"nested":1}');
	});

	it("cuts an oversized record to MAX_LINE_BYTES, keeping valid UTF-8", () => {
		const line = toLogLine({ message: "é".repeat(200_000), big: "x".repeat(50_000) }, "idx", NOW);
		expect(utf8Length(line)).toBeLessThanOrEqual(MAX_LINE_BYTES);
		const parsed = JSON.parse(line);
		expect(parsed.attrs).toEqual({ truncated: true });
		expect(parsed.message).toMatch(/^é+$/);
	});

	it("cuts a message that grows under JSON escaping", () => {
		const line = toLogLine({ message: "\u0001".repeat(MAX_LINE_BYTES) }, undefined, NOW);
		expect(utf8Length(line)).toBeLessThanOrEqual(MAX_LINE_BYTES);
	});
});
