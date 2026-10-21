/** One JSON line per event, so Workers Logs can filter and alert on `event`. */
export function log(level: "info" | "warn" | "error", event: string, fields: Record<string, unknown> = {}): void {
	const entry = JSON.stringify({ level, event, ...fields });
	if (level === "error") console.error(entry);
	else if (level === "warn") console.warn(entry);
	else console.log(entry);
}
