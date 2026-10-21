import { uploadConfigProblems, type Env } from "./env";
import { json } from "./opensearch";
import { PermanentError, requestToken } from "./svclient";

interface CheckResult {
	name: string;
	ok: boolean;
	detail: string;
}

const HINTS: Record<string, string> = {
	invalid_client: " (check SV_CLIENT_ID and SV_CLIENT_SECRET, and that the agent is not revoked)",
	not_found: " (log agents are not enabled on this StreamVessel, or SV_SERVER is wrong)",
};

/**
 * `GET /_check`: tells a customer right after deploying whether the Worker
 * can work, without waiting for logs. It checks the settings, then asks
 * StreamVessel for a token, which proves SV_SERVER, SV_CLIENT_ID and
 * SV_CLIENT_SECRET together. It uploads nothing. COLLECTOR_PASSWORD needs no
 * row: the request only gets here after the password has been checked.
 */
export async function handleCheck(env: Env): Promise<Response> {
	const checks: CheckResult[] = [];

	checks.push(
		env.LOG_QUEUE
			? { name: "queue", ok: true, detail: "bound" }
			: { name: "queue", ok: false, detail: "the LOG_QUEUE binding is missing" },
	);

	const problems = uploadConfigProblems(env);
	checks.push(
		problems.length === 0
			? { name: "settings", ok: true, detail: "SV_SERVER, SV_CLIENT_ID, SV_CLIENT_SECRET, SV_INSTANCE look valid" }
			: { name: "settings", ok: false, detail: problems.join("; ") },
	);

	if (problems.length > 0) {
		checks.push({ name: "streamvessel", ok: false, detail: "skipped until the settings above are fixed" });
	} else {
		try {
			await requestToken({ server: env.SV_SERVER, clientId: env.SV_CLIENT_ID, clientSecret: env.SV_CLIENT_SECRET });
			checks.push({ name: "streamvessel", ok: true, detail: `got a token from ${env.SV_SERVER}` });
		} catch (err) {
			const detail = err instanceof Error ? err.message : String(err);
			const hint = err instanceof PermanentError ? (HINTS[err.code] ?? "") : "";
			checks.push({ name: "streamvessel", ok: false, detail: detail + hint });
		}
	}

	const ok = checks.every((c) => c.ok);
	return json({ ok, checks }, ok ? 200 : 503);
}
