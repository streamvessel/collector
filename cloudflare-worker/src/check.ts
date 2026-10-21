import { passwordProblem, uploadConfigProblems, type Env } from "./env";
import { json } from "./opensearch";
import { PermanentError, requestToken } from "./svclient";

interface CheckResult {
	name: string;
	ok: boolean;
	detail: string;
}

/**
 * `GET /_check`: tells a customer right after deploying whether the Worker
 * can work, without waiting for logs. It checks the settings, then asks
 * StreamVessel for a token, which proves SV_SERVER, SV_CLIENT_ID and
 * SV_CLIENT_SECRET together. It uploads nothing.
 */
export async function handleCheck(env: Env): Promise<Response> {
	const checks: CheckResult[] = [];
	const add = (name: string, problem: string | null, okDetail: string) =>
		checks.push({ name, ok: problem === null, detail: problem ?? okDetail });

	add("COLLECTOR_PASSWORD", passwordProblem(env), "set");
	add("queue", env.LOG_QUEUE ? null : "the LOG_QUEUE binding is missing", "bound");
	const problems = uploadConfigProblems(env);
	add("settings", problems.length > 0 ? problems.join("; ") : null, "SV_SERVER, SV_CLIENT_ID, SV_CLIENT_SECRET, SV_INSTANCE look valid");

	if (problems.length === 0) {
		try {
			await requestToken({ server: env.SV_SERVER, clientId: env.SV_CLIENT_ID, clientSecret: env.SV_CLIENT_SECRET });
			add("streamvessel", null, `got a token from ${env.SV_SERVER}`);
		} catch (err) {
			const detail = err instanceof Error ? err.message : String(err);
			const hint =
				err instanceof PermanentError && err.code === "invalid_client"
					? " (check SV_CLIENT_ID and SV_CLIENT_SECRET, and that the agent is not revoked)"
					: err instanceof PermanentError && err.code === "not_found"
						? " (log agents are not enabled on this StreamVessel, or SV_SERVER is wrong)"
						: "";
			add("streamvessel", detail + hint, "");
		}
	} else {
		add("streamvessel", "skipped until the settings above are fixed", "");
	}

	const ok = checks.every((c) => c.ok);
	return json({ ok, checks }, ok ? 200 : 503);
}
