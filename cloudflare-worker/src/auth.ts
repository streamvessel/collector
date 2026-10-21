const encoder = new TextEncoder();

/**
 * Compares two strings in time independent of where they differ and of their
 * lengths: both sides are hashed first so timingSafeEqual sees equal-length
 * inputs, and the hash hides the secret's length.
 */
async function constantTimeEqual(a: string, b: string): Promise<boolean> {
	const [ha, hb] = await Promise.all([
		crypto.subtle.digest("SHA-256", encoder.encode(a)),
		crypto.subtle.digest("SHA-256", encoder.encode(b)),
	]);
	return crypto.subtle.timingSafeEqual(ha, hb);
}

interface BasicCredentials {
	user: string;
	password: string;
}

function parseBasicAuth(header: string | null): BasicCredentials | null {
	if (!header) return null;
	const match = /^Basic\s+([A-Za-z0-9+/=]+)\s*$/i.exec(header);
	if (!match) return null;
	let decoded: string;
	try {
		const bytes = Uint8Array.from(atob(match[1]), (c) => c.charCodeAt(0));
		decoded = new TextDecoder().decode(bytes);
	} catch {
		return null;
	}
	const colon = decoded.indexOf(":");
	if (colon < 0) return null;
	return { user: decoded.slice(0, colon), password: decoded.slice(colon + 1) };
}

/**
 * Checks Basic credentials against the configured user (if any) and password.
 * Both comparisons always run, so the response time does not reveal which one
 * failed.
 */
export async function checkBasicAuth(
	header: string | null,
	expectedUser: string | undefined,
	expectedPassword: string,
): Promise<boolean> {
	const creds = parseBasicAuth(header) ?? { user: "", password: "" };
	const [userOk, passwordOk] = await Promise.all([
		expectedUser ? constantTimeEqual(creds.user, expectedUser) : Promise.resolve(true),
		constantTimeEqual(creds.password, expectedPassword),
	]);
	return userOk && passwordOk && header !== null;
}
