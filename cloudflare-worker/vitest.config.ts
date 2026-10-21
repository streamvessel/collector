import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.toml" },
			miniflare: {
				bindings: {
					SV_SERVER: "https://sv.test",
					SV_CLIENT_ID: "sva_TestClient-0123456789_",
					SV_CLIENT_SECRET: "svas_testsecret",
					COLLECTOR_PASSWORD: "correct-horse-battery-staple",
				},
			},
		}),
	],
});
