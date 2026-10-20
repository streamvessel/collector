import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.toml" },
			miniflare: {
				bindings: {
					SV_SERVER: "https://sv.test",
					SV_CLIENT_ID: "sva_testclient",
					SV_CLIENT_SECRET: "svas_testsecret",
					BULK_PASSWORD: "correct-horse-battery-staple",
				},
			},
		}),
	],
});
