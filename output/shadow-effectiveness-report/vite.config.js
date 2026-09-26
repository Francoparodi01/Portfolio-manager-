import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

const localThreadId = [process.env.CODEX_SESSION_ID, process.env.CODEX_THREAD_ID].find((value) =>
  /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/iu.test(value ?? ""),
);

const snapshotIntegrityPlugin = {
  name: "data-app-snapshot-integrity",
  transformIndexHtml() {
    return [
      {
        tag: "meta",
        attrs: {
          name: "data-app-snapshot-sha256",
          content: createHash("sha256")
            .update(readFileSync(new URL("./src/data.json", import.meta.url)))
            .digest("hex"),
        },
        injectTo: "head",
      },
    ];
  },
};

export default defineConfig(({ command, isSsrBuild }) => ({
  plugins: isSsrBuild
    ? []
    : [
        viteSingleFile(),
        snapshotIntegrityPlugin,
        {
          name: "data-app-local-task",
          transformIndexHtml: () =>
            localThreadId
              ? [
                  {
                    tag: "meta",
                    attrs: {
                      name: "data-app-local-thread",
                      content: localThreadId,
                    },
                    injectTo: "head",
                  },
                ]
              : [],
        },
      ],
  server: {
    allowedHosts: ["terminal.local"],
    ...(process.env.CODEX_SANDBOX === "seatbelt" ? { watch: { useFsEvents: false, usePolling: true } } : {}),
  },
  define: {
    __DATA_APP_PROJECT_ROOT__: JSON.stringify(command === "serve" ? process.cwd() : ""),
  },
  build: isSsrBuild ? { rollupOptions: { output: { entryFileNames: "index.js" } } } : {},
}));
