import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

// One page for the four tool examples (web-tools-ts, web-tools-go,
// sandbox-tools-ts, sandbox-tools-go). Each server serves this build and
// says, at /api/info, what it is and what to try.
export default defineConfig({
  plugins: [react()],
  resolve: {
    // Use the hook's source straight from the workspace, so no build step
    // stands between an edit to the package and this page.
    alias: {
      'use-agentenkit': fileURLToPath(new URL('../../packages/use-agentenkit/src/index.ts', import.meta.url)),
    },
  },
  server: {
    port: 5174,
    // In development Vite serves the page and forwards the API to whichever
    // example runs on API (default: web-tools-ts).
    proxy: { '/api': { target: process.env.API ?? 'http://localhost:3101', changeOrigin: true } },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
