import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Use the hook's source straight from the workspace, so no build step
    // stands between an edit to the package and this app.
    alias: {
      'use-agentenkit': fileURLToPath(new URL('../../../packages/use-agentenkit/src/index.ts', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    // In development Vite serves the SPA and forwards the API to Go.
    proxy: { '/api': { target: 'http://localhost:8080', changeOrigin: true } },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
