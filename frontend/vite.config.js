import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite replaces Create React App as the build tool / dev server.
//
// The dev server proxies the same paths CRA's "proxy" package.json field used to:
//   - /api  -> backend REST endpoints
//   - /ws   -> the SockJS/STOMP endpoint (ws:true forwards the WebSocket upgrade as
//              well as SockJS's HTTP fallback requests)
// In the built Docker image there is no dev server; nginx does this proxying instead
// (see frontend/nginx.conf), so this block only matters for `npm start` / `npm run dev`.
//
// The dev proxy targets VITE_API_URL when it's set (so a dev can point the dev server at
// a remote backend), otherwise the local backend at http://localhost:8080.
//
// PORT is honoured so external tooling can assign a port; it defaults to 3000 to match
// the old CRA dev server. The production bundle is emitted to build/ (rather than Vite's
// default dist/) so the Dockerfile, .gitignore, and .dockerignore keep working unchanged.
const apiTarget = process.env.VITE_API_URL || "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: Number(process.env.PORT) || 3000,
    proxy: {
      "/api": { target: apiTarget, changeOrigin: true },
      "/ws": { target: apiTarget, changeOrigin: true, ws: true },
    },
  },
  build: {
    outDir: "build",
  },
});
