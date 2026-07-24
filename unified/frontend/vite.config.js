import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// Dev-only proxy. Override the backend target with VITE_API_PROXY (e.g. when the
// API runs on a non-default port or host). In production the app is served as a
// static build behind a reverse proxy that routes /api and /ws — see README.md.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  // Use explicit 127.0.0.1 (not "localhost"): Node resolves "localhost" to IPv6
  // ::1 first, but uvicorn --host 127.0.0.1 binds IPv4 only, so a "localhost"
  // target makes the proxy hit ::1:8000 and fail. IPv4 is deterministic.
  const apiTarget = env.VITE_API_PROXY || "http://127.0.0.1:8000";
  const wsTarget = apiTarget.replace(/^http/, "ws");
  return {
    plugins: [react()],
    server: {
      port: Number(env.VITE_PORT) || 5173,
      proxy: {
        "/api": apiTarget,
        "/ws": { target: wsTarget, ws: true },
      },
    },
  };
});
