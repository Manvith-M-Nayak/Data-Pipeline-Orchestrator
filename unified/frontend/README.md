# Pipeline Orchestrator — Frontend

React + Vite SPA for the unified agent backend.

## Develop

```bash
npm install
npm run dev        # http://localhost:5173
```

The dev server proxies `/api` and `/ws` to the backend. Defaults to
`http://localhost:8000`; override via a `.env` file in this directory:

```
VITE_API_PROXY=http://localhost:8011   # backend host:port
VITE_PORT=5173                          # dev server port
VITE_API_KEY=your-api-key               # sent as x-api-key when backend auth is on
```

## Build & serve (production)

```bash
npm run build      # → dist/ (gitignored; not committed)
npm run preview    # serve the build locally to smoke-test
```

`dist/` is a static bundle. In production, serve it from any static host / CDN,
or from the FastAPI app, behind a reverse proxy that routes:

- `/api/*` → backend (FastAPI, default port 8000)
- `/ws/*`  → backend WebSocket (`/ws/live`)

The app calls the API at the relative path `/api`, so no rebuild is needed to
point at a different backend — only the reverse-proxy routing changes. When the
backend has `API_KEY` set, provide `VITE_API_KEY` at build time so requests carry
the `x-api-key` header.
