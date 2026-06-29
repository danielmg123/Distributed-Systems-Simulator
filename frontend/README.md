# DSS Frontend

The Distributed Systems Simulator dashboard: a React app built with [Vite](https://vite.dev/).

## Scripts

In this directory:

### `npm start` (or `npm run dev`)

Runs the dev server at [http://localhost:3000](http://localhost:3000) with hot module
reloading. It proxies `/api` and `/ws` (the SockJS/STOMP WebSocket) to the backend on
`http://localhost:8080`, so run the backend alongside it (see the repo root README).

### `npm run build`

Produces the static production bundle in `build/`.

### `npm run preview`

Serves the contents of `build/` locally to sanity-check a production build.

## Docker

`Dockerfile` builds the bundle and serves it with nginx, which reverse-proxies `/api` and
`/ws` to the backend container (see `nginx.conf`). It's built as part of the stack in
`deployment/docker-compose.yml`.
