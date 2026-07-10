# --- Build stage: produce the static production bundle with Vite ---
FROM node:22-alpine AS build
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm ci
COPY . .
# Vite is configured to emit into build/ (see vite.config.js), matching the copy below.
RUN npm run build

# --- Run stage: serve the bundle with nginx and proxy API/WebSocket calls to the backend ---
FROM nginx:alpine
COPY --from=build /usr/src/app/build /usr/share/nginx/html
# Replaces the default server block: SPA routing + /api and /ws reverse proxy.
COPY nginx.conf /etc/nginx/conf.d/default.conf
# On-theme holding page nginx serves when the backend proxy is unreachable (cold start).
# Kept out of the Vite bundle so it ships only with the Docker image, not the static site.
COPY nginx-waking.html /usr/share/nginx/html/waking.html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
