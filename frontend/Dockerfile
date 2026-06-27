# --- Build stage: produce the static production bundle ---
FROM node:18-alpine AS build
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# --- Run stage: serve the bundle with nginx and proxy API/WebSocket calls to the backend ---
FROM nginx:alpine
COPY --from=build /usr/src/app/build /usr/share/nginx/html
# Replaces the default server block: SPA routing + /api and /ws reverse proxy.
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
