# Stage 1: build with Node
FROM node:18 AS build

WORKDIR /app

# 1) copy only package files first (better cache)
COPY package*.json ./

# 2) install deps (dev deps included for build)
RUN npm ci || npm install

# 3) copy the rest of the app
COPY . .

# (optional but helpful) make local bin executables actually executable
RUN chmod -R +x node_modules/.bin || true

# 4) build (use npx so we don't rely on the executable bit)
# If the script exists, this will still call the local vite.
RUN npm run build || npx vite build

# Stage 2: serve with nginx
FROM nginx:alpine

# copy built assets
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
