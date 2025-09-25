# frontend/Dockerfile

# ---------- Build stage ----------
FROM node:18-alpine AS build

WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
# If your build command is different, change it here:
RUN npm run build

# ---------- Run stage ----------
FROM nginx:alpine

# Change this if your bundler outputs "build" instead of "dist"
# - React CRA -> "build"
# - Vite -> "dist"
# Copy both possibilities; only one will exist
COPY --from=build /app/dist /usr/share/nginx/html
COPY --from=build /app/build /usr/share/nginx/html

# Optional: custom Nginx config (uncomment and add nginx.conf if you need SPA fallback)
# COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
