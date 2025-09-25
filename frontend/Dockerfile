# frontend/Dockerfile
FROM node:18-alpine

WORKDIR /app

# copy only dependency files first for better caching
COPY package.json package-lock.json ./

# If you have a lock file, keep it; otherwise this still works.
# These flags skip audit/fund output and relax peer-deps a bit.
RUN npm install --no-audit --no-fund --legacy-peer-deps

# now copy the rest of the app
COPY . .

# build (adjust if your script name differs)
RUN npm run build

# (optional) if you run a preview server in container:
EXPOSE 5173
CMD ["npm","run","preview","--","--host","0.0.0.0","--port","5173"]
