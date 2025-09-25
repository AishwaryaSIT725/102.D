# Simple static site image — no Node or build step required
FROM nginx:1.25-alpine

# Use your nginx.conf if you have one, otherwise this line can be removed
# and Nginx's default will serve /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy only static assets
COPY index.html /usr/share/nginx/html/index.html
COPY main.js /usr/share/nginx/html/main.js

# If you have more assets (css, images, etc.), copy them too, e.g.:
# COPY assets/ /usr/share/nginx/html/assets/
