# --- Stage 1: Build Environment ---
# This stage uses a Node.js image to build your Vite application into static files.
FROM node:20-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to leverage Docker's caching mechanism.
# This layer is only rebuilt if your dependencies change.
COPY package*.json ./

# Install project dependencies
RUN npm install

# Copy the rest of your application's source code into the container
COPY . .

# Build the application for production. This creates the 'dist' folder.
RUN npm run build

# --- Stage 2: Production Environment ---
# This stage creates the final, lightweight image using Nginx to serve the static files.
FROM nginx:stable-alpine

# Copy the custom Nginx configuration for handling SPA routing.
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy only the built application files from the 'builder' stage into Nginx's web root.
COPY --from=builder /app/dist /usr/share/nginx/html

# Expose port 80 to allow traffic to the web server
EXPOSE 8080

# The command to start the Nginx web server when the container launches
CMD ["nginx", "-g", "daemon off;"]

