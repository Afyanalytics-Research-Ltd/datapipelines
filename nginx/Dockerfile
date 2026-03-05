FROM ubuntu:latest

# Update and install necessary packages
RUN apt-get update && \
    apt-get install -y nginx python3-certbot-nginx && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /etc/nginx/sites-enabled/default

# Copy your custom nginx configurations
COPY nginx.conf /etc/nginx/sites-enabled/nginx.conf
COPY nginx.conf /etc/nginx/sites-available/nginx.conf

# Command to start nginx (if not started by default)
CMD ["nginx", "-g", "daemon off;"]