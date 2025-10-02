FROM node:18-alpine

WORKDIR /app

# Install dependencies using npm to avoid yarn.lock requirement
COPY package.json ./
RUN npm install --legacy-peer-deps

# Copy app source
COPY . .

# Install curl for healthchecks and build the app
RUN apk add --no-cache curl \
    && npm run build \
    && npm install -g serve

ENV PORT=3000
ENV HOST=0.0.0.0

# Serve production build
CMD ["serve", "-s", "build", "-l", "3000"]


