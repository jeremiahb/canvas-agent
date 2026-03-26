FROM python:3.11-slim

# Install Node.js 20 (needed to build the React dashboard)
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first so this layer is cached across code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake Playwright's Chromium + all system dependencies into the image.
# This replaces the `playwright install chromium` in the Railway start command,
# eliminating the 167 MB download on every cold start.
RUN playwright install --with-deps chromium

# Build React dashboard
COPY dashboard/package*.json dashboard/
RUN cd dashboard && npm ci --prefer-offline
COPY dashboard/ dashboard/
RUN cd dashboard && npm run build

# Copy application code last so code changes don't bust the layers above
COPY . .

CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
