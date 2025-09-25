# backend/customer_service/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# (Optional) system deps helpful for common wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install service deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Service port
EXPOSE 8001

# Start FastAPI service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
