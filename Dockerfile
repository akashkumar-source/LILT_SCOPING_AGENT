# Use a lightweight Python base image
FROM python:3.11-slim

# Set environment variables to prevent bytecode and buffer issues
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libsm6 \
    libxext6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create a directory for persistent data (logs/outputs)
RUN mkdir -p /data/logs /data/outputs && chmod -R 777 /data

# Set environment variables for the app to use these paths
ENV LOG_DIR=/data/logs
ENV OUTPUT_DIR=/data/outputs
ENV PORT=8080

# Expose the Cloud Run port
EXPOSE 8080

# Run the application using uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
