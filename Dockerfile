# Use a Python base image
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .

# Install FFmpeg and basic runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

# Copy the application code
COPY file_monitor.py .
COPY telegram_monitor.py .
COPY app/ app/
# Copy sanitized default configuration file (no secrets)
COPY config/config.example.json config/config.json

# Expose the port on which the Flask app runs
EXPOSE 5001

# Command to run the Flask application
CMD ["python", "-u", "app/app.py"]