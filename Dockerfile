# Use a Python base image
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Install FFmpeg + JS runtimes needed by yt-dlp (node/deno)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg \
    curl \
    unzip \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install deno runtime (used by yt-dlp EJS challenge path)
RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:${PATH}"

# Copy requirements file and install Python dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt
RUN pip install --no-cache-dir --default-timeout=1000 gunicorn

# Copy the application code
COPY file_monitor.py .
COPY telegram_monitor.py .
COPY scripts/update_drama_calendar_env.py scripts/update_drama_calendar_env.py
COPY app/ app/
# Copy sanitized default configuration file (no secrets)
COPY config/config.example.json config/config.json

# Expose the port on which the Flask app runs
EXPOSE 5001

# Command to run the Flask application via production WSGI server
CMD ["gunicorn", "-c", "app/gunicorn_conf.py", "app.app:app"]
