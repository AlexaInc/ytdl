FROM python:3.12-slim

# ffmpeg + deno (JS runtime for yt-dlp) + curl
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl unzip ca-certificates \
 && curl -fsSL https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip -o /tmp/deno.zip \
 && unzip -q /tmp/deno.zip -d /usr/local/bin && chmod +x /usr/local/bin/deno && rm /tmp/deno.zip \
 && apt-get purge -y unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=8080 PYTHONUNBUFFERED=1
EXPOSE 8080
# refresh yt-dlp on every start (YouTube changes weekly), then serve
CMD sh -c "pip install -q -U yt-dlp || true; yt-dlp --version; gunicorn -w 1 --threads 8 --timeout 900 -b 0.0.0.0:${PORT} app:app"
