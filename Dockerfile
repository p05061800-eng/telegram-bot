# Long-running process: `run_polling`. Set TELEGRAM_BOT_TOKEN in the host (secret/env).
# Railway / Fly.io / any VPS: build and run this image.
# Render: create a **Background Worker** (not a Web Service) and point to this Dockerfile.

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY bot.py .

CMD ["python3", "bot.py"]
