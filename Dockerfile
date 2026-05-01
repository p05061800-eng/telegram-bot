# Long-running process: `run_polling`. Set TELEGRAM_BOT_TOKEN in the host (secret/env).
# Login API: на Railway задайте PORT — бот слушает 0.0.0.0:$PORT (send-code / verify-code).
# При прокси с другим Host: LOGIN_API_PUBLIC_URL=https://ваш-публичный-URL-бота
# Railway / Fly.io / any VPS: build and run this image.
# Render: create a **Background Worker** (not a Web Service) and point to this Dockerfile.

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY main.py .
COPY web ./web

CMD ["python3", "main.py"]
