FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends cron curl ca-certificates \
    && curl -fsSL https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY openbb_blackrock/ ./openbb_blackrock/
COPY scripts/ ./scripts/

ENV BLACKROCK_DB_DIR=/data
ENV PYTHONUNBUFFERED=1

EXPOSE 8040

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
