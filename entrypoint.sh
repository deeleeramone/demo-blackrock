#!/bin/sh
set -e

DB_DIR="${BLACKROCK_DB_DIR:-/data}"
DB_FILE="$DB_DIR/holdings.db"
mkdir -p "$DB_DIR"

if [ ! -f "$DB_FILE" ]; then
    echo "Database not found. Running initial ingestion (this will take a while) ..."
    python -m openbb_blackrock.ingest --portfolio iShares --verbose
    echo "Ingestion complete."
fi

NAV_COUNT=$(python -c "
import sqlite3, sys
try:
    c = sqlite3.connect('$DB_FILE')
    print(c.execute('SELECT COUNT(*) FROM nav_history').fetchone()[0])
except Exception:
    print(0)
" 2>/dev/null || echo 0)

if [ "$NAV_COUNT" = "0" ]; then
    echo "NAV history empty. Running NAV history + distributions ingestion ..."
    python -m openbb_blackrock.ingest --nav-history --verbose
    echo "NAV history ingestion complete."
fi

# Nightly re-ingest at 2:00 AM (holdings + nav history)
echo "0 2 * * * cd /app && BLACKROCK_DB_DIR=${DB_DIR} python -m openbb_blackrock.ingest --portfolio iShares --nav-history --verbose >> /var/log/ingest.log 2>&1" | crontab -
cron

exec uvicorn openbb_blackrock.portfolio_api:app \
    --host 0.0.0.0 \
    --port "${PORT:-8040}"
