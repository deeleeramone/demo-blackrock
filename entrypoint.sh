#!/bin/sh
set -e

DB_DIR="${BLACKROCK_DB_DIR:-/data}"
DB_FILE="$DB_DIR/holdings.db"
CLONE_DIR="$DB_DIR/dolt-clone"
REPO="${DOLT_REPO:-deeleeramone/blackrock-public}"
mkdir -p "$DB_DIR"

# Build the local SQLite DB from the published DoltHub dataset instead of
# generating it from BlackRock (~60 min). The nightly GitHub Action is the
# producer (BlackRock -> DoltHub); this container is a consumer.
if [ ! -f "$DB_FILE" ]; then
    echo "Database not found. Materializing from DoltHub ($REPO) ..."
    python /app/scripts/materialize_from_dolthub.py --repo "$REPO" --dolt-dir "$CLONE_DIR"
    echo "Materialization complete."
fi

# Nightly: pull the latest DoltHub commit and rebuild the local tables in
# place. Scheduled after the publishing Action (07:30 UTC); adjust as needed.
# NOTE: cron runs with a minimal PATH (/usr/bin:/bin) that excludes
# /usr/local/bin, where the slim image keeps python AND dolt — so the PATH
# line below is required or the job dies with "python: not found".
{
    echo "PATH=/usr/local/bin:/usr/bin:/bin"
    echo "0 9 * * * cd /app && BLACKROCK_DB_DIR=${DB_DIR} python /app/scripts/materialize_from_dolthub.py --repo ${REPO} --dolt-dir ${CLONE_DIR} >> /var/log/materialize.log 2>&1"
} | crontab -
cron

exec uvicorn openbb_blackrock.portfolio_api:app \
    --host 0.0.0.0 \
    --port "${PORT:-8040}"
