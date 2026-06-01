"""Seed the local SQLite DB from the currently-published DoltHub tables.

A CI runner is stateless, so without this the nightly ingest starts from an
empty DB and the no-wipe-on-failure guard has nothing to fall back on — a
transient fetch failure would drop that fund from the push.  Seeding from
the Dolt clone makes DoltHub the durable source of truth: each run starts
from the last good published state, refreshes what it can, and retains the
rest.  On the very first run (empty repo) the exports are no-ops.

  python scripts/seed_sqlite_from_dolt.py --dolt-dir dolt-repo
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from openbb_blackrock.db import init_db  # noqa: E402

# Source tables to seed (derived tables are rebuilt by the ingest anyway, but
# seeding them keeps the DB queryable before the rebuild and is cheap).
TABLES = [
    "funds",
    "holdings",
    "nav_history",
    "distributions",
    "fund_documents",
    "fund_links",
]
BATCH = 10000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dolt-dir", required=True)
    args = ap.parse_args()

    conn = init_db()  # creates the SQLite schema at $BLACKROCK_DB_DIR/holdings.db
    sqlite_cols = {
        t: {r[1] for r in conn.execute(f"PRAGMA table_info({t})")} for t in TABLES
    }

    with tempfile.TemporaryDirectory() as tmp:
        for t in TABLES:
            out = Path(tmp) / f"{t}.csv"
            r = subprocess.run(
                ["dolt", "table", "export", t, str(out)],
                cwd=args.dolt_dir,
                capture_output=True,
                text=True,
            )
            if r.returncode != 0 or not out.exists():
                print(f"  {t}: nothing published yet (skip)", flush=True)
                continue
            with out.open(newline="", encoding="utf-8") as fh:
                rd = csv.reader(fh)
                header = next(rd, None)
                if not header:
                    continue
                cols = [c for c in header if c in sqlite_cols[t]]
                idx = [header.index(c) for c in cols]
                ph = ",".join("?" * len(cols))
                sql = f"INSERT OR REPLACE INTO {t} ({','.join(cols)}) VALUES ({ph})"
                batch, n = [], 0
                for row in rd:
                    batch.append([row[i] if row[i] != "" else None for i in idx])
                    n += 1
                    if len(batch) >= BATCH:
                        conn.executemany(sql, batch)
                        batch = []
                if batch:
                    conn.executemany(sql, batch)
                print(f"  {t}: seeded {n:,} rows", flush=True)
    conn.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
