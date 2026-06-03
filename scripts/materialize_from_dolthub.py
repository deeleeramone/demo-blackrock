"""Build the app's SQLite DB from the published DoltHub dataset.

Re-running pulls the latest commit and rebuilds the local tables in place.
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

# Every published table, including the derived look-through views.
TABLES = [
    "funds",
    "holdings",
    "holdings_lookthrough",
    "holdings_lt_latest",
    "nav_history",
    "distributions",
    "fund_documents",
    "fund_links",
    "premium_discount_history",
    "performance_history",
]

# ``holdings`` ACCUMULATES: each pull appends the incoming snapshot(s) by
# as_of_date and keeps prior days, so the local table becomes a growing
# time series (DoltHub itself only carries the latest snapshot). Every other
# table is REPLACED with the incoming data — the funds screener metadata /
# key facts, the derived look-through views, and the full NAV/dist series.
APPEND_BY_DATE = {"holdings": "as_of_date"}
BATCH = 20000


def _clone_or_pull(repo: str, dolt_dir: Path) -> None:
    if (dolt_dir / ".dolt").exists():
        print(f"pulling latest into {dolt_dir} ...", flush=True)
        subprocess.run(["dolt", "pull"], cwd=str(dolt_dir), check=True)
    else:
        print(f"cloning {repo} -> {dolt_dir} ...", flush=True)
        dolt_dir.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["dolt", "clone", repo, str(dolt_dir)], check=True)


def _load_table(conn, dolt_dir: Path, table: str, sqlite_cols: set[str], tmp: Path) -> int:
    out = tmp / f"{table}.csv"
    r = subprocess.run(
        ["dolt", "table", "export", "-f", table, str(out)],
        cwd=str(dolt_dir),
        capture_output=True,
        text=True,
    )
    if r.returncode != 0 or not out.exists():
        print(f"  {table}: not published yet (skip)", flush=True)
        return 0

    date_col = APPEND_BY_DATE.get(table)
    with out.open(newline="", encoding="utf-8") as fh:
        rd = csv.reader(fh)
        header = next(rd, None)
        if not header:
            return 0
        # In append mode, drop the surrogate `id` so SQLite assigns fresh ones
        # (incoming ids would collide with rows already accumulated locally).
        cols = [c for c in header if c in sqlite_cols and not (date_col and c == "id")]
        idx = [header.index(c) for c in cols]
        ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})"

        conn.execute("BEGIN")
        try:
            if date_col:
                # ACCUMULATE: keep prior snapshots, add only as_of_dates we
                # don't already have (idempotent on a same-day re-run).
                have = {row[0] for row in conn.execute(f"SELECT DISTINCT {date_col} FROM {table}")}
                d_idx = header.index(date_col)
                batch, added, skipped = [], 0, 0
                for row in rd:
                    if row[d_idx] in have:
                        skipped += 1
                        continue
                    batch.append([row[i] if row[i] != "" else None for i in idx])
                    added += 1
                    if len(batch) >= BATCH:
                        conn.executemany(ins, batch)
                        batch = []
                if batch:
                    conn.executemany(ins, batch)
                conn.execute("COMMIT")
                print(
                    f"  {table}: +{added:,} rows (added), {skipped:,} already present",
                    flush=True,
                )
                return added
            else:
                # REPLACE (atomic): readers see old rows until COMMIT.
                conn.execute(f"DELETE FROM {table}")
                batch, n = [], 0
                for row in rd:
                    batch.append([row[i] if row[i] != "" else None for i in idx])
                    n += 1
                    if len(batch) >= BATCH:
                        conn.executemany(ins, batch)
                        batch = []
                if batch:
                    conn.executemany(ins, batch)
                conn.execute("COMMIT")
                print(f"  {table}: {n:,} rows (replaced)", flush=True)
                return n
        except Exception:
            conn.execute("ROLLBACK")
            raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="deeleeramone/blackrock-public")
    ap.add_argument("--dolt-dir", required=True, help="Path to keep the dolt clone")
    args = ap.parse_args()

    _clone_or_pull(args.repo, Path(args.dolt_dir).expanduser())
    conn = init_db()
    conn.execute("PRAGMA foreign_keys = OFF")
    sqlite_cols = {t: {r[1] for r in conn.execute(f"PRAGMA table_info({t})")} for t in TABLES}
    with tempfile.TemporaryDirectory() as tmp:
        for t in TABLES:
            _load_table(conn, Path(args.dolt_dir), t, sqlite_cols[t], Path(tmp))
    print("materialization complete.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
