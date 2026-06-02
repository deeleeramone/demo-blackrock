"""Load the freshly-ingested SQLite DB into the Dolt working set, commit,
and push to DoltHub."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

# Tables to publish, with their primary key(s).  raw_json columns are dropped.
TABLES = [
    "funds",
    "holdings",
    "holdings_lookthrough",
    "holdings_lt_latest",
    "nav_history",
    "distributions",
    "fund_documents",
    "fund_links",
    # fx_rates is empty in the current pipeline; skip unless populated.
]
DROP_COLUMNS = {"raw_json"}


def _run(cmd: list[str], cwd: str | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def _export_csv(conn: sqlite3.Connection, table: str, dest: Path) -> int:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall() if r[1] not in DROP_COLUMNS]
    col_sql = ",".join(f'"{c}"' for c in cols)
    cur = conn.execute(f"SELECT {col_sql} FROM {table}")
    n = 0
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for row in cur:
            w.writerow(["" if v is None else v for v in row])
            n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", required=True, help="Path to the ingested holdings.db")
    ap.add_argument("--dolt-dir", required=True, help="Path to the dolt clone")
    ap.add_argument("--schema", default="dolt/schema.sql")
    ap.add_argument("--message", default="Nightly iShares refresh")
    ap.add_argument("--remote", default="origin")
    ap.add_argument("--branch", default="main")
    ap.add_argument(
        "--user",
        default=None,
        help="Only for Dolt SQL Server remotes (uses --user + "
        "DOLT_REMOTE_PASSWORD). Leave unset for DoltHub, which authenticates "
        "via the imported JWK credential.",
    )
    ap.add_argument("--no-push", action="store_true", help="Build + commit only")
    args = ap.parse_args()

    sqlite_path = Path(args.sqlite).expanduser()
    dolt_dir = Path(args.dolt_dir).expanduser()
    schema_sql = Path(args.schema).read_text()
    if not sqlite_path.exists():
        sys.exit(f"sqlite db not found: {sqlite_path}")
    if not (dolt_dir / ".dolt").exists():
        sys.exit(f"not a dolt repo: {dolt_dir}")

    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)

    # 1) Drop + recreate the published tables from the canonical schema so the
    #    column types/sizes stay fixed regardless of CSV-inferred types.
    drops = "\n".join(f"DROP TABLE IF EXISTS {t};" for t in TABLES + ["fx_rates"])
    _run(["dolt", "sql", "-q", drops], cwd=str(dolt_dir))
    _run(["dolt", "sql", "-q", schema_sql], cwd=str(dolt_dir))

    # 2) Export each table to CSV and import it.
    with tempfile.TemporaryDirectory() as tmp:
        for t in TABLES:
            dest = Path(tmp) / f"{t}.csv"
            n = _export_csv(conn, t, dest)
            print(f"  {t}: exported {n:,} rows", flush=True)
            if n == 0:
                continue
            _run(["dolt", "table", "import", "-u", t, str(dest)], cwd=str(dolt_dir))
    conn.close()

    # 3) Commit (skip if nothing changed) and push.
    _run(["dolt", "add", "-A"], cwd=str(dolt_dir))
    commit = subprocess.run(
        ["dolt", "commit", "--skip-empty", "-m", args.message],
        cwd=str(dolt_dir),
    )
    if commit.returncode != 0:
        print("nothing to commit (no data changes) — skipping push", flush=True)
        return 0
    if not args.no_push:
        push = ["dolt", "push"]
        if args.user:
            push += ["--user", args.user]
        push += [args.remote, args.branch]
        _run(push, cwd=str(dolt_dir))
    print("done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
