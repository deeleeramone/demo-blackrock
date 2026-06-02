This repo is a demonstration OpenBB Workspace application. It utilizes public data from the U.S. iShares universe to construct a "Total Portfolio View".

## Architecture

The data is **produced** once nightly and **consumed** locally:

- **Producer** — a GitHub Action ([.github/workflows/nightly-dolthub.yml](.github/workflows/nightly-dolthub.yml)) fetches from BlackRock, rebuilds the look-through views, and publishes to DoltHub: [deeleeramone/blackrock-public](https://www.dolthub.com/repositories/deeleeramone/blackrock-public). See [dolt/README.md](dolt/README.md).
- **Consumer** — this container materializes its local SQLite DB from DoltHub (`scripts/materialize_from_dolthub.py`) and the app serves from SQLite.
## Run

```sh
docker compose up --build -d
```

On first start it clones DoltHub and materializes the SQLite DB (minutes, not hours). The server runs on port 8040. A nightly cron pulls the latest DoltHub commit (published ~07:30 UTC by the Action) and rebuilds the local tables.

Configure the source repo with `DOLT_REPO` (default `deeleeramone/blackrock-public`).
