This repo is a demonstration OpenBB Workspace application. It utilizes public data from the U.S. iShares universe to construct a "Total Portfolio View".

It requires assembling a database, and this is slow to build. The first time it is run, it will build.

To start:

```sh
docker compose up --build -d
```

Let it cook, it can take 60+ mins, it'll be worth the wait and much better than canned demoware data.

The server will run on port 8040, and the database will update itself at approximately 2:00 AM.
