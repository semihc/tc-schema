# EOD Stock Market Data — Compact Schema

PostgreSQL 18 + TimescaleDB schema for private investors.

## Project structure

```
tc-schema/
├── src/
│   ├── deploy_schema.py      # Automated, idempotent schema deployment
│   └── eod_ingest.py         # Data ingestion CLI (prices, instruments, actions)
├── sql/
│   └── patch/
│       └── 001_eod_schema.sql
└── doc/
    └── README.md
```

## Quick start

```bash
pip install psycopg[binary]

# 1. Create database and apply schema
# By default the script reads EOD_DSN from cfg/dev.env.
python src/deploy_schema.py --create-db

# 2. Verify
python src/deploy_schema.py --check

# 3. Ingest prices (instruments auto-created)
python src/eod_ingest.py prices data/2026-04-09.csv

# 4. Backfill metadata from Yahoo Finance
pip install yfinance
python src/eod_ingest.py enrich
```

## Adding patches

Create new files in `sql/patch/` following the naming convention:

```
002_add_watchlist.sql
003_add_portfolio_table.sql
```

Each file must be idempotent (use `IF NOT EXISTS`, `CREATE OR REPLACE`,
`ON CONFLICT DO NOTHING`, `DO $$ ... EXCEPTION ... $$` blocks).

Run `python src/deploy_schema.py` — it skips already-applied versions,
applies only new ones, and records each successful patch in
`schema_version`.

## PostgreSQL operational tasks

### DB init

```bash
mkdir -p ~/TC/pgdb
initdb -D ~/TC/pgdb
vi ~/TC/pgdb/postgresql.conf
# Uncomment the following to use a non-default port:
# port = 5433
```

### Start / stop

```bash
# Start the instance (log written to server.log)
pg_ctl -D ~/TC/pgdb -l ~/TC/pgdb/server.log start

# Check status
pg_ctl -D ~/TC/pgdb status
# pg_ctl: server is running (PID: 25306)
# /opt/stow/postgresql-18.3/bin/postgres "-D" "/home/semihc/TC/pgdb"

# Stop the instance
pg_ctl -D ~/TC/pgdb stop
```

### Verify connectivity

```bash
# Confirm the instance is listening on the expected port
sudo ss -tulpn | grep :5433

# Open a psql session
psql -p 5433 -d postgres
```

## Environment variables

| Variable             | Default                                      |
|----------------------|----------------------------------------------|
| `EOD_ENV`            | `dev` (`cfg/dev.env`; `prod` uses `cfg/prod.env`) |
| `EOD_DSN`            | Overrides `cfg/<env>.env` when set            |
| `EOD_PATCH_DIR`      | `./sql/patch`                                 |
