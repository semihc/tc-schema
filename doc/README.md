# EOD Stock Market Data — Compact Schema

PostgreSQL 18 + TimescaleDB schema for private investors.

## Project structure

```
tc-schema/
├── src/
│   ├── deploy_schema.py      # Automated, idempotent schema deployment
│   └── eod_ingest.py         # Data ingestion CLI (prices, instruments, actions)
├── sql/
│   └── boot/
│       └── 001_eod_schema.sql
└── doc/
    └── README.md
```

## Quick start

```bash
pip install psycopg[binary]

# 1. Create database and apply schema
python src/deploy_schema.py --create-db --dsn "postgresql://localhost:5432/marketdata"

# 2. Verify
python src/deploy_schema.py --check

# 3. Ingest prices (instruments auto-created)
python src/eod_ingest.py prices data/2026-04-09.csv

# 4. Backfill metadata from Yahoo Finance
pip install yfinance
python src/eod_ingest.py enrich
```

## Adding migrations

Create new files in `sql/boot/` following the naming convention:

```
002_add_watchlist.sql
003_add_portfolio_table.sql
```

Each file must be idempotent (use `IF NOT EXISTS`, `CREATE OR REPLACE`,
`ON CONFLICT DO NOTHING`, `DO $$ ... EXCEPTION ... $$` blocks).

Run `python src/deploy_schema.py` — it skips already-applied versions,
applies only new ones, and records each successful migration in
`schema_version`.

## Environment variables

| Variable             | Default                                      |
|----------------------|----------------------------------------------|
| `EOD_DSN`            | `postgresql://localhost:5432/marketdata`      |
| `EOD_MIGRATIONS_DIR` | `./sql/boot`                                  |
