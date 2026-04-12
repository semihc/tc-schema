# tc-schema — Claude guidance

PostgreSQL 18 + TimescaleDB schema and ingestion scripts for EOD stock market
data. See `doc/README.md` for user-facing setup and usage instructions.

## Project structure

```
src/deploy_schema.py   — idempotent migration runner
src/eod_ingest.py      — ingestion CLI (prices, instruments, corporate actions)
sql/boot/              — numbered migration files (001_eod_schema.sql, …)
cfg/dev.env            — default DSN for local development (tracked, no creds)
```

## Running the tools

```bash
# Apply pending migrations (creates DB if needed)
python src/deploy_schema.py --create-db

# Verify schema without changing anything
python src/deploy_schema.py --check

# Drop and fully recreate the database (prompts for confirmation)
python src/deploy_schema.py --drop-db

# Ingest EOD prices from CSV
python src/eod_ingest.py prices data/2026-04-09.csv

# Backfill instrument metadata from Yahoo Finance
pip install yfinance
python src/eod_ingest.py enrich
```

## Non-obvious invariants — do not break these

### Migrations must be idempotent
Every statement in `sql/boot/*.sql` must be safe to re-run against an already
migrated database. Use `IF NOT EXISTS`, `CREATE OR REPLACE`, `ON CONFLICT DO
NOTHING`, and `DO $$ … EXCEPTION … $$` blocks. Never write a migration that
fails on a second run.

### No foreign key on eod_price.instrument_id
TimescaleDB hypertables do not support FK constraints on the partitioned table.
The missing FK is intentional. Do not add one — it will fail at runtime.
Referential integrity is enforced at the application layer in `eod_ingest.py`.

### Instrument stub creation and price upsert share one transaction
In `cmd_prices`, auto-creating instrument stubs and upserting the prices that
reference them must succeed or fail together. `auto_add_instruments` must not
commit internally. The caller (`cmd_prices`) owns the single commit after both
writes succeed.

### DSN resolution order
Both scripts resolve the connection string the same way:
1. `--dsn` CLI flag
2. `EOD_DSN` environment variable
3. `EOD_DSN` key in `cfg/<EOD_ENV>.env` (default: `cfg/dev.env`)
4. Hardcoded fallback `postgresql://localhost:5433/tcdata`

Do not simplify this to a single hardcoded default.

### cfg/dev.env is tracked by git
It contains only a localhost DSN — no credentials. Real passwords must go in
`cfg/dev.env.local` (gitignored) or the `EOD_DSN` environment variable.
`cfg/prod.env` is gitignored and must never be committed.

## Dependencies

- `psycopg[binary]` — required by both scripts
- `yfinance` — optional, only needed for `eod_ingest.py enrich`
