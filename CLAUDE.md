# tc-schema — Claude guidance

PostgreSQL 18 + TimescaleDB schema and ingestion scripts for EOD stock market
data. See `doc/README.md` for user-facing setup and usage instructions.

## Project structure

```
src/opSchema.py        — idempotent patch runner
src/loadEodData.py     — ingestion CLI (prices, instruments, corporate actions)
sql/patch/             — numbered patch files (001_schema.sql, …)
cfg/dev.env            — default DSN for local development (tracked, no creds)
```

## Running the tools

```bash
# Apply pending patches (creates DB if needed)
python src/opSchema.py --create-db

# Verify schema without changing anything
python src/opSchema.py --check

# Drop the database only (prompts for confirmation)
python src/opSchema.py --drop-db

# Drop, recreate, and re-apply all patches (prompts for confirmation)
python src/opSchema.py --reset-db

# Ingest EOD prices from CSV
python src/loadEodData.py prices data/2026-04-09.csv

# Backfill instrument metadata from Yahoo Finance
pip install yfinance
python src/loadEodData.py enrich
```

## Non-obvious invariants — do not break these

### Patches must be idempotent
Every statement in `sql/patch/*.sql` must be safe to re-run against an already
patched database. Use `IF NOT EXISTS`, `CREATE OR REPLACE`, `ON CONFLICT DO
NOTHING`, and `DO $$ … EXCEPTION … $$` blocks. Never write a patch that
fails on a second run.

### No foreign key on eod_price.instrument_id
TimescaleDB hypertables do not support FK constraints on the partitioned table.
The missing FK is intentional. Do not add one — it will fail at runtime.
Referential integrity is enforced at the application layer in `loadEodData.py`.

### Instrument stub creation and price upsert share one transaction
In `cmdPrices`, auto-creating instrument stubs and upserting the prices that
reference them must succeed or fail together. `autoAddInstruments` must not
commit internally. The caller (`cmdPrices`) owns the single commit after both
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
- `yfinance` — optional, only needed for `loadEodData.py enrich`
