#!/usr/bin/env python3
"""
deploy_schema.py — Automated, idempotent schema deployment.

Creates the database if it doesn't exist, runs all numbered migration
files in order, skips already-applied versions.

Dependencies:  pip install psycopg[binary]

Usage:
    python src/deploy_schema.py
    python src/deploy_schema.py --dsn "postgresql://user:pw@host:5433/tcdata"
    python src/deploy_schema.py --migrations ./sql/boot
    python src/deploy_schema.py --check      # verify only, don't apply
    python src/deploy_schema.py --create-db  # create database if missing

Environment variables (override defaults):
    EOD_ENV              dev|prod   (selects cfg/<env>.env, default: dev)
    EOD_DSN              overrides DSN from cfg/<env>.env when set
    EOD_MIGRATIONS_DIR   ./sql/boot
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV = os.environ.get("EOD_ENV", "dev")
DEFAULT_MIGRATIONS = os.environ.get(
    "EOD_MIGRATIONS_DIR",
    str(PROJECT_ROOT / "sql" / "boot"),
)

DEFAULT_DSN_FALLBACK = "postgresql://localhost:5433/tcdata"

# Migration files must match: NNN_description.sql  (e.g. 001_eod_schema.sql)
MIGRATION_PATTERN = re.compile(r"^(\d{3})_.+\.sql$")


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass
class Migration:
    version: int
    filename: str
    path: Path
    description: str


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_simple_env_file(path: Path) -> dict[str, str]:
    """Read KEY=VALUE pairs from a small .env file.

    This loader is intentionally minimal because the project config files are
    simple and checked into the repo. We ignore blank lines and comments, and
    leave shell-style expansion out of scope so DSN resolution stays explicit
    and predictable.
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")

    return values


def resolve_default_dsn() -> str:
    """Resolve the default DSN from cfg/<env>.env, then env vars, then fallback."""
    env_name = DEFAULT_ENV
    env_file = PROJECT_ROOT / "cfg" / f"{env_name}.env"
    file_values = load_simple_env_file(env_file)

    # Default DSN should come from the selected config file so local runs pick
    # up the repo's dev/prod conventions without requiring a long CLI flag.
    # We still allow the process environment to win, because automation and
    # one-off operational commands often need to override the checked-in file
    # without editing repository config.
    return os.environ.get(
        "EOD_DSN",
        file_values.get("EOD_DSN", DEFAULT_DSN_FALLBACK),
    )


DEFAULT_DSN = resolve_default_dsn()

def parse_dsn(dsn: str) -> tuple[str, str]:
    """Extract (server_dsn, dbname) from a full DSN.
    server_dsn connects to 'postgres' db for admin operations."""
    # Simple extraction — handles postgresql://user:pass@host:port/dbname
    if "/" in dsn.rsplit("@", 1)[-1]:
        base, dbname = dsn.rsplit("/", 1)
        # Strip query params from dbname
        dbname = dbname.split("?")[0]
        server_dsn = f"{base}/postgres"
        return server_dsn, dbname
    return dsn, "tcdata"


def discover_migrations(directory: Path) -> list[Migration]:
    """Find and sort migration files."""
    if not directory.is_dir():
        print(f"Migrations directory not found: {directory}", file=sys.stderr)
        sys.exit(1)

    migrations = []
    for f in sorted(directory.iterdir()):
        match = MIGRATION_PATTERN.match(f.name)
        if match:
            version = int(match.group(1))
            description = f.stem.split("_", 1)[1].replace("_", " ")
            migrations.append(Migration(
                version=version,
                filename=f.name,
                path=f,
                description=description,
            ))

    return sorted(migrations, key=lambda m: m.version)


def database_exists(server_dsn: str, dbname: str) -> bool:
    with psycopg.connect(server_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)
            )
            return cur.fetchone() is not None


def create_database(server_dsn: str, dbname: str):
    with psycopg.connect(server_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Use format() not %s — CREATE DATABASE doesn't support params
            cur.execute(
                psycopg.sql.SQL("CREATE DATABASE {}").format(
                    psycopg.sql.Identifier(dbname)
                )
            )
    print(f"✓ Created database: {dbname}")


def drop_database(server_dsn: str, dbname: str):
    with psycopg.connect(server_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Terminate active connections first so DROP DATABASE doesn't block.
            cur.execute(
                psycopg.sql.SQL(
                    "SELECT pg_terminate_backend(pid)"
                    "  FROM pg_stat_activity"
                    " WHERE datname = {} AND pid <> pg_backend_pid()"
                ).format(psycopg.sql.Literal(dbname))
            )
            cur.execute(
                psycopg.sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    psycopg.sql.Identifier(dbname)
                )
            )
    print(f"✓ Dropped database: {dbname}")


def get_applied_versions(conn: psycopg.Connection) -> set[int]:
    """Return set of already-applied migration versions."""
    with conn.cursor() as cur:
        # Check if schema_version table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = 'schema_version'
            )
        """)
        if not cur.fetchone()[0]:
            return set()

        try:
            cur.execute("SELECT version FROM schema_version")
            return {row[0] for row in cur.fetchall()}
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            return set()


def apply_migration(conn: psycopg.Connection, migration: Migration):
    """Execute a single migration file."""
    sql = migration.path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            """
            INSERT INTO schema_version (version, description)
            VALUES (%s, %s)
            ON CONFLICT (version) DO UPDATE
            SET description = EXCLUDED.description
            """,
            (migration.version, migration.description),
        )
    conn.commit()


def get_available_extension_version(conn: psycopg.Connection, name: str) -> str | None:
    """Return the installable version for an extension, or None if unavailable."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT default_version
              FROM pg_available_extensions
             WHERE name = %s
            """,
            (name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def ensure_timescaledb_available(conn: psycopg.Connection):
    """Fail early with a friendly message if TimescaleDB is not installed."""
    available_version = get_available_extension_version(conn, "timescaledb")
    if available_version:
        return

    print(
        "Cannot apply migrations: TimescaleDB is not installed on this PostgreSQL server.",
        file=sys.stderr,
    )
    print(
        "The schema starts by running `CREATE EXTENSION timescaledb`, so migration "
        "001 would fail immediately.",
        file=sys.stderr,
    )
    print(
        "Install the TimescaleDB extension for your PostgreSQL instance, then rerun "
        "this command.",
        file=sys.stderr,
    )
    sys.exit(1)


def verify_schema(conn: psycopg.Connection) -> dict:
    """Check that core objects exist and return a status report."""
    checks = {}

    with conn.cursor(row_factory=dict_row) as cur:
        # Tables
        for table in ["instrument", "eod_price", "corporate_action", "schema_version"]:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                     WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table,))
            checks[f"table:{table}"] = cur.fetchone()["exists"]

        # TimescaleDB version
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        row = cur.fetchone()
        checks["timescaledb_version"] = row["extversion"] if row else "NOT INSTALLED"

        # PostgreSQL version
        cur.execute("SELECT version()")
        checks["pg_version"] = cur.fetchone()["version"]

        if row:
            # Hypertable
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                     WHERE hypertable_name = 'eod_price'
                )
            """)
            checks["hypertable:eod_price"] = cur.fetchone()["exists"]

            # Compression
            cur.execute("""
                SELECT compression_enabled
                  FROM timescaledb_information.hypertables
                 WHERE hypertable_name = 'eod_price'
            """)
            hypertable = cur.fetchone()
            checks["compression:eod_price"] = (
                hypertable["compression_enabled"] if hypertable else False
            )
        else:
            checks["hypertable:eod_price"] = False
            checks["compression:eod_price"] = False

        # Views
        for view in ["v_latest_price", "v_adjustment_factor", "v_cumulative_factor",
                      "v_adjusted_price", "v_daily_return"]:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.views
                     WHERE table_schema = 'public' AND table_name = %s
                )
            """, (view,))
            checks[f"view:{view}"] = cur.fetchone()["exists"]

        # Indexes
        for idx in ["idx_eod_pk", "idx_eod_date", "idx_corpact"]:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes
                     WHERE schemaname = 'public' AND indexname = %s
                )
            """, (idx,))
            checks[f"index:{idx}"] = cur.fetchone()["exists"]

        # Row counts
        for table in ["instrument", "eod_price", "corporate_action"]:
            cur.execute(f"SELECT count(*) AS n FROM {table}")  # noqa: S608
            checks[f"rows:{table}"] = cur.fetchone()["n"]

        # Applied migrations
        try:
            cur.execute("SELECT version, description, applied_at FROM schema_version ORDER BY version")
            checks["migrations"] = cur.fetchall()
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            checks["migrations"] = []

    return checks


def print_verification(checks: dict):
    print("\n── Schema verification ──────────────────────────────")
    print(f"  PostgreSQL:   {checks['pg_version']}")
    print(f"  TimescaleDB:  {checks['timescaledb_version']}")
    print()

    # Objects
    for key, val in sorted(checks.items()):
        if key.startswith(("table:", "hypertable:", "compression:", "view:", "index:")):
            status = "✓" if val else "✗"
            print(f"  {status}  {key}")

    # Row counts
    print()
    for key, val in sorted(checks.items()):
        if key.startswith("rows:"):
            print(f"  {key.replace('rows:', ''):<20} {val:>10} rows")

    # Migrations
    print()
    migrations = checks.get("migrations", [])
    if migrations:
        print("  Applied migrations:")
        for m in migrations:
            print(f"    v{m['version']:03d}  {m['description']:<30}  {m['applied_at']:%Y-%m-%d %H:%M}")
    else:
        print("  No migrations applied yet.")

    # Overall status
    print()
    failed = [k for k, v in checks.items()
              if isinstance(v, bool) and not v]
    if failed:
        print(f"  ⚠  {len(failed)} check(s) failed: {failed}")
    else:
        print("  ✓  All checks passed.")
    print()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Deploy EOD schema — idempotent migration runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s                                     # apply pending migrations
  %(prog)s --create-db                         # create DB first if needed
  %(prog)s --drop-db                           # drop, recreate, and migrate
  %(prog)s --check                             # verify schema, don't change
  %(prog)s --dsn postgresql://prod:5432/mktdata
  %(prog)s --migrations /opt/eod/sql/boot
        """,
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    parser.add_argument("--migrations", default=DEFAULT_MIGRATIONS,
                        help="Directory containing NNN_*.sql files")
    parser.add_argument("--create-db", action="store_true",
                        help="Create the database if it doesn't exist")
    parser.add_argument("--drop-db", action="store_true",
                        help="Drop the database if it exists, recreate it, then run all migrations")
    parser.add_argument("--check", action="store_true",
                        help="Verify schema only, don't apply migrations")
    args = parser.parse_args()

    migrations_dir = Path(args.migrations)
    server_dsn, dbname = parse_dsn(args.dsn)

    # ── Drop + recreate if requested ──
    if args.drop_db:
        print(f"WARNING: about to drop and recreate '{dbname}' — all data will be lost.")
        confirm = input("Type the database name to confirm: ").strip()
        if confirm != dbname:
            print("Aborted.", file=sys.stderr)
            sys.exit(1)
        drop_database(server_dsn, dbname)
        create_database(server_dsn, dbname)

    # ── Create database if requested ──
    elif args.create_db:
        if database_exists(server_dsn, dbname):
            print(f"Database '{dbname}' already exists.")
        else:
            create_database(server_dsn, dbname)

    # ── Check mode ──
    if args.check:
        try:
            with psycopg.connect(args.dsn) as conn:
                checks = verify_schema(conn)
                print_verification(checks)
        except psycopg.OperationalError as e:
            print(f"Cannot connect: {e}", file=sys.stderr)
            sys.exit(1)
        return

    # ── Discover migrations ──
    migrations = discover_migrations(migrations_dir)
    if not migrations:
        print(f"No migration files found in {migrations_dir}", file=sys.stderr)
        print(f"Expected files like: 001_eod_schema.sql", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(migrations)} migration(s) in {migrations_dir}")

    # ── Connect and apply ──
    try:
        with psycopg.connect(args.dsn) as conn:
            # Preflight the server before we start running migration files.
            # This gives a much clearer failure than letting the first migration
            # abort inside `CREATE EXTENSION timescaledb`.
            ensure_timescaledb_available(conn)

            applied = get_applied_versions(conn)
            pending = [m for m in migrations if m.version not in applied]

            if not pending:
                print("Schema is up to date — nothing to apply.")
            else:
                print(f"Applying {len(pending)} pending migration(s)...")
                for m in pending:
                    print(f"  → v{m.version:03d}  {m.description} ({m.filename})")
                    apply_migration(conn, m)
                    print(f"    ✓ applied")

            # Verify
            checks = verify_schema(conn)
            print_verification(checks)

    except psycopg.OperationalError as e:
        print(f"Cannot connect: {e}", file=sys.stderr)
        if "does not exist" in str(e):
            print(f"Hint: run with --create-db to create the database first.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
