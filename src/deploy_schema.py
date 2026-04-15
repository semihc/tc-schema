#!/usr/bin/env python3
"""
deploy_schema.py — Automated, idempotent schema deployment.

Creates the database if it doesn't exist, runs all numbered patch
files in order, skips already-applied versions.

Dependencies:  pip install psycopg[binary]

Usage:
    python src/deploy_schema.py
    python src/deploy_schema.py --dsn "postgresql://user:pw@host:5433/tcdata"
    python src/deploy_schema.py --patch ./sql/patch
    python src/deploy_schema.py --check      # verify only, don't apply
    python src/deploy_schema.py --create-db  # create database if missing
    python src/deploy_schema.py --drop-db    # drop database only
    python src/deploy_schema.py --reset-db   # drop, recreate, and patch

Environment variables (override defaults):
    EOD_ENV          dev|prod   (selects cfg/<env>.env, default: dev)
    EOD_DSN          overrides DSN from cfg/<env>.env when set
    EOD_PATCH_DIR    ./sql/patch
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
DEFAULT_PATCH_DIR = os.environ.get(
    "EOD_PATCH_DIR",
    str(PROJECT_ROOT / "sql" / "patch"),
)

DEFAULT_DSN_FALLBACK = "postgresql://localhost:5433/tcdata"

# Patch files must match: NNN_description.sql  (e.g. 001_eod_schema.sql)
PATCH_PATTERN = re.compile(r"^(\d{3})_.+\.sql$")


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass
class Patch:
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


def discover_patches(directory: Path) -> list[Patch]:
    """Find and sort patch files."""
    if not directory.is_dir():
        print(f"Patch directory not found: {directory}", file=sys.stderr)
        sys.exit(1)

    patches = []
    for f in sorted(directory.iterdir()):
        match = PATCH_PATTERN.match(f.name)
        if match:
            version = int(match.group(1))
            description = f.stem.split("_", 1)[1].replace("_", " ")
            patches.append(Patch(
                version=version,
                filename=f.name,
                path=f,
                description=description,
            ))

    return sorted(patches, key=lambda p: p.version)


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
    """Return set of already-applied patch versions."""
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


def apply_patch(conn: psycopg.Connection, patch: Patch):
    """Execute a single patch file."""
    sql = patch.path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            """
            INSERT INTO schema_version (version, description)
            VALUES (%s, %s)
            ON CONFLICT (version) DO UPDATE
            SET description = EXCLUDED.description
            """,
            (patch.version, patch.description),
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
        "Cannot apply patches: TimescaleDB is not installed on this PostgreSQL server.",
        file=sys.stderr,
    )
    print(
        "The schema starts by running `CREATE EXTENSION timescaledb`, so patch "
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
    """Discover and report all schema objects present in the public schema."""
    checks = {}

    with conn.cursor(row_factory=dict_row) as cur:
        # PostgreSQL version
        cur.execute("SELECT version()")
        checks["pg_version"] = cur.fetchone()["version"]

        # TimescaleDB version
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        row = cur.fetchone()
        checks["timescaledb_version"] = row["extversion"] if row else "NOT INSTALLED"

        # All tables in public schema (discovered, not hardcoded)
        cur.execute("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
             ORDER BY table_name
        """)
        for r in cur.fetchall():
            checks[f"table:{r['table_name']}"] = True

        # All views in public schema (discovered, not hardcoded)
        cur.execute("""
            SELECT table_name
              FROM information_schema.views
             WHERE table_schema = 'public'
             ORDER BY table_name
        """)
        for r in cur.fetchall():
            checks[f"view:{r['table_name']}"] = True

        # All indexes in public schema (discovered, not hardcoded)
        cur.execute("""
            SELECT indexname
              FROM pg_indexes
             WHERE schemaname = 'public'
             ORDER BY indexname
        """)
        for r in cur.fetchall():
            checks[f"index:{r['indexname']}"] = True

        # TimescaleDB hypertables and their compression state
        if row:
            cur.execute("""
                SELECT hypertable_name, compression_enabled
                  FROM timescaledb_information.hypertables
                 WHERE hypertable_schema = 'public'
                 ORDER BY hypertable_name
            """)
            for r in cur.fetchall():
                checks[f"hypertable:{r['hypertable_name']}"] = True
                checks[f"compression:{r['hypertable_name']}"] = r["compression_enabled"]

        # Row counts for all user tables (excludes schema_version)
        cur.execute("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_type = 'BASE TABLE'
               AND table_name <> 'schema_version'
             ORDER BY table_name
        """)
        user_tables = [r["table_name"] for r in cur.fetchall()]
        for table in user_tables:
            cur.execute(psycopg.sql.SQL("SELECT count(*) AS n FROM {}").format(
                psycopg.sql.Identifier(table)
            ))
            checks[f"rows:{table}"] = cur.fetchone()["n"]

        # Applied patches
        try:
            cur.execute("SELECT version, description, applied_at FROM schema_version ORDER BY version")
            checks["patches"] = cur.fetchall()
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            checks["patches"] = []

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

    # Applied patches
    print()
    patches = checks.get("patches", [])
    if patches:
        print("  Applied patches:")
        for p in patches:
            print(f"    v{p['version']:03d}  {p['description']:<30}  {p['applied_at']:%Y-%m-%d %H:%M}")
    else:
        print("  No patches applied yet.")

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
        description="Deploy EOD schema — idempotent patch runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s                                     # apply pending patches
  %(prog)s --create-db                         # create DB first if needed
  %(prog)s --drop-db                           # drop DB only (no recreate)
  %(prog)s --reset-db                          # drop, recreate, and patch
  %(prog)s --check                             # verify schema, don't change
  %(prog)s --dsn postgresql://prod:5433/tcdata
  %(prog)s --patch /opt/eod/sql/patch
        """,
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    parser.add_argument("--patch", default=DEFAULT_PATCH_DIR,
                        help="Directory containing NNN_*.sql files")
    parser.add_argument("--create-db", action="store_true",
                        help="Create the database if it doesn't exist")
    parser.add_argument("--drop-db", action="store_true",
                        help="Drop the database if it exists (no recreate)")
    parser.add_argument("--reset-db", action="store_true",
                        help="Drop the database if it exists, recreate it, then run all patches")
    parser.add_argument("--check", action="store_true",
                        help="Verify schema only, don't apply patches")
    args = parser.parse_args()

    patch_dir = Path(args.patch)
    server_dsn, dbname = parse_dsn(args.dsn)

    # ── Drop only ──
    if args.drop_db:
        print(f"WARNING: about to drop '{dbname}' — all data will be lost.")
        confirm = input("Type the database name to confirm: ").strip()
        if confirm != dbname:
            print("Aborted.", file=sys.stderr)
            sys.exit(1)
        drop_database(server_dsn, dbname)
        return

    # ── Drop + recreate + patch ──
    if args.reset_db:
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

    # ── Discover patches ──
    patches = discover_patches(patch_dir)
    if not patches:
        print(f"No patch files found in {patch_dir}", file=sys.stderr)
        print(f"Expected files like: 001_eod_schema.sql", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(patches)} patch(es) in {patch_dir}")

    # ── Connect and apply ──
    try:
        with psycopg.connect(args.dsn) as conn:
            # Preflight the server before we start running patch files.
            # This gives a much clearer failure than letting the first patch
            # abort inside `CREATE EXTENSION timescaledb`.
            ensure_timescaledb_available(conn)

            applied = get_applied_versions(conn)
            pending = [p for p in patches if p.version not in applied]

            if not pending:
                print("Schema is up to date — nothing to apply.")
            else:
                print(f"Applying {len(pending)} pending patch(es)...")
                for p in pending:
                    print(f"  → v{p.version:03d}  {p.description} ({p.filename})")
                    apply_patch(conn, p)
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
