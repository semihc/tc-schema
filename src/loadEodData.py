#!/usr/bin/env python3
"""
loadEodData.py — Load/ingest EOD data.

No pandas dependency — uses only stdlib csv + psycopg.

Subcommands:
  prices             Upsert EOD prices from CSV (auto-creates unknown instruments)
  add-instrument     Add or update a single instrument
  list-instruments   Show all registered instruments
  import-instruments Bulk import instruments from CSV
  enrich             Backfill name/sector/type from Yahoo Finance
  add-action         Record a corporate action
  list-actions       List corporate actions for a symbol

Dependencies:  pip install psycopg[binary]
Optional:      pip install yfinance   (only needed for 'enrich' subcommand)

Usage:
    python src/loadEodData.py prices data/2026-04-09.csv
    python src/loadEodData.py prices data/2026-04-09.csv --no-auto-add
    python src/loadEodData.py enrich
    python src/loadEodData.py enrich --symbol BHP
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


# ── Configuration ─────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_ENV  = os.environ.get("EOD_ENV", "dev")
_DSN_FALLBACK = "postgresql://localhost:5433/tcdata"


def _loadSimpleEnvFile(path: Path) -> dict[str, str]:
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


def _resolveDsn() -> str:
    """Resolve DSN from cfg/<env>.env → EOD_DSN env var → hardcoded fallback.

    Mirrors the resolution logic in opSchema.py so both tools use the
    same default connection without requiring a --dsn flag on every invocation.
    """
    env_file   = _PROJECT_ROOT / "cfg" / f"{_DEFAULT_ENV}.env"
    file_values = _loadSimpleEnvFile(env_file)
    return os.environ.get("EOD_DSN", file_values.get("EOD_DSN", _DSN_FALLBACK))


DSN = _resolveDsn()

YAHOO_TYPE_MAP = {
    "EQUITY": "equity",
    "ETF": "etf",
    "MUTUALFUND": "etf",
    "INDEX": "index",
}


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass
class PriceRow:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @classmethod
    def fromCsv(cls, row: dict[str, str]) -> PriceRow:
        """Parse a CSV dict row. Raises ValueError on bad data."""
        return cls(
            symbol=row["symbol"].upper().strip(),
            trade_date=date.fromisoformat(row["trade_date"].strip()),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(float(row["volume"])),  # int(float()) handles "1000.0"
        )

    def isValid(self) -> bool:
        """OHLC sanity check."""
        return (
            self.high >= self.low
            and self.high >= self.open
            and self.high >= self.close
            and self.low <= self.open
            and self.low <= self.close
            and self.open > 0
            and self.close > 0
            and self.volume >= 0
        )


# ── Helpers ──────────────────────────────────────────────────────────────────

def loadSymbolMap(conn: psycopg.Connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, instrument_id FROM instrument")
        return dict(cur.fetchall())


def autoAddInstruments(
    conn: psycopg.Connection,
    symbols: set[str],
    exchange: str = "ASX",
    currency: str = "AUD",
) -> dict[str, int]:
    sym_map = loadSymbolMap(conn)
    new_syms = symbols - set(sym_map)

    if not new_syms:
        return sym_map

    # Do not commit inside this helper.
    #
    # `prices` needs stub creation and price upserts to behave like one atomic
    # unit of work: either both parts land, or neither does. If we commit here
    # and the later `eod_price` upsert fails, we leave behind newly-created
    # instruments with no matching prices. That partial state is surprising for
    # operators and changes the behavior of reruns, because those symbols are no
    # longer considered "unknown" on the second attempt.
    #
    # By leaving the transaction open, the caller can commit only after the
    # actual ingest succeeds, and psycopg will roll everything back together if
    # an exception escapes the surrounding connection context.
    with conn.cursor() as cur:
        for sym in sorted(new_syms):
            cur.execute(
                """INSERT INTO instrument (symbol, name, exchange, currency)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (symbol) DO NOTHING
                   RETURNING instrument_id""",
                (sym, f"[{sym}]", exchange, currency),
            )
            row = cur.fetchone()
            if row:
                sym_map[sym] = row[0]

    created_syms = sorted(new_syms & set(sym_map))
    print(f"✓ Auto-created {len(created_syms)} instrument stub(s): {created_syms}")
    return sym_map


def readCsvPrices(path: Path) -> tuple[list[PriceRow], list[tuple[int, str]]]:
    """Read CSV, return (valid_rows, [(line_num, error_msg), ...])."""
    rows: list[PriceRow] = []
    errors: list[tuple[int, str]] = []

    with open(path, newline="") as f:
        reader = csv.DictReader(f)

        # Check required columns
        required = {"symbol", "trade_date", "open", "high", "low", "close", "volume"}
        if reader.fieldnames is None:
            print("Empty CSV file.", file=sys.stderr)
            sys.exit(1)
        missing = required - set(reader.fieldnames)
        if missing:
            print(f"CSV missing columns: {missing}", file=sys.stderr)
            sys.exit(1)

        for i, raw in enumerate(reader, start=2):  # line 1 is header
            try:
                pr = PriceRow.fromCsv(raw)
            except (ValueError, KeyError) as e:
                errors.append((i, f"parse error: {e}"))
                continue

            if not pr.isValid():
                errors.append((i, f"OHLC sanity failed: {pr.symbol} {pr.trade_date}"))
                continue

            rows.append(pr)

    return rows, errors


# ── Price ingestion ──────────────────────────────────────────────────────────

UPSERT_SQL = """
INSERT INTO eod_price (trade_date, instrument_id, open, high, low, close, volume)
VALUES (%(trade_date)s, %(instrument_id)s, %(open)s, %(high)s, %(low)s,
        %(close)s, %(volume)s)
ON CONFLICT (instrument_id, trade_date)
DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
    close = EXCLUDED.close, volume = EXCLUDED.volume
"""


def cmdPrices(args):
    path = Path(args.csv)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    rows, errors = readCsvPrices(path)

    if errors:
        print(f"⚠  {len(errors)} rows skipped:", file=sys.stderr)
        for line_num, msg in errors[:10]:
            print(f"   line {line_num}: {msg}", file=sys.stderr)
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more", file=sys.stderr)

    if not rows:
        print("No valid rows to ingest.", file=sys.stderr)
        sys.exit(1)

    all_symbols = {r.symbol for r in rows}

    with psycopg.connect(args.dsn) as conn:
        # Keep the whole ingest in a single transaction.
        #
        # This command may do two writes in sequence:
        # 1. create missing instrument stubs
        # 2. upsert prices that reference those instruments
        #
        # Those writes are logically inseparable. A failure in step 2 should not
        # leave step 1 committed, otherwise the database ends up in a "half
        # ingested" state that is hard to reason about operationally. The
        # connection context will commit on normal exit and roll back on errors,
        # so we deliberately avoid intermediate commits before the final upsert
        # has succeeded.
        if args.auto_add:
            sym_map = autoAddInstruments(conn, all_symbols, args.exchange, args.currency)
        else:
            sym_map = loadSymbolMap(conn)
            unknown = all_symbols - set(sym_map)
            if unknown:
                print(f"⚠  Unknown symbols (skipped): {sorted(unknown)}", file=sys.stderr)
                rows = [r for r in rows if r.symbol in sym_map]

        if not rows:
            print("Nothing to ingest.", file=sys.stderr)
            sys.exit(1)

        params = [
            {
                "trade_date": r.trade_date,
                "instrument_id": sym_map[r.symbol],
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in rows
        ]

        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, params)
        conn.commit()

        dates = {r.trade_date for r in rows}
        print(f"✓ Upserted {len(rows)} rows across {len(dates)} date(s)")


# ── Enrichment from Yahoo Finance ────────────────────────────────────────────

def fetchYahooInfo(symbol: str, exchange: str = "ASX") -> dict | None:
    try:
        import yfinance as yf
    except ImportError:
        print("Install yfinance:  pip install yfinance", file=sys.stderr)
        sys.exit(1)

    suffix_map = {
        "ASX": ".AX", "LSE": ".L", "NYSE": "", "NASDAQ": "",
        "TSX": ".TO", "HKG": ".HK",
    }
    ticker = f"{symbol}{suffix_map.get(exchange, '')}"

    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            return None
    except Exception:
        return None

    return {
        "name": info.get("longName") or info.get("shortName") or f"[{symbol}]",
        "sector": info.get("sector"),
        "instrument_type": YAHOO_TYPE_MAP.get(info.get("quoteType", ""), "equity"),
        "currency": info.get("currency", "AUD"),
    }


def cmdEnrich(args):
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if args.symbol:
                cur.execute(
                    "SELECT instrument_id, symbol, exchange FROM instrument WHERE symbol = %s",
                    (args.symbol.upper(),),
                )
            else:
                cur.execute(
                    """SELECT instrument_id, symbol, exchange FROM instrument
                        WHERE name LIKE '[%%]' OR name = symbol"""
                )
            stubs = cur.fetchall()

        if not stubs:
            print("Nothing to enrich — all instruments have metadata.")
            return

        print(f"Enriching {len(stubs)} instrument(s) from Yahoo Finance...")
        enriched = 0
        failed = []

        for row in stubs:
            sym = row["symbol"]
            info = fetchYahooInfo(sym, row["exchange"])

            if info is None:
                failed.append(sym)
                print(f"  ✗ {sym} — not found on Yahoo Finance")
                time.sleep(0.3)
                continue

            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE instrument
                          SET name = %s,
                              sector = COALESCE(%s, sector),
                              instrument_type = %s,
                              currency = %s
                        WHERE instrument_id = %s""",
                    (info["name"], info["sector"], info["instrument_type"],
                     info["currency"], row["instrument_id"]),
                )
            # Commit per-instrument intentionally: enrichment is a slow,
            # best-effort loop against an external API. Committing each update
            # immediately preserves progress if the loop is interrupted midway
            # (network error, rate limit, KeyboardInterrupt), avoiding the need
            # to re-fetch metadata for symbols that were already enriched.
            conn.commit()
            enriched += 1
            print(f"  ✓ {sym} → {info['name']}  ({info['instrument_type']}, {info['sector'] or '—'})")
            time.sleep(0.5)

        print(f"\nDone: {enriched} enriched, {len(failed)} failed")


# ── Instrument management ───────────────────────────────────────────────────

def cmdAddInstrument(args):
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO instrument (symbol, name, exchange, instrument_type, currency, sector)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (symbol) DO UPDATE SET
                       name = EXCLUDED.name,
                       exchange = EXCLUDED.exchange,
                       instrument_type = EXCLUDED.instrument_type,
                       currency = EXCLUDED.currency,
                       sector = EXCLUDED.sector
                   RETURNING instrument_id""",
                (args.symbol.upper(), args.name, args.exchange, args.type, args.currency, args.sector),
            )
            iid = cur.fetchone()[0]
        conn.commit()
        print(f"✓ instrument_id={iid}  {args.symbol.upper()}  {args.name}")


def cmdListInstruments(args):
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT instrument_id, symbol, name, instrument_type,
                          currency, sector, is_active
                     FROM instrument ORDER BY symbol"""
            )
            rows = cur.fetchall()

    if not rows:
        print("No instruments registered.")
        return

    print(f"{'ID':>4}  {'Symbol':<8} {'Type':<8} {'Cur':<4} {'Sector':<14} {'Name'}")
    print("-" * 70)
    for r in rows:
        flag = "" if r["is_active"] else " [inactive]"
        print(
            f"{r['instrument_id']:>4}  {r['symbol']:<8} {r['instrument_type']:<8} "
            f"{r['currency']:<4} {(r['sector'] or '—'):<14} {r['name']}{flag}"
        )


def cmdImportInstruments(args):
    path = Path(args.csv)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    count = 0
    total = 0
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "symbol" not in reader.fieldnames or "name" not in reader.fieldnames:
            print("CSV must have at least 'symbol' and 'name' columns.", file=sys.stderr)
            sys.exit(1)

        with psycopg.connect(args.dsn) as conn:
            with conn.cursor() as cur:
                for row in reader:
                    total += 1
                    cur.execute(
                        """INSERT INTO instrument (symbol, name, instrument_type, currency, sector)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (symbol) DO NOTHING""",
                        (
                            row["symbol"].upper().strip(),
                            row["name"],
                            row.get("instrument_type", "equity"),
                            row.get("currency", "AUD"),
                            row.get("sector"),
                        ),
                    )
                    count += cur.rowcount
            conn.commit()

    print(f"✓ Imported {count} new instruments ({total - count} already existed)")


# ── Corporate action management ─────────────────────────────────────────────

def cmdAddAction(args):
    try:
        ex_date = date.fromisoformat(args.ex_date)
    except ValueError:
        print(f"Invalid --ex-date '{args.ex_date}': expected YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    with psycopg.connect(args.dsn) as conn:
        sym_map = loadSymbolMap(conn)
        sym = args.symbol.upper()
        if sym not in sym_map:
            print(f"Unknown symbol: {sym}", file=sys.stderr)
            sys.exit(1)

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO corporate_action
                       (instrument_id, ex_date, action_type, factor, description)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING action_id""",
                (sym_map[sym], ex_date, args.action_type, args.factor, args.desc),
            )
            aid = cur.fetchone()[0]
        conn.commit()
        print(f"✓ action_id={aid}  {sym}  {args.action_type}  factor={args.factor}  ex={args.ex_date}")


def cmdListActions(args):
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        sym_map = loadSymbolMap(conn)
        sym = args.symbol.upper()
        if sym not in sym_map:
            print(f"Unknown symbol: {sym}", file=sys.stderr)
            sys.exit(1)

        with conn.cursor() as cur:
            cur.execute(
                """SELECT action_id, ex_date, action_type, factor, description
                     FROM corporate_action
                    WHERE instrument_id = %s
                    ORDER BY ex_date DESC""",
                (sym_map[sym],),
            )
            rows = cur.fetchall()

    if not rows:
        print(f"No corporate actions for {sym}.")
        return

    print(f"Corporate actions for {sym}:")
    print(f"{'ID':>4}  {'Ex-date':<12} {'Type':<16} {'Factor':>10}  Description")
    print("-" * 72)
    for r in rows:
        print(
            f"{r['action_id']:>4}  {r['ex_date']!s:<12} {r['action_type']:<16} "
            f"{r['factor']:>10}  {r['description'] or ''}"
        )


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compact EOD data ingestion tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
workflow:
  1. Ingest prices (instruments auto-created as stubs):
       python src/loadEodData.py prices data/2026-04-09.csv

  2. Backfill metadata from Yahoo Finance:
       python src/loadEodData.py enrich

  3. Record corporate actions as they happen:
       python src/loadEodData.py add-action BHP split 2.0 --ex-date 2026-06-01

examples:
  python src/loadEodData.py prices data/2026-04-09.csv
  python src/loadEodData.py prices data/2026-04-09.csv --no-auto-add
  python src/loadEodData.py prices data/2026-04-09.csv --exchange NYSE --currency USD
  python src/loadEodData.py enrich
  python src/loadEodData.py enrich --symbol BHP
  python src/loadEodData.py list-instruments
  python src/loadEodData.py add-action CBA dividend 2.10 --ex-date 2026-03-20
  python src/loadEodData.py list-actions BHP
        """,
    )
    parser.add_argument("--dsn", default=DSN, help="PostgreSQL connection string")
    sub = parser.add_subparsers(dest="command", required=True)

    # prices
    p_prices = sub.add_parser("prices", help="Upsert EOD prices from CSV")
    p_prices.add_argument("csv", help="Path to CSV file")
    p_prices.add_argument(
        "--no-auto-add", dest="auto_add", action="store_false", default=True,
        help="Skip unknown symbols instead of auto-creating them",
    )
    p_prices.add_argument("--exchange", default="ASX", help="Default exchange (default: ASX)")
    p_prices.add_argument("--currency", default="AUD", help="Default currency (default: AUD)")

    # enrich
    p_enrich = sub.add_parser("enrich", help="Backfill metadata from Yahoo Finance")
    p_enrich.add_argument("--symbol", default=None, help="Enrich one symbol (default: all stubs)")

    # add-instrument
    p_add = sub.add_parser("add-instrument", help="Add or update a single instrument")
    p_add.add_argument("symbol")
    p_add.add_argument("name")
    p_add.add_argument("--exchange", default="ASX")
    p_add.add_argument("--type", default="equity", choices=["equity", "etf", "reit", "index"])
    p_add.add_argument("--currency", default="AUD")
    p_add.add_argument("--sector", default=None)

    # list-instruments
    sub.add_parser("list-instruments", help="List all instruments")

    # import-instruments
    p_imp = sub.add_parser("import-instruments", help="Bulk import instruments from CSV")
    p_imp.add_argument("csv", help="CSV: symbol, name, [instrument_type, currency, sector]")

    # add-action
    p_act = sub.add_parser("add-action", help="Record a corporate action")
    p_act.add_argument("symbol")
    p_act.add_argument("action_type", choices=["split", "reverse_split", "dividend", "spinoff", "merger"])
    p_act.add_argument("factor", type=float)
    p_act.add_argument("--ex-date", required=True, help="Ex-date YYYY-MM-DD")
    p_act.add_argument("--desc", default=None, help="Description")

    # list-actions
    p_la = sub.add_parser("list-actions", help="List corporate actions for a symbol")
    p_la.add_argument("symbol")

    args = parser.parse_args()

    dispatch = {
        "prices": cmdPrices,
        "enrich": cmdEnrich,
        "add-instrument": cmdAddInstrument,
        "list-instruments": cmdListInstruments,
        "import-instruments": cmdImportInstruments,
        "add-action": cmdAddAction,
        "list-actions": cmdListActions,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
