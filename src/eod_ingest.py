#!/usr/bin/env python3
"""
eod_ingest.py — Compact EOD ingestion for private investors.

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
    python src/eod_ingest.py prices data/2026-04-09.csv
    python src/eod_ingest.py prices data/2026-04-09.csv --no-auto-add
    python src/eod_ingest.py enrich
    python src/eod_ingest.py enrich --symbol BHP
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DSN = "postgresql://localhost:5432/marketdata"

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
    def from_csv(cls, row: dict[str, str]) -> PriceRow:
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

    def is_valid(self) -> bool:
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

def load_symbol_map(conn: psycopg.Connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, instrument_id FROM instrument")
        return dict(cur.fetchall())


def auto_add_instruments(
    conn: psycopg.Connection,
    symbols: set[str],
    exchange: str = "ASX",
    currency: str = "AUD",
) -> dict[str, int]:
    sym_map = load_symbol_map(conn)
    new_syms = symbols - set(sym_map)

    if not new_syms:
        return sym_map

    with conn.cursor() as cur:
        for sym in sorted(new_syms):
            cur.execute(
                """INSERT INTO instrument (symbol, name, exchange, currency)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (symbol) DO NOTHING
                   RETURNING instrument_id""",
                (sym, f"[{sym}]", exchange, currency),
            )
            cur.fetchone()

    conn.commit()
    updated_map = load_symbol_map(conn)
    created_syms = sorted(set(updated_map) - set(sym_map))
    print(f"✓ Auto-created {len(created_syms)} instrument stub(s): {created_syms}")
    return updated_map


def read_csv_prices(path: Path) -> tuple[list[PriceRow], list[tuple[int, str]]]:
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
                pr = PriceRow.from_csv(raw)
            except (ValueError, KeyError) as e:
                errors.append((i, f"parse error: {e}"))
                continue

            if not pr.is_valid():
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


def cmd_prices(args):
    path = Path(args.csv)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    rows, errors = read_csv_prices(path)

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
        if args.auto_add:
            sym_map = auto_add_instruments(conn, all_symbols, args.exchange, args.currency)
        else:
            sym_map = load_symbol_map(conn)
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

def fetch_yahoo_info(symbol: str, exchange: str = "ASX") -> dict | None:
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


def cmd_enrich(args):
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
            info = fetch_yahoo_info(sym, row["exchange"])

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
            conn.commit()
            enriched += 1
            print(f"  ✓ {sym} → {info['name']}  ({info['instrument_type']}, {info['sector'] or '—'})")
            time.sleep(0.5)

        print(f"\nDone: {enriched} enriched, {len(failed)} failed")


# ── Instrument management ───────────────────────────────────────────────────

def cmd_add_instrument(args):
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO instrument (symbol, name, instrument_type, currency, sector)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (symbol) DO UPDATE SET
                       name = EXCLUDED.name,
                       instrument_type = EXCLUDED.instrument_type,
                       currency = EXCLUDED.currency,
                       sector = EXCLUDED.sector
                   RETURNING instrument_id""",
                (args.symbol.upper(), args.name, args.type, args.currency, args.sector),
            )
            iid = cur.fetchone()[0]
        conn.commit()
        print(f"✓ instrument_id={iid}  {args.symbol.upper()}  {args.name}")


def cmd_list_instruments(args):
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


def cmd_import_instruments(args):
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

def cmd_add_action(args):
    with psycopg.connect(args.dsn) as conn:
        sym_map = load_symbol_map(conn)
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
                (sym_map[sym], args.ex_date, args.action_type, args.factor, args.desc),
            )
            aid = cur.fetchone()[0]
        conn.commit()
        print(f"✓ action_id={aid}  {sym}  {args.action_type}  factor={args.factor}  ex={args.ex_date}")


def cmd_list_actions(args):
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        sym_map = load_symbol_map(conn)
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
       python src/eod_ingest.py prices data/2026-04-09.csv

  2. Backfill metadata from Yahoo Finance:
       python src/eod_ingest.py enrich

  3. Record corporate actions as they happen:
       python src/eod_ingest.py add-action BHP split 2.0 --ex-date 2026-06-01

examples:
  python src/eod_ingest.py prices data/2026-04-09.csv
  python src/eod_ingest.py prices data/2026-04-09.csv --no-auto-add
  python src/eod_ingest.py prices data/2026-04-09.csv --exchange NYSE --currency USD
  python src/eod_ingest.py enrich
  python src/eod_ingest.py enrich --symbol BHP
  python src/eod_ingest.py list-instruments
  python src/eod_ingest.py add-action CBA dividend 2.10 --ex-date 2026-03-20
  python src/eod_ingest.py list-actions BHP
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
        "prices": cmd_prices,
        "enrich": cmd_enrich,
        "add-instrument": cmd_add_instrument,
        "list-instruments": cmd_list_instruments,
        "import-instruments": cmd_import_instruments,
        "add-action": cmd_add_action,
        "list-actions": cmd_list_actions,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
