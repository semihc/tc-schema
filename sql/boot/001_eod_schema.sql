-- ============================================================================
-- Compact EOD Schema — IDEMPOTENT MIGRATION
-- Safe to run repeatedly against the same database.
--
-- PostgreSQL >= 18  +  TimescaleDB >= 2.11
--
-- Every statement uses IF NOT EXISTS, DO blocks, or ON CONFLICT
-- so re-running this script changes nothing if already applied.
-- ============================================================================

BEGIN;

-- --------------------------------------------------------------------------
-- 0. Extensions
-- --------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- --------------------------------------------------------------------------
-- 1. Instruments
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument (
    instrument_id   SERIAL       PRIMARY KEY,
    symbol          VARCHAR(16)  NOT NULL UNIQUE,
    name            TEXT         NOT NULL,
    exchange        VARCHAR(10)  NOT NULL DEFAULT 'ASX',
    instrument_type VARCHAR(10)  NOT NULL DEFAULT 'equity'
                    CHECK (instrument_type IN ('equity','etf','reit','index')),
    currency        CHAR(3)     NOT NULL DEFAULT 'AUD',
    sector          TEXT,
    ipo_date        DATE,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE
);

-- --------------------------------------------------------------------------
-- 2. EOD Prices
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eod_price (
    trade_date      DATE          NOT NULL,
    instrument_id   INT           NOT NULL,
    open            NUMERIC(12,4) NOT NULL,
    high            NUMERIC(12,4) NOT NULL,
    low             NUMERIC(12,4) NOT NULL,
    close           NUMERIC(12,4) NOT NULL,
    volume          BIGINT        NOT NULL DEFAULT 0,

    CONSTRAINT eod_ohlc_sanity CHECK (
        high >= low AND high >= open AND high >= close
        AND low <= open AND low <= close
    ),
    CONSTRAINT eod_positive CHECK (open > 0 AND close > 0 AND volume >= 0)
);

-- Hypertable (idempotent via if_not_exists)
SELECT create_hypertable('eod_price', 'trade_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists       => TRUE
);

-- Indexes: CREATE INDEX IF NOT EXISTS is native PostgreSQL
CREATE UNIQUE INDEX IF NOT EXISTS idx_eod_pk
    ON eod_price (instrument_id, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_eod_date
    ON eod_price (trade_date DESC);

-- Compression (idempotent — will no-op if already set)
DO $$
BEGIN
    -- Only set compression if not already enabled
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables
         WHERE hypertable_name = 'eod_price'
           AND compression_enabled
    ) THEN
        ALTER TABLE eod_price SET (
            timescaledb.compress,
            timescaledb.compress_segmentby  = 'instrument_id',
            timescaledb.compress_orderby    = 'trade_date DESC'
        );
    END IF;
END $$;

-- Compression policy (idempotent — catches duplicate policy error)
DO $$
BEGIN
    PERFORM add_compression_policy('eod_price', INTERVAL '6 months');
EXCEPTION
    WHEN duplicate_object THEN NULL;  -- policy already exists
END $$;

-- --------------------------------------------------------------------------
-- 3. Corporate Actions
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporate_action (
    action_id       SERIAL       PRIMARY KEY,
    instrument_id   INT          NOT NULL REFERENCES instrument(instrument_id),
    ex_date         DATE         NOT NULL,
    action_type     VARCHAR(16)  NOT NULL
                    CHECK (action_type IN ('split','reverse_split',
                                           'dividend','spinoff','merger')),
    factor          NUMERIC(12,6) NOT NULL,
    description     TEXT
);

CREATE INDEX IF NOT EXISTS idx_corpact
    ON corporate_action (instrument_id, ex_date DESC);

-- --------------------------------------------------------------------------
-- 4. Views (CREATE OR REPLACE is inherently idempotent)
-- --------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_latest_price AS
SELECT DISTINCT ON (instrument_id)
    i.symbol, i.name,
    p.trade_date, p.open, p.high, p.low, p.close, p.volume
FROM eod_price p
JOIN instrument i USING (instrument_id)
ORDER BY instrument_id, trade_date DESC;

CREATE OR REPLACE VIEW v_adjustment_factor AS
SELECT
    ca.instrument_id,
    ca.ex_date,
    ca.action_type,
    CASE
        WHEN ca.action_type IN ('split', 'reverse_split')
            THEN 1.0 / ca.factor
        WHEN ca.action_type = 'dividend'
            THEN (p.close - ca.factor) / p.close
        ELSE 1.0
    END AS day_factor
FROM corporate_action ca
LEFT JOIN eod_price p
    ON  p.instrument_id = ca.instrument_id
    AND p.trade_date    = ca.ex_date;

CREATE OR REPLACE VIEW v_cumulative_factor AS
SELECT
    instrument_id,
    ex_date,
    EXP(
        SUM(LN(day_factor)) OVER (
            PARTITION BY instrument_id
            ORDER BY ex_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    ) AS cumulative_factor
FROM v_adjustment_factor
WHERE day_factor > 0 AND day_factor != 1.0;

CREATE OR REPLACE VIEW v_adjusted_price AS
SELECT
    p.trade_date,
    p.instrument_id,
    p.open, p.high, p.low, p.close, p.volume,
    ROUND(p.close * COALESCE(cf.cumulative_factor, 1.0), 4) AS adj_close,
    ROUND(p.open  * COALESCE(cf.cumulative_factor, 1.0), 4) AS adj_open,
    ROUND(p.high  * COALESCE(cf.cumulative_factor, 1.0), 4) AS adj_high,
    ROUND(p.low   * COALESCE(cf.cumulative_factor, 1.0), 4) AS adj_low,
    COALESCE(cf.cumulative_factor, 1.0) AS factor_applied
FROM eod_price p
LEFT JOIN LATERAL (
    SELECT cumulative_factor
      FROM v_cumulative_factor c
     WHERE c.instrument_id = p.instrument_id
       AND c.ex_date > p.trade_date
     ORDER BY c.ex_date ASC
     LIMIT 1
) cf ON TRUE;

CREATE OR REPLACE VIEW v_daily_return AS
SELECT
    instrument_id, trade_date, adj_close,
    (adj_close - lag(adj_close) OVER w)
        / NULLIF(lag(adj_close) OVER w, 0) AS return_pct
FROM v_adjusted_price
WINDOW w AS (PARTITION BY instrument_id ORDER BY trade_date);

-- --------------------------------------------------------------------------
-- 5. Schema version tracking
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_version (
    version     INT          PRIMARY KEY,
    description TEXT         NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO schema_version (version, description)
VALUES (1, 'Initial compact EOD schema')
ON CONFLICT (version) DO NOTHING;

COMMIT;
