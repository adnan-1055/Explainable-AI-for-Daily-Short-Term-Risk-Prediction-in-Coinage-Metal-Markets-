-- =========================================================
-- METAL RISK PREDICTION DATABASE (FINAL SCHEMA)
-- Student: Mohammed Adnan Osman (33114153)
-- Project: Explainable AI for Short-Term Risk Prediction
--          in Coinage Metal Markets
-- =========================================================
-- Usage: Run this file once in pgAdmin to create all tables.
--        All tables use IF NOT EXISTS so it's safe to re-run.
-- =========================================================

-- 1) METALS
CREATE TABLE IF NOT EXISTS metals (
    metal_id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(50) NOT NULL,
    yfinance_ticker VARCHAR(20) NOT NULL,
    market_type VARCHAR(20)
);

INSERT INTO metals (symbol, name, yfinance_ticker, market_type)
VALUES
    ('GOLD',   'Gold',   'GC=F', 'precious'),
    ('SILVER', 'Silver', 'SI=F', 'precious'),
    ('COPPER', 'Copper', 'HG=F', 'industrial')
ON CONFLICT (symbol) DO NOTHING;


-- 2) PRICE DATA
CREATE TABLE IF NOT EXISTS price_data (
    price_id   SERIAL PRIMARY KEY,
    metal_id   INTEGER NOT NULL REFERENCES metals(metal_id) ON DELETE CASCADE,
    date       DATE NOT NULL,
    open       NUMERIC(12, 4) NOT NULL CHECK (open > 0),
    high       NUMERIC(12, 4) NOT NULL CHECK (high > 0),
    low        NUMERIC(12, 4) NOT NULL CHECK (low > 0),
    close      NUMERIC(12, 4) NOT NULL CHECK (close > 0),
    volume     BIGINT CHECK (volume >= 0),
    adjusted_close NUMERIC(12, 4),
    data_source VARCHAR(50) DEFAULT 'yfinance',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metal_id, date)
);

CREATE INDEX IF NOT EXISTS idx_price_date       ON price_data(date);
CREATE INDEX IF NOT EXISTS idx_price_metal_date ON price_data(metal_id, date);


-- 3) MACROECONOMIC DATA
CREATE TABLE IF NOT EXISTS macroeconomic_data (
    macro_id           SERIAL PRIMARY KEY,
    date               DATE UNIQUE NOT NULL,
    usd_index          NUMERIC(10, 4) CHECK (usd_index > 0),
    vix                NUMERIC(10, 4) CHECK (vix >= 0),
    treasury_yield_10y NUMERIC(10, 4) CHECK (treasury_yield_10y >= 0),
    sp500_close        NUMERIC(12, 4) CHECK (sp500_close > 0),
    sp500_return       NUMERIC(18, 10),
    data_source        VARCHAR(50) DEFAULT 'yfinance',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_macro_date ON macroeconomic_data(date);


-- 4) TECHNICAL FEATURES
CREATE TABLE IF NOT EXISTS technical_features (
    feature_id SERIAL PRIMARY KEY,
    metal_id   INTEGER NOT NULL REFERENCES metals(metal_id) ON DELETE CASCADE,
    date       DATE NOT NULL,

    -- Returns
    daily_return NUMERIC(18, 10),
    log_return   NUMERIC(18, 10),

    -- Simple Moving Averages
    sma_5  NUMERIC(20, 6),
    sma_10 NUMERIC(20, 6),
    sma_20 NUMERIC(20, 6),
    sma_50 NUMERIC(20, 6),

    -- Exponential Moving Averages
    ema_12 NUMERIC(20, 6),
    ema_26 NUMERIC(20, 6),

    -- Bollinger Bands
    bollinger_upper  NUMERIC(20, 6),
    bollinger_middle NUMERIC(20, 6),
    bollinger_lower  NUMERIC(20, 6),
    bollinger_width  NUMERIC(18, 10),

    -- RSI
    rsi_14 NUMERIC(6, 2),

    -- MACD
    macd           NUMERIC(20, 6),
    macd_signal    NUMERIC(20, 6),
    macd_histogram NUMERIC(20, 6),

    -- Price Range
    high_low_range NUMERIC(20, 6),
    high_low_ratio NUMERIC(18, 10),

    -- Volume
    volume_change NUMERIC(18, 10),
    volume_sma_20 NUMERIC(20, 2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metal_id, date)
);

CREATE INDEX IF NOT EXISTS idx_features_metal_date ON technical_features(metal_id, date);
CREATE INDEX IF NOT EXISTS idx_features_date       ON technical_features(date);


-- 5) RISK EVENTS
CREATE TABLE IF NOT EXISTS risk_events (
    event_id         SERIAL PRIMARY KEY,
    metal_id         INTEGER NOT NULL REFERENCES metals(metal_id) ON DELETE CASCADE,
    date             DATE NOT NULL,
    is_risk_event    BOOLEAN NOT NULL,
    price_change_pct NUMERIC(18, 10),
    previous_close   NUMERIC(12, 4),
    current_close    NUMERIC(12, 4),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metal_id, date)
);

CREATE INDEX IF NOT EXISTS idx_risk_events_metal_date ON risk_events(metal_id, date);
CREATE INDEX IF NOT EXISTS idx_risk_events_target     ON risk_events(is_risk_event);


-- =========================================================
-- VERIFICATION QUERIES
-- Run these after populating data to check everything works
-- =========================================================

SELECT current_database();

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT COUNT(*) AS price_rows        FROM price_data;
SELECT COUNT(*) AS macro_rows        FROM macroeconomic_data;
SELECT COUNT(*) AS feature_rows      FROM technical_features;
SELECT COUNT(*) AS risk_rows         FROM risk_events;

SELECT
    m.name,
    COUNT(p.price_id) AS rows,
    MIN(p.date)       AS start_date,
    MAX(p.date)       AS end_date
FROM metals m
LEFT JOIN price_data p ON m.metal_id = p.metal_id
GROUP BY m.name
ORDER BY m.name;