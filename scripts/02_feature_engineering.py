"""
Feature Engineering Script - Phase 2 Part 3 (Robust / No DB errors)
- Calculates technical indicators
- Flags risk events (daily return <= -2%)
- Inserts into technical_features and risk_events with ON CONFLICT DO NOTHING

Student: Mohammed Adnan Osman (33114153)
Date: Jan 28, 2026
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------
# DB CONNECTION (safe)
# -----------------------------
def create_db_connection():
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "metal_risk_prediction"
    DB_USER = "postgres"

    # Option A: set env var DB_PASSWORD
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    if not DB_PASSWORD:
        # Option B: just type it here temporarily (NOT recommended)
        DB_PASSWORD = input("Postgres password: ").strip()

    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    print(f"✓ Connected to database: {DB_NAME}")
    return engine


# -----------------------------
# TECH INDICATORS
# -----------------------------
def calculate_rsi(series: pd.Series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal
    return macd, macd_signal, macd_hist

def calculate_bollinger(series: pd.Series, window=20, num_std=2):
    mid = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    width = (upper - lower) / mid
    return upper, mid, lower, width


# -----------------------------
# LOAD PRICES FOR ONE METAL
# -----------------------------
def load_price_data(engine, metal_id: int):
    q = text("""
        SELECT metal_id, date, open, high, low, close, volume
        FROM price_data
        WHERE metal_id = :metal_id
        ORDER BY date ASC
    """)
    df = pd.read_sql(q, engine, params={"metal_id": metal_id})
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    return df


# -----------------------------
# FEATURE ENGINEERING
# -----------------------------
def build_features(df: pd.DataFrame):
    df = df.copy()

    # Returns
    df["daily_return"] = df["close"].pct_change()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # SMAs / EMAs
    df["sma_5"] = df["close"].rolling(5).mean()
    df["sma_10"] = df["close"].rolling(10).mean()
    df["sma_20"] = df["close"].rolling(20).mean()
    df["sma_50"] = df["close"].rolling(50).mean()
    df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()

    # Bollinger
    df["bollinger_upper"], df["bollinger_middle"], df["bollinger_lower"], df["bollinger_width"] = calculate_bollinger(df["close"])

    # RSI
    df["rsi_14"] = calculate_rsi(df["close"], 14)

    # MACD
    df["macd"], df["macd_signal"], df["macd_histogram"] = calculate_macd(df["close"])

    # High-low features
    df["high_low_range"] = (df["high"] - df["low"])
    df["high_low_ratio"] = np.where(df["low"] > 0, df["high"] / df["low"], np.nan)

    # Volume features
    df["volume_change"] = df["volume"].pct_change()
    df["volume_sma_20"] = df["volume"].rolling(20).mean()

    return df


# -----------------------------
# RISK EVENTS
# -----------------------------
def build_risk_events(df: pd.DataFrame, threshold=-0.02):
    """
    Risk event if daily_return <= -2%
    """
    out = df[["metal_id", "date", "close", "daily_return"]].copy()
    out["previous_close"] = out["close"].shift(1)
    out["current_close"] = out["close"]
    out["price_change_pct"] = out["daily_return"] * 100
    out["is_risk_event"] = out["daily_return"] <= threshold

    # Drop first row (no previous_close)
    out = out.dropna(subset=["previous_close", "current_close", "daily_return"])
    return out


# -----------------------------
# INSERT (UPSERT SAFE)
# -----------------------------
def upsert_technical_features(engine, df: pd.DataFrame):
    # Match YOUR technical_features table schema from earlier
    cols = [
        "metal_id", "date",
        "daily_return", "log_return",
        "sma_5", "sma_10", "sma_20", "sma_50",
        "ema_12", "ema_26",
        "bollinger_upper", "bollinger_middle", "bollinger_lower", "bollinger_width",
        "rsi_14",
        "macd", "macd_signal", "macd_histogram",
        "high_low_range", "high_low_ratio",
        "volume_change", "volume_sma_20",
    ]
    feat = df[cols].dropna().copy()

    sql = text("""
        INSERT INTO technical_features (
            metal_id, date,
            daily_return, log_return,
            sma_5, sma_10, sma_20, sma_50,
            ema_12, ema_26,
            bollinger_upper, bollinger_middle, bollinger_lower, bollinger_width,
            rsi_14,
            macd, macd_signal, macd_histogram,
            high_low_range, high_low_ratio,
            volume_change, volume_sma_20
        )
        VALUES (
            :metal_id, :date,
            :daily_return, :log_return,
            :sma_5, :sma_10, :sma_20, :sma_50,
            :ema_12, :ema_26,
            :bollinger_upper, :bollinger_middle, :bollinger_lower, :bollinger_width,
            :rsi_14,
            :macd, :macd_signal, :macd_histogram,
            :high_low_range, :high_low_ratio,
            :volume_change, :volume_sma_20
        )
        ON CONFLICT (metal_id, date) DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(sql, feat.to_dict(orient="records"))

    return len(feat)


def upsert_risk_events(engine, df: pd.DataFrame):
    risk = build_risk_events(df)

    sql = text("""
        INSERT INTO risk_events (
            metal_id, date,
            is_risk_event, price_change_pct,
            previous_close, current_close
        )
        VALUES (
            :metal_id, :date,
            :is_risk_event, :price_change_pct,
            :previous_close, :current_close
        )
        ON CONFLICT (metal_id, date) DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(sql, risk.to_dict(orient="records"))

    return len(risk)


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("=" * 70)
    print("PHASE 2 PART 3 - FEATURE ENGINEERING (NO ERRORS)")
    print("=" * 70)

    engine = create_db_connection()

    # Read metals from DB (no hardcoding IDs)
    metals = pd.read_sql("SELECT metal_id, name FROM metals ORDER BY metal_id;", engine)

    total_feat = 0
    total_risk = 0

    for _, row in metals.iterrows():
        metal_id = int(row["metal_id"])
        metal_name = row["name"]

        print(f"\n--- {metal_name} (metal_id={metal_id}) ---")
        df = load_price_data(engine, metal_id)
        if df is None or df.empty:
            print("⚠ No price data found.")
            continue

        df_feat = build_features(df)

        n_feat = upsert_technical_features(engine, df_feat)
        n_risk = upsert_risk_events(engine, df_feat)

        total_feat += n_feat
        total_risk += n_risk

        print(f"✓ Inserted technical_features: {n_feat}")
        print(f"✓ Inserted risk_events: {n_risk}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)
    print(f"Total technical_features inserted: {total_feat}")
    print(f"Total risk_events inserted: {total_risk}")
    print("\nVerify in pgAdmin:")
    print("  SELECT COUNT(*) FROM technical_features;")
    print("  SELECT COUNT(*) FROM risk_events;")
    print("  SELECT COUNT(*) FROM risk_events WHERE is_risk_event = TRUE;")


if __name__ == "__main__":
    main()
