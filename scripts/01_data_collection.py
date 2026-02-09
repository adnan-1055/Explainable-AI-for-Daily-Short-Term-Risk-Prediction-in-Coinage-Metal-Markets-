"""
PHASE 2 DATA COLLECTION (Level 6, robust)
- Pull OHLCV for Gold/Silver/Copper from yFinance
- Pull macro (DXY, VIX, TNX, S&P500)
- Insert into PostgreSQL with ON CONFLICT DO NOTHING (no duplicate crashes)

Works with newer yfinance versions (handles MultiIndex columns + Date/Datetime).
"""

import getpass
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values

# -------------------------------
# DB CONFIG
# -------------------------------
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "metal_risk_prediction"
DB_USER = "postgres"


def connect_db():
    pwd = getpass.getpass("Postgres password: ")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=pwd
    )
    conn.autocommit = False
    print(f"✓ Connected to DB: {DB_NAME}")
    return conn


def get_metal_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, metal_id, yfinance_ticker FROM metals ORDER BY metal_id;")
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("metals table is empty. Run the SQL insert into metals first.")
    return {sym: {"id": mid, "ticker": tkr} for sym, mid, tkr in rows}


def _flatten_yfinance_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance can return MultiIndex columns like ('Open','GC=F').
    This flattens them to 'Open', 'High', etc.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df


def download_prices(ticker, start="2020-01-01", end="2025-12-31"):
    # Force stable output
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    if df is None or df.empty:
        return None

    df = _flatten_yfinance_columns(df)

    # Turn index into a column
    df = df.reset_index()

    # yfinance sometimes uses Datetime instead of Date
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    elif "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "date"})
    else:
        # If neither exists, use the first column as date
        df = df.rename(columns={df.columns[0]: "date"})

    # Standardise names (case-insensitive)
    colmap = {}
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "open":
            colmap[c] = "open"
        elif cl == "high":
            colmap[c] = "high"
        elif cl == "low":
            colmap[c] = "low"
        elif cl == "close":
            colmap[c] = "close"
        elif cl == "adj close" or cl == "adj_close":
            colmap[c] = "adjusted_close"
        elif cl == "volume":
            colmap[c] = "volume"

    df = df.rename(columns=colmap)

    # If adjusted_close missing, add it as None
    if "adjusted_close" not in df.columns:
        df["adjusted_close"] = None

    required = ["date", "open", "high", "low", "close"]
    for r in required:
        if r not in df.columns:
            raise RuntimeError(f"yfinance output missing column '{r}'. Columns found: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"]).dt.date

    keep = ["date", "open", "high", "low", "close", "volume", "adjusted_close"]
    df = df[keep].dropna(subset=["date", "open", "high", "low", "close"])

    return df


def insert_price_data(conn, metal_id, df):
    if df is None or df.empty:
        print("⚠ No price data returned.")
        return 0

    records = []
    for _, r in df.iterrows():
        records.append((
            metal_id,
            r["date"],
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            None if pd.isna(r["volume"]) else int(r["volume"]),
            None if pd.isna(r["adjusted_close"]) else float(r["adjusted_close"]),
            "yfinance"
        ))

    sql = """
    INSERT INTO price_data
      (metal_id, date, open, high, low, close, volume, adjusted_close, data_source)
    VALUES %s
    ON CONFLICT (metal_id, date) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=2000)

    return len(records)


def download_macro(start="2020-01-01", end="2025-12-31"):
    tickers = {
        "DX-Y.NYB": "usd_index",
        "^VIX": "vix",
        "^TNX": "treasury_yield_10y",
        "^GSPC": "sp500_close",
    }

    frames = []
    for tkr, col in tickers.items():
        df = yf.download(tkr, start=start, end=end, progress=False, auto_adjust=False)
        if df is None or df.empty:
            raise RuntimeError(f"No macro data returned for {tkr}")

        df = _flatten_yfinance_columns(df)
        df = df.reset_index()

        if "Date" in df.columns:
            df = df.rename(columns={"Date": "date"})
        elif "Datetime" in df.columns:
            df = df.rename(columns={"Datetime": "date"})
        else:
            df = df.rename(columns={df.columns[0]: "date"})

        # prefer Close
        if "Close" in df.columns:
            df = df[["date", "Close"]].rename(columns={"Close": col})
        elif "close" in df.columns:
            df = df[["date", "close"]].rename(columns={"close": col})
        else:
            raise RuntimeError(f"Macro df missing Close for {tkr}. Columns: {list(df.columns)}")

        frames.append(df)

    macro = frames[0]
    for f in frames[1:]:
        macro = macro.merge(f, on="date", how="outer")

    macro = macro.sort_values("date").ffill()
    macro["sp500_return"] = macro["sp500_close"].pct_change()
    macro = macro.dropna(subset=["usd_index", "vix", "treasury_yield_10y", "sp500_close", "sp500_return"])

    macro["date"] = pd.to_datetime(macro["date"]).dt.date
    return macro


def insert_macro(conn, df):
    if df is None or df.empty:
        print("⚠ No macro data returned.")
        return 0

    records = []
    for _, r in df.iterrows():
        records.append((
            r["date"],
            float(r["usd_index"]),
            float(r["vix"]),
            float(r["treasury_yield_10y"]),
            float(r["sp500_close"]),
            float(r["sp500_return"]),
            "yfinance"
        ))

    sql = """
    INSERT INTO macroeconomic_data
      (date, usd_index, vix, treasury_yield_10y, sp500_close, sp500_return, data_source)
    VALUES %s
    ON CONFLICT (date) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=2000)

    return len(records)


def verify_counts(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM price_data;")
        price_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM macroeconomic_data;")
        macro_n = cur.fetchone()[0]

        cur.execute("""
            SELECT m.name, COUNT(p.price_id) AS rows, MIN(p.date), MAX(p.date)
            FROM metals m
            LEFT JOIN price_data p ON m.metal_id = p.metal_id
            GROUP BY m.name
            ORDER BY m.name;
        """)
        coverage = cur.fetchall()

    print("\n====================")
    print("VERIFY COUNTS")
    print("====================")
    print(f"price_data rows: {price_n}")
    print(f"macroeconomic_data rows: {macro_n}")
    print("\nMetal coverage:")
    for name, rows, dmin, dmax in coverage:
        print(f"  {name}: {rows} rows | {dmin} -> {dmax}")


def main():
    print("=" * 70)
    print("PHASE 2 DATA COLLECTION (ROBUST)")
    print("=" * 70)

    conn = None
    try:
        conn = connect_db()
        metal_map = get_metal_map(conn)

        for sym in ["GOLD", "SILVER", "COPPER"]:
            mid = metal_map[sym]["id"]
            tkr = metal_map[sym]["ticker"]

            print(f"\n--- {sym} ({tkr}) ---")
            df_prices = download_prices(tkr)
            n = insert_price_data(conn, mid, df_prices)
            conn.commit()
            print(f"✓ Insert attempted: {n} rows (duplicates ignored)")

        print("\n--- MACRO (DXY, VIX, TNX, S&P500) ---")
        df_macro = download_macro()
        n = insert_macro(conn, df_macro)
        conn.commit()
        print(f"✓ Insert attempted: {n} rows (duplicates ignored)")

        verify_counts(conn)
        print("\n✓ DONE.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n✗ ERROR: {e}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
