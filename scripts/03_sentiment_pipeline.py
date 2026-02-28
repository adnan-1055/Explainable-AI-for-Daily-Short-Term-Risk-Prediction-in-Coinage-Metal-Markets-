"""
FinBERT Sentiment Analysis Pipeline - Phase 2 Part 4
- Collects financial news headlines from GDELT (historical) and NewsAPI (recent)
- Scores headlines using FinBERT sentiment model
- Aggregates daily sentiment per metal
- Stores results in sentiment_data and daily_sentiment tables

Student: Mohammed Adnan Osman (33114153)
Date: Feb 2026
"""

import os
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# FinBERT imports
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# =============================================================
# CONFIG
# =============================================================
NEWSAPI_KEY = None  # Set your key here or use env var NEWSAPI_KEY

# Search terms for each metal
METAL_KEYWORDS = {
    "GOLD":   ["gold price", "gold market", "gold futures", "gold trading", "gold commodity"],
    "SILVER": ["silver price", "silver market", "silver futures", "silver trading", "silver commodity"],
    "COPPER": ["copper price", "copper market", "copper futures", "copper trading", "copper commodity"],
}


# =============================================================
# DB CONNECTION
# =============================================================
def create_db_connection():
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "metal_risk_prediction"
    DB_USER = "postgres"

    DB_PASSWORD = os.getenv("DB_PASSWORD")
    if not DB_PASSWORD:
        DB_PASSWORD = input("Postgres password: ").strip()

    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    print(f"✓ Connected to database: {DB_NAME}")
    return engine


# =============================================================
# FINBERT MODEL (loads once, reused for all headlines)
# =============================================================
class FinBERTAnalyzer:
    def __init__(self):
        print("Loading FinBERT model (first time may download ~400MB)...")
        model_name = "ProsusAI/finbert"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()  # Set to evaluation mode
        self.labels = ["positive", "negative", "neutral"]
        print("✓ FinBERT model loaded successfully")

    def analyze(self, headlines: list) -> list:
        """
        Score a batch of headlines.
        Returns list of dicts with sentiment_label, sentiment_score,
        positive_score, negative_score, neutral_score
        """
        if not headlines:
            return []

        results = []
        # Process in batches of 32 to avoid memory issues
        batch_size = 32
        for i in range(0, len(headlines), batch_size):
            batch = headlines[i:i + batch_size]

            inputs = self.tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=128,
                return_tensors="pt"
            )

            with torch.no_grad():
                outputs = self.model(**inputs)

            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            for j, probs in enumerate(probabilities):
                pos, neg, neu = probs[0].item(), probs[1].item(), probs[2].item()
                label_idx = torch.argmax(probs).item()

                # Sentiment score: positive = +1, negative = -1, neutral = 0
                sentiment_score = pos - neg

                results.append({
                    "headline": batch[j],
                    "sentiment_label": self.labels[label_idx],
                    "sentiment_score": sentiment_score,
                    "positive_score": pos,
                    "negative_score": neg,
                    "neutral_score": neu,
                })

        return results


# =============================================================
# GDELT NEWS COLLECTION (historical + recent, no API key needed)
# =============================================================
def collect_gdelt_headlines(keyword: str, start_date: str, end_date: str, max_records=250):
    """
    Fetch article headlines from GDELT DOC API.
    GDELT reliably covers last 3 months; older dates may return fewer results.
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": f'"{keyword}" sourcelang:english',
        "mode": "artlist",
        "maxrecords": str(max_records),
        "format": "json",
        "startdatetime": start_date.replace("-", "") + "000000",
        "enddatetime": end_date.replace("-", "") + "235959",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return []

        data = resp.json()
        articles = data.get("articles", [])

        results = []
        for art in articles:
            title = art.get("title", "").strip()
            seen = art.get("seendate", "")
            domain = art.get("domain", "")

            if title and seen:
                # Parse GDELT date format: "20250215T120000Z"
                try:
                    dt = datetime.strptime(seen[:8], "%Y%m%d").date()
                except ValueError:
                    continue

                results.append({
                    "date": dt,
                    "headline": title,
                    "source": domain,
                    "data_source": "gdelt",
                })

        return results

    except Exception as e:
        print(f"  ⚠ GDELT error for '{keyword}': {e}")
        return []


def collect_gdelt_for_metal(metal_symbol: str, start_date: str, end_date: str):
    """
    Collect headlines for all keywords associated with a metal.
    Iterates in monthly chunks to maximize coverage.
    """
    keywords = METAL_KEYWORDS[metal_symbol]
    all_headlines = []

    # Split into monthly chunks for better coverage
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current < end:
        chunk_end = min(current + timedelta(days=30), end)
        chunk_start_str = current.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d")

        for kw in keywords:
            articles = collect_gdelt_headlines(kw, chunk_start_str, chunk_end_str)
            all_headlines.extend(articles)
            time.sleep(1)  # Be polite to the API

        current = chunk_end + timedelta(days=1)

    # Deduplicate by headline text
    seen = set()
    unique = []
    for h in all_headlines:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            unique.append(h)

    return unique


# =============================================================
# NEWSAPI COLLECTION (recent 30 days, requires free API key)
# =============================================================
def collect_newsapi_headlines(keyword: str, api_key: str, page_size=100):
    """
    Fetch recent headlines from NewsAPI (free tier = last 30 days).
    """
    if not api_key:
        return []

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": keyword,
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": page_size,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  ⚠ NewsAPI error ({resp.status_code}) for '{keyword}'")
            return []

        data = resp.json()
        articles = data.get("articles", [])

        results = []
        for art in articles:
            title = art.get("title", "").strip()
            pub = art.get("publishedAt", "")
            source_name = art.get("source", {}).get("name", "")

            if title and pub and title != "[Removed]":
                try:
                    dt = datetime.strptime(pub[:10], "%Y-%m-%d").date()
                except ValueError:
                    continue

                results.append({
                    "date": dt,
                    "headline": title,
                    "source": source_name,
                    "data_source": "newsapi",
                })

        return results

    except Exception as e:
        print(f"  ⚠ NewsAPI error for '{keyword}': {e}")
        return []


def collect_newsapi_for_metal(metal_symbol: str, api_key: str):
    """Collect recent headlines from NewsAPI for a metal."""
    if not api_key:
        return []

    keywords = METAL_KEYWORDS[metal_symbol]
    all_headlines = []

    for kw in keywords:
        articles = collect_newsapi_headlines(kw, api_key)
        all_headlines.extend(articles)
        time.sleep(0.5)

    # Deduplicate
    seen = set()
    unique = []
    for h in all_headlines:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            unique.append(h)

    return unique


# =============================================================
# DATABASE INSERT
# =============================================================
def insert_sentiment_data(engine, metal_id: int, scored_headlines: list):
    """Insert individual scored headlines into sentiment_data table."""
    if not scored_headlines:
        return 0

    sql = text("""
        INSERT INTO sentiment_data (
            metal_id, date, headline, source,
            sentiment_label, sentiment_score,
            positive_score, negative_score, neutral_score,
            data_source
        )
        VALUES (
            :metal_id, :date, :headline, :source,
            :sentiment_label, :sentiment_score,
            :positive_score, :negative_score, :neutral_score,
            :data_source
        )
        ON CONFLICT (metal_id, date, headline) DO NOTHING;
    """)

    records = []
    for h in scored_headlines:
        records.append({
            "metal_id": metal_id,
            "date": h["date"],
            "headline": h["headline"][:500],  # Truncate very long headlines
            "source": h.get("source", "")[:100],
            "sentiment_label": h["sentiment_label"],
            "sentiment_score": float(h["sentiment_score"]),
            "positive_score": float(h["positive_score"]),
            "negative_score": float(h["negative_score"]),
            "neutral_score": float(h["neutral_score"]),
            "data_source": h.get("data_source", "gdelt"),
        })

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(records)


def aggregate_daily_sentiment(engine, metal_id: int):
    """
    Aggregate individual headlines into daily sentiment features.
    These daily aggregates become model input features.
    """
    sql = text("""
        INSERT INTO daily_sentiment (
            metal_id, date,
            avg_sentiment, avg_positive, avg_negative, avg_neutral,
            headline_count, positive_ratio, negative_ratio, sentiment_std
        )
        SELECT
            metal_id,
            date,
            AVG(sentiment_score)   AS avg_sentiment,
            AVG(positive_score)    AS avg_positive,
            AVG(negative_score)    AS avg_negative,
            AVG(neutral_score)     AS avg_neutral,
            COUNT(*)               AS headline_count,
            AVG(CASE WHEN sentiment_label = 'positive' THEN 1.0 ELSE 0.0 END) AS positive_ratio,
            AVG(CASE WHEN sentiment_label = 'negative' THEN 1.0 ELSE 0.0 END) AS negative_ratio,
            COALESCE(STDDEV(sentiment_score), 0) AS sentiment_std
        FROM sentiment_data
        WHERE metal_id = :metal_id
        GROUP BY metal_id, date
        ON CONFLICT (metal_id, date) DO UPDATE SET
            avg_sentiment  = EXCLUDED.avg_sentiment,
            avg_positive   = EXCLUDED.avg_positive,
            avg_negative   = EXCLUDED.avg_negative,
            avg_neutral    = EXCLUDED.avg_neutral,
            headline_count = EXCLUDED.headline_count,
            positive_ratio = EXCLUDED.positive_ratio,
            negative_ratio = EXCLUDED.negative_ratio,
            sentiment_std  = EXCLUDED.sentiment_std;
    """)

    with engine.begin() as conn:
        conn.execute(sql, {"metal_id": metal_id})


# =============================================================
# MAIN PIPELINE
# =============================================================
def main():
    print("=" * 70)
    print("PHASE 2 PART 4 - FINBERT SENTIMENT PIPELINE")
    print("=" * 70)

    # DB connection
    engine = create_db_connection()

    # Get metal mappings
    metals = pd.read_sql("SELECT metal_id, symbol, name FROM metals ORDER BY metal_id;", engine)

    # Load FinBERT model (once)
    finbert = FinBERTAnalyzer()

    # Check for NewsAPI key
    api_key = NEWSAPI_KEY or os.getenv("NEWSAPI_KEY")
    if api_key:
        print(f"✓ NewsAPI key found - will collect recent headlines")
    else:
        print("⚠ No NewsAPI key - using GDELT only (set NEWSAPI_KEY env var to add NewsAPI)")

    # Date range: try to cover as much as GDELT will give us
    # GDELT reliably gives last 3 months, sometimes more
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = "2024-11-01"  # ~3 months back for reliable GDELT coverage

    print(f"\nDate range: {start_date} to {end_date}")
    print(f"Note: GDELT covers ~3 months reliably. Older data may be sparse.\n")

    for _, row in metals.iterrows():
        metal_id = int(row["metal_id"])
        metal_symbol = row["symbol"]
        metal_name = row["name"]

        print(f"\n{'='*50}")
        print(f"--- {metal_name} ({metal_symbol}) ---")
        print(f"{'='*50}")

        # 1. Collect from GDELT
        print(f"  Collecting GDELT headlines...")
        gdelt_headlines = collect_gdelt_for_metal(metal_symbol, start_date, end_date)
        print(f"  ✓ GDELT: {len(gdelt_headlines)} unique headlines")

        # 2. Collect from NewsAPI (if key available)
        newsapi_headlines = []
        if api_key:
            print(f"  Collecting NewsAPI headlines...")
            newsapi_headlines = collect_newsapi_for_metal(metal_symbol, api_key)
            print(f"  ✓ NewsAPI: {len(newsapi_headlines)} unique headlines")

        # 3. Combine and deduplicate
        all_headlines = gdelt_headlines + newsapi_headlines
        seen = set()
        unique_headlines = []
        for h in all_headlines:
            if h["headline"] not in seen:
                seen.add(h["headline"])
                unique_headlines.append(h)

        print(f"  Total unique headlines: {len(unique_headlines)}")

        if not unique_headlines:
            print(f"  ⚠ No headlines found for {metal_name}. Skipping.")
            continue

        # 4. Score with FinBERT
        print(f"  Running FinBERT sentiment analysis...")
        headline_texts = [h["headline"] for h in unique_headlines]
        scores = finbert.analyze(headline_texts)

        # Merge scores back with metadata
        scored = []
        for h, s in zip(unique_headlines, scores):
            scored.append({**h, **s})

        # 5. Insert into database
        print(f"  Inserting into sentiment_data...")
        n_inserted = insert_sentiment_data(engine, metal_id, scored)
        print(f"  ✓ Inserted: {n_inserted} rows")

        # 6. Aggregate daily sentiment
        print(f"  Aggregating daily sentiment...")
        aggregate_daily_sentiment(engine, metal_id)
        print(f"  ✓ Daily sentiment aggregated")

    # Final verification
    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM sentiment_data;"))
        sent_count = result.scalar()
        result = conn.execute(text("SELECT COUNT(*) FROM daily_sentiment;"))
        daily_count = result.scalar()

        print(f"  sentiment_data rows:  {sent_count}")
        print(f"  daily_sentiment rows: {daily_count}")

        result = conn.execute(text("""
            SELECT m.name,
                   COUNT(s.sentiment_id) AS headlines,
                   COUNT(DISTINCT s.date) AS days_covered,
                   MIN(s.date) AS first_date,
                   MAX(s.date) AS last_date
            FROM metals m
            LEFT JOIN sentiment_data s ON m.metal_id = s.metal_id
            GROUP BY m.name ORDER BY m.name;
        """))
        for row in result:
            print(f"  {row[0]}: {row[1]} headlines across {row[2]} days ({row[3]} to {row[4]})")

    print("\n✓ SENTIMENT PIPELINE COMPLETE")
    print("\nVerify in pgAdmin:")
    print("  SELECT COUNT(*) FROM sentiment_data;")
    print("  SELECT COUNT(*) FROM daily_sentiment;")
    print("  SELECT sentiment_label, COUNT(*) FROM sentiment_data GROUP BY sentiment_label;")


if __name__ == "__main__":
    main()
