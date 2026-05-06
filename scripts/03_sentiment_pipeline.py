
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
NEWSAPI_KEY = "f8259ccfc0f8474fa508484d028694b4"

# Search terms for each metal
METAL_KEYWORDS = {
    "GOLD":   ["gold price", "gold market", "gold futures"],
    "SILVER": ["silver price", "silver market", "silver futures"],
    "COPPER": ["copper price", "copper market", "copper futures"],
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
    """
    Uses the pre-trained FinBERT model to classify financial text
    as positive, negative, or neutral. FinBERT is used here purely
    as a feature extraction tool - the output scores become input
    features for the Random Forest classifier.
    """

    def __init__(self):
        print("Loading FinBERT model...")
        model_name = "ProsusAI/finbert"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()  # Set to evaluation mode (no training)
        self.labels = ["positive", "negative", "neutral"]
        print("✓ FinBERT model loaded successfully")

    def analyze(self, headlines: list) -> list:

        if not headlines:
            return []

        results = []

        # Process in batches of 32 to manage memory
        batch_size = 32
        for i in range(0, len(headlines), batch_size):
            batch = headlines[i:i + batch_size]

            # Tokenize the text for FinBERT
            inputs = self.tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=128,
                return_tensors="pt"
            )

            # Get predictions without computing gradients (saves memory)
            with torch.no_grad():
                outputs = self.model(**inputs)

            # Convert raw outputs to probabilities using softmax
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            for j, probs in enumerate(probabilities):
                pos = probs[0].item()   # Positive probability
                neg = probs[1].item()   # Negative probability
                neu = probs[2].item()   # Neutral probability
                label_idx = torch.argmax(probs).item()

                # Overall sentiment score: ranges from -1 (negative) to +1 (positive)
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
# NEWSAPI COLLECTION (recent headlines, free tier = last 30 days)
# =============================================================
def collect_newsapi_headlines(keyword: str, api_key: str, page_size=100):
    """
    Fetch recent headlines from NewsAPI matching the keyword.

    Args:
        keyword: search term (e.g. "gold price")
        api_key: NewsAPI API key
        page_size: max articles to return (up to 100)

    Returns:
        list of dicts with date, headline, source, data_source
    """
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": keyword,
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": page_size,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
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

            # Skip removed or empty articles
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


def collect_headlines_for_metal(metal_symbol: str, api_key: str):
    """
    Collect and deduplicate headlines for all keywords of a metal.

    Args:
        metal_symbol: "GOLD", "SILVER", or "COPPER"
        api_key: NewsAPI key

    Returns:
        list of unique headline dicts
    """
    keywords = METAL_KEYWORDS[metal_symbol]
    all_headlines = []

    for kw in keywords:
        print(f"    Searching: '{kw}'...")
        articles = collect_newsapi_headlines(kw, api_key)
        all_headlines.extend(articles)
        time.sleep(0.5)  # Small delay between requests

    # Remove duplicate headlines
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
            "headline": h["headline"][:500],
            "source": h.get("source", "")[:100],
            "sentiment_label": h["sentiment_label"],
            "sentiment_score": float(h["sentiment_score"]),
            "positive_score": float(h["positive_score"]),
            "negative_score": float(h["negative_score"]),
            "neutral_score": float(h["neutral_score"]),
            "data_source": h.get("data_source", "newsapi"),
        })

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(records)


def aggregate_daily_sentiment(engine, metal_id: int):
    """
    Aggregate individual headline scores into daily sentiment features.
    These daily aggregates become input features for the Random Forest model.

    Features created:
        avg_sentiment:  mean sentiment score for the day (-1 to +1)
        avg_positive:   mean positive probability
        avg_negative:   mean negative probability
        avg_neutral:    mean neutral probability
        headline_count: number of headlines that day
        positive_ratio: proportion of headlines classified as positive
        negative_ratio: proportion of headlines classified as negative
        sentiment_std:  standard deviation of sentiment (measures disagreement)
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

    # Step 1: Connect to database
    engine = create_db_connection()

    # Step 2: Get metal mappings from DB
    metals = pd.read_sql(
        "SELECT metal_id, symbol, name FROM metals ORDER BY metal_id;",
        engine
    )

    # Step 3: Load FinBERT model (once, reused for all metals)
    finbert = FinBERTAnalyzer()

    # Step 4: Check API key
    api_key = NEWSAPI_KEY or os.getenv("NEWSAPI_KEY")
    if not api_key:
        print("✗ No NewsAPI key found. Set NEWSAPI_KEY in the script or as env var.")
        return

    print(f"✓ NewsAPI key found")

    # Step 5: Process each metal
    for _, row in metals.iterrows():
        metal_id = int(row["metal_id"])
        metal_symbol = row["symbol"]
        metal_name = row["name"]

        print(f"\n{'='*50}")
        print(f"--- {metal_name} ({metal_symbol}) ---")
        print(f"{'='*50}")

        # 5a. Collect headlines from NewsAPI
        print(f"  Collecting headlines from NewsAPI...")
        headlines = collect_headlines_for_metal(metal_symbol, api_key)
        print(f"  ✓ Found {len(headlines)} unique headlines")

        if not headlines:
            print(f"  ⚠ No headlines found for {metal_name}. Skipping.")
            continue

        # 5b. Score headlines with FinBERT
        print(f"  Running FinBERT sentiment analysis...")
        headline_texts = [h["headline"] for h in headlines]
        scores = finbert.analyze(headline_texts)

        # Merge scores with headline metadata
        scored = []
        for h, s in zip(headlines, scores):
            scored.append({**h, **s})

        # 5c. Insert into sentiment_data table
        print(f"  Inserting into database...")
        n_inserted = insert_sentiment_data(engine, metal_id, scored)
        print(f"  ✓ Inserted: {n_inserted} headline scores")

        # 5d. Aggregate into daily_sentiment table
        print(f"  Aggregating daily sentiment features...")
        aggregate_daily_sentiment(engine, metal_id)
        print(f"  ✓ Daily sentiment aggregated")

    # Step 6: Verification
    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM sentiment_data;"))
        sent_count = result.scalar()
        result = conn.execute(text("SELECT COUNT(*) FROM daily_sentiment;"))
        daily_count = result.scalar()

        print(f"  Total headlines scored:    {sent_count}")
        print(f"  Daily sentiment records:   {daily_count}")

        # Breakdown by metal
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
            print(f"  {row[0]}: {row[1]} headlines | {row[2]} days | {row[3]} to {row[4]}")

        # Sentiment distribution
        result = conn.execute(text("""
            SELECT sentiment_label, COUNT(*)
            FROM sentiment_data
            GROUP BY sentiment_label
            ORDER BY sentiment_label;
        """))
        print("\n  Sentiment distribution:")
        for row in result:
            print(f"    {row[0]}: {row[1]}")

    print("\n✓ SENTIMENT PIPELINE COMPLETE")


if __name__ == "__main__":
    main()
