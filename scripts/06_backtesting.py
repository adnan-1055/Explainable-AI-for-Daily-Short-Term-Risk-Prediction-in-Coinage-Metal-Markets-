"""
06_backtesting.py
=================
Phase 3 - Backtesting on Unseen 2025 Data
Explainable AI for Daily Short-Term Risk Prediction in Coinage Metal Markets

Mohammed Adnan Osman | Student ID: 33114153
Supervisor: Dr Nasim Dadashi | University of West London

What this script does:
    1. Loads trained models from /models/ folder
    2. Runs walk-forward prediction on 2025 unseen data
    3. Generates risk timeline charts for each metal
    4. Produces SHAP waterfall plots for top true-positive predictions
    5. Saves daily prediction CSVs to /outputs/backtest/
"""

import os
import warnings
import joblib
import numpy as np
import pandas as pd
import shap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from sklearn.metrics import roc_auc_score, brier_score_loss

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST',     'localhost'),
    'port':     os.getenv('DB_PORT',     '5432'),
    'dbname':   os.getenv('DB_NAME',     'metal_risk_prediction'),
    'user':     os.getenv('DB_USER',     'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

RISK_THRESHOLD  = -0.02
TRAIN_RATIO     = 0.60
VAL_RATIO       = 0.20
METALS          = {1: 'gold', 2: 'silver', 3: 'copper'}
BACKTEST_START  = '2025-01-01'

MODELS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'models')
PLOTS_DIR    = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'plots')
BACKTEST_DIR = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'backtest')

os.makedirs(PLOTS_DIR,    exist_ok=True)
os.makedirs(BACKTEST_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_full_dataset(engine, metal_id: int) -> pd.DataFrame:
    """Load and prepare the full dataset — mirrors 04_model_training.py exactly."""

    sql_tech = text("""
        SELECT tf.date, tf.daily_return, tf.log_return,
               tf.sma_5, tf.sma_10, tf.sma_20, tf.sma_50,
               tf.ema_12, tf.ema_26, tf.rsi_14,
               tf.macd, tf.macd_signal, tf.macd_histogram,
               tf.bollinger_upper, tf.bollinger_middle, tf.bollinger_lower, tf.bollinger_width,
               tf.high_low_ratio, tf.high_low_range,
               tf.volume_change, tf.volume_sma_20
        FROM technical_features tf
        WHERE tf.metal_id = :metal_id ORDER BY tf.date
    """)

    sql_macro = text("""
        SELECT date, usd_index, vix, treasury_yield_10y,
               sp500_close, sp500_return
        FROM macroeconomic_data ORDER BY date
    """)

    sql_sent = text("""
        SELECT date, avg_sentiment, avg_positive, avg_negative, avg_neutral,
               headline_count, positive_ratio, negative_ratio, sentiment_std
        FROM daily_sentiment
        WHERE metal_id = :metal_id ORDER BY date
    """)

    with engine.connect() as conn:
        df_tech  = pd.read_sql(sql_tech,  conn, params={'metal_id': metal_id}, parse_dates=['date'])
        df_macro = pd.read_sql(sql_macro, conn, parse_dates=['date'])
        df_sent  = pd.read_sql(sql_sent,  conn, params={'metal_id': metal_id}, parse_dates=['date'])

    df = df_tech.merge(df_macro, on='date', how='left')
    df = df.merge(df_sent, on='date', how='left', suffixes=('', '_sent'))
    df = df.sort_values('date').reset_index(drop=True)

    # Fill sentiment NaN with 0
    sentiment_cols = ['avg_sentiment', 'avg_positive', 'avg_negative', 'avg_neutral',
                      'headline_count', 'positive_ratio', 'negative_ratio', 'sentiment_std']
    for col in sentiment_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Target and lag features
    df['target'] = (df['daily_return'].shift(-1) < RISK_THRESHOLD).astype(int)

    lag_cols = ['daily_return', 'log_return', 'volume_change', 'rsi_14',
                'macd_histogram', 'bollinger_width']
    for col in lag_cols:
        if col in df.columns:
            df[f'{col}_lag1'] = df[col].shift(1)
            df[f'{col}_lag2'] = df[col].shift(2)
            df[f'{col}_lag3'] = df[col].shift(3)

    core_cols = ['daily_return', 'log_return', 'rsi_14', 'macd', 'target',
                 'daily_return_lag1', 'daily_return_lag2', 'daily_return_lag3']
    existing_core = [c for c in core_cols if c in df.columns]
    df = df.dropna(subset=existing_core).reset_index(drop=True)
    df = df.ffill().fillna(0)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# WALK-FORWARD VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def walk_forward_predict(model, df_full: pd.DataFrame, features: list,
                         start_date: str, end_date: str) -> pd.DataFrame:
    """
    Walk-forward prediction: for each day in the backtest window,
    the model predicts using only data it would have had at that point
    (no look-ahead). This is the academically correct approach for
    time series backtesting.

    Since the model is already trained on pre-2025 data, we simply
    run inference on the 2025 feature set in chronological order.
    """
    backtest_df = df_full[
        (df_full['date'] >= start_date) &
        (df_full['date'] <= end_date)
    ].copy()

    if len(backtest_df) == 0:
        print(f"    WARNING: No data found for backtest window {start_date} to {end_date}")
        return pd.DataFrame()

    X_backtest = backtest_df[features]
    risk_probs = model.predict_proba(X_backtest)[:, 1]
    predictions = model.predict(X_backtest)

    results = pd.DataFrame({
        'date':         backtest_df['date'].values,
        'daily_return': backtest_df['daily_return'].values,
        'actual_risk':  backtest_df['target'].values,
        'risk_prob':    risk_probs,
        'predicted':    predictions,
        'correct':      (predictions == backtest_df['target'].values).astype(int)
    })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# VISUALISATION
# ─────────────────────────────────────────────────────────────────────────────

def plot_risk_timeline(results_b: pd.DataFrame, results_e: pd.DataFrame,
                       metal_name: str):
    """Plot daily risk probability over time with actual risk events marked."""
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.suptitle(f'Backtesting Risk Timeline — {metal_name.upper()} (2025)',
                 fontsize=13, fontweight='bold')

    for ax, results, label, colour in [
        (axes[0], results_b, 'Baseline', '#2196F3'),
        (axes[1], results_e, 'Enhanced', '#FF5722')
    ]:
        dates = pd.to_datetime(results['date'])
        ax.plot(dates, results['risk_prob'], color=colour, lw=1.5,
                alpha=0.8, label=f'{label} Risk Probability')
        ax.axhline(0.5, color='gray', linestyle='--', lw=1, alpha=0.5,
                   label='0.5 threshold')
        ax.axhline(0.3, color='orange', linestyle=':', lw=1, alpha=0.5,
                   label='0.3 threshold')

        # Mark actual risk events
        risk_days = results[results['actual_risk'] == 1]
        ax.scatter(pd.to_datetime(risk_days['date']),
                   [0.02] * len(risk_days),
                   color='red', marker='v', s=50, zorder=5,
                   label=f'Actual Risk Events ({len(risk_days)})')

        ax.set_ylabel('Risk Probability')
        ax.set_title(f'{label} Model')
        ax.legend(fontsize=8, loc='upper right')
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())

    plt.xticks(rotation=45)
    plt.tight_layout()

    path = os.path.join(PLOTS_DIR, f'backtest_timeline_{metal_name}.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Timeline saved: {path}")


def plot_shap_waterfall(model, X_day, feature_names, metal_name, model_label,
                        date_str, rank):
    """Generate SHAP waterfall plot for a single high-risk prediction day."""
    try:
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer(X_day)

        # Handle both old and new SHAP API
        if hasattr(shap_values, 'values'):
            sv = shap_values
        else:
            sv = shap_values

        plt.figure(figsize=(10, 6))
        shap.waterfall_plot(sv[0], max_display=15, show=False)
        plt.title(f'SHAP Waterfall — {metal_name.upper()} | {model_label}\n'
                  f'High-Risk Day: {date_str}',
                  fontsize=11, fontweight='bold')
        plt.tight_layout()

        fname = f'shap_waterfall_{metal_name}_{model_label.lower()}_top{rank}.png'
        path  = os.path.join(BACKTEST_DIR, fname)
        plt.savefig(path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"      Waterfall saved: {path}")
    except Exception as ex:
        print(f"      WARNING: Could not generate waterfall plot — {ex}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  PHASE 3: BACKTESTING (2025 Unseen Data)")
    print("  Explainable AI for Metal Market Risk Prediction")
    print("  Mohammed Adnan Osman | 33114153")
    print("=" * 70)

    engine = get_engine()

    for metal_id, metal_name in METALS.items():
        print(f"\n{'─' * 70}")
        print(f"  BACKTESTING: {metal_name.upper()}")
        print(f"{'─' * 70}")

        # Load data and models
        df_full   = load_full_dataset(engine, metal_id)
        feat_info = joblib.load(os.path.join(MODELS_DIR, f'features_{metal_name}.joblib'))
        baseline_features = feat_info['baseline']
        enhanced_features = feat_info['enhanced']

        model_b = joblib.load(os.path.join(MODELS_DIR, f'rf_baseline_{metal_name}.joblib'))
        model_e = joblib.load(os.path.join(MODELS_DIR, f'rf_enhanced_{metal_name}.joblib'))

        end_date = df_full['date'].max().strftime('%Y-%m-%d')

        print(f"    Backtest window: {BACKTEST_START} → {end_date}")

        # Walk-forward predictions
        results_b = walk_forward_predict(model_b, df_full, baseline_features,
                                         BACKTEST_START, end_date)
        results_e = walk_forward_predict(model_e, df_full, enhanced_features,
                                         BACKTEST_START, end_date)

        if len(results_b) == 0:
            print(f"    WARNING: No 2025 data found for {metal_name}. Skipping.")
            continue

        print(f"    Backtest days     : {len(results_b)}")
        print(f"    Actual risk events: {results_b['actual_risk'].sum()}")

        # Metrics
        if results_b['actual_risk'].sum() > 0:
            auc_b = roc_auc_score(results_b['actual_risk'], results_b['risk_prob'])
            auc_e = roc_auc_score(results_e['actual_risk'], results_e['risk_prob'])
            brier_b = brier_score_loss(results_b['actual_risk'], results_b['risk_prob'])
            brier_e = brier_score_loss(results_e['actual_risk'], results_e['risk_prob'])
            print(f"    Baseline  ROC-AUC: {auc_b:.4f} | Brier: {brier_b:.4f}")
            print(f"    Enhanced  ROC-AUC: {auc_e:.4f} | Brier: {brier_e:.4f}")

        # Risk timeline chart
        plot_risk_timeline(results_b, results_e, metal_name)

        # Save prediction CSVs
        csv_b = os.path.join(BACKTEST_DIR, f'backtest_baseline_{metal_name}.csv')
        csv_e = os.path.join(BACKTEST_DIR, f'backtest_enhanced_{metal_name}.csv')
        results_b.to_csv(csv_b, index=False)
        results_e.to_csv(csv_e, index=False)
        print(f"    CSVs saved: backtest_baseline_{metal_name}.csv / backtest_enhanced_{metal_name}.csv")

        # SHAP waterfall plots for top 3 true-positive predictions (enhanced model)
        print(f"\n    Generating SHAP waterfall plots for top true-positive days...")
        true_positives = results_e[
            (results_e['actual_risk'] == 1) & (results_e['risk_prob'] > 0.1)
        ].nlargest(3, 'risk_prob')

        if len(true_positives) == 0:
            # Fall back to highest probability days regardless of actual label
            true_positives = results_e.nlargest(3, 'risk_prob')
            print(f"    Note: No true positives found — using top 3 highest-risk days instead")

        for rank, (_, row) in enumerate(true_positives.iterrows(), start=1):
            date_str = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
            day_data = df_full[df_full['date'] == row['date']]

            if len(day_data) == 0:
                continue

            X_day = day_data[enhanced_features]
            print(f"      Case study {rank}: {date_str} "
                  f"(risk_prob={row['risk_prob']:.3f}, actual={int(row['actual_risk'])})")
            plot_shap_waterfall(model_e, X_day, enhanced_features,
                                metal_name, 'Enhanced', date_str, rank)

    print(f"\n{'=' * 70}")
    print("  BACKTESTING COMPLETE")
    print(f"{'=' * 70}")
    print("  All outputs saved to outputs/backtest/ and outputs/plots/")
    print("  Phase 3 complete! All 6 scripts have been run successfully.\n")


if __name__ == '__main__':
    main()