"""
07_regenerate_shap.py
=====================
Standalone script to regenerate SHAP plots correctly.
Models are already trained — this just reloads and re-plots.

Mohammed Adnan Osman | Student ID: 33114153
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

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

warnings.filterwarnings('ignore')

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST',     'localhost'),
    'port':     os.getenv('DB_PORT',     '5432'),
    'dbname':   os.getenv('DB_NAME',     'metal_risk_prediction'),
    'user':     os.getenv('DB_USER',     'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

RISK_THRESHOLD = -0.02
TRAIN_RATIO    = 0.60
VAL_RATIO      = 0.20
METALS         = {1: 'gold', 2: 'silver', 3: 'copper'}

MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
PLOTS_DIR  = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)


def get_engine():
    url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
           f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    return create_engine(url)


def load_test_data(engine, metal_id):
    sql_tech = text("""
        SELECT tf.date, tf.daily_return, tf.log_return,
               tf.sma_5, tf.sma_10, tf.sma_20, tf.sma_50,
               tf.ema_12, tf.ema_26, tf.rsi_14,
               tf.macd, tf.macd_signal, tf.macd_histogram,
               tf.bollinger_upper, tf.bollinger_middle, tf.bollinger_lower, tf.bollinger_width,
               tf.high_low_ratio, tf.high_low_range, tf.volume_change, tf.volume_sma_20
        FROM technical_features tf WHERE tf.metal_id = :metal_id ORDER BY tf.date
    """)
    sql_macro = text("""
        SELECT date, usd_index, vix, treasury_yield_10y, sp500_close, sp500_return
        FROM macroeconomic_data ORDER BY date
    """)
    sql_sent = text("""
        SELECT date, avg_sentiment, avg_positive, avg_negative, avg_neutral,
               headline_count, positive_ratio, negative_ratio, sentiment_std
        FROM daily_sentiment WHERE metal_id = :metal_id ORDER BY date
    """)

    with engine.connect() as conn:
        df_tech  = pd.read_sql(sql_tech,  conn, params={'metal_id': metal_id}, parse_dates=['date'])
        df_macro = pd.read_sql(sql_macro, conn, parse_dates=['date'])
        df_sent  = pd.read_sql(sql_sent,  conn, params={'metal_id': metal_id}, parse_dates=['date'])

    df = df_tech.merge(df_macro, on='date', how='left')
    df = df.merge(df_sent, on='date', how='left', suffixes=('', '_sent'))
    df = df.sort_values('date').reset_index(drop=True)

    for col in ['avg_sentiment', 'avg_positive', 'avg_negative', 'avg_neutral',
                'headline_count', 'positive_ratio', 'negative_ratio', 'sentiment_std']:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df['target'] = (df['daily_return'].shift(-1) < RISK_THRESHOLD).astype(int)
    for col in ['daily_return', 'log_return', 'volume_change', 'rsi_14',
                'macd_histogram', 'bollinger_width']:
        if col in df.columns:
            df[f'{col}_lag1'] = df[col].shift(1)
            df[f'{col}_lag2'] = df[col].shift(2)
            df[f'{col}_lag3'] = df[col].shift(3)

    core = ['daily_return', 'log_return', 'rsi_14', 'macd', 'target',
            'daily_return_lag1', 'daily_return_lag2', 'daily_return_lag3']
    df = df.dropna(subset=[c for c in core if c in df.columns]).reset_index(drop=True)
    df = df.ffill().fillna(0)

    n = len(df)
    val_end = int(n * (TRAIN_RATIO + VAL_RATIO))
    return df.iloc[val_end:].copy()


def get_shap_values(model, X_test):
    """Extract class-1 SHAP values handling all possible output shapes."""
    explainer = shap.TreeExplainer(model, feature_perturbation='tree_path_dependent')
    raw = explainer.shap_values(X_test)

    arr = np.array(raw)
    print(f"      Raw SHAP shape: {arr.shape}")

    # Shape (2, n_samples, n_features) — list of two class arrays
    if arr.ndim == 3 and arr.shape[0] == 2:
        return arr[1]

    # Shape (n_samples, n_features, 2) — per-sample per-feature per-class
    if arr.ndim == 3 and arr.shape[2] == 2:
        return arr[:, :, 1]

    # Shape (n_samples, n_features) — already single output
    if arr.ndim == 2:
        return arr

    # Fallback
    return arr


def make_shap_plots(model, X_test, feature_names, metal_name, model_label):
    """Generate correct SHAP bar and beeswarm plots."""
    print(f"    Generating SHAP for {metal_name} {model_label}...")

    feature_names_list = list(feature_names)
    sv = get_shap_values(model, X_test)
    print(f"      Final SHAP shape: {sv.shape}")

    label = f"{metal_name}_{model_label.lower()}"

    # ── Bar chart (mean absolute SHAP) ───────────────────────────────────────
    mean_abs  = np.abs(sv).mean(axis=0)
    n_show    = min(20, len(feature_names_list))
    sorted_idx = np.argsort(mean_abs)[-n_show:]  # ascending for barh

    top_feats = [feature_names_list[int(i)] for i in sorted_idx]
    top_vals  = [float(mean_abs[int(i)]) for i in sorted_idx]

    fig, ax = plt.subplots(figsize=(10, 7))
    colours = ['#FF6B6B' if 'avg_' in f or 'sentiment' in f or 'positive' in f
               or 'negative' in f or 'neutral' in f or 'headline' in f
               else '#4A90D9' for f in top_feats]
    ax.barh(range(len(top_feats)), top_vals, color=colours, alpha=0.85)
    ax.set_yticks(range(len(top_feats)))
    ax.set_yticklabels(top_feats, fontsize=9)
    ax.set_xlabel('Mean |SHAP Value|', fontsize=11)
    ax.set_title(f'SHAP Feature Importance\n{metal_name.upper()} | {model_label}',
                 fontsize=13, fontweight='bold')

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#4A90D9', alpha=0.85, label='Technical / Macro'),
        Patch(facecolor='#FF6B6B', alpha=0.85, label='Sentiment')
    ]
    ax.legend(handles=legend_elements, fontsize=9, loc='lower right')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    bar_path = os.path.join(PLOTS_DIR, f'shap_bar_{label}.png')
    plt.savefig(bar_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"      Bar chart saved: {bar_path}")

    # ── Beeswarm plot — top 15 features ──────────────────────────────────────
    n_bee     = min(15, len(feature_names_list))
    top15_idx = np.argsort(mean_abs)[::-1][:n_bee]
    sv_top    = sv[:, [int(i) for i in top15_idx]]
    feat_top  = [feature_names_list[int(i)] for i in top15_idx]

    if hasattr(X_test, 'iloc'):
        X_top = X_test.iloc[:, [int(i) for i in top15_idx]]
    else:
        X_top = X_test[:, [int(i) for i in top15_idx]]

    fig, ax = plt.subplots(figsize=(10, 8))
    shap.summary_plot(
        sv_top,
        X_top,
        feature_names=feat_top,
        show=False,
        plot_type='dot',
        max_display=n_bee
    )
    plt.title(f'SHAP Summary Plot\n{metal_name.upper()} | {model_label}',
              fontsize=13, fontweight='bold')
    plt.tight_layout()

    bee_path = os.path.join(PLOTS_DIR, f'shap_beeswarm_{label}.png')
    plt.savefig(bee_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"      Beeswarm saved:   {bee_path}")


def main():
    print("=" * 60)
    print("  SHAP PLOT REGENERATION")
    print("=" * 60)

    engine = get_engine()

    for metal_id, metal_name in METALS.items():
        print(f"\n  {metal_name.upper()}")
        print(f"  {'─' * 40}")

        test_df   = load_test_data(engine, metal_id)
        feat_info = joblib.load(os.path.join(MODELS_DIR, f'features_{metal_name}.joblib'))
        baseline_features = feat_info['baseline']
        enhanced_features = feat_info['enhanced']

        model_b = joblib.load(os.path.join(MODELS_DIR, f'rf_baseline_{metal_name}.joblib'))
        model_e = joblib.load(os.path.join(MODELS_DIR, f'rf_enhanced_{metal_name}.joblib'))

        X_test_b = test_df[baseline_features]
        X_test_e = test_df[enhanced_features]

        make_shap_plots(model_b, X_test_b, baseline_features, metal_name, 'Baseline')
        make_shap_plots(model_e, X_test_e, enhanced_features, metal_name, 'Enhanced')

    print("\n  All SHAP plots regenerated successfully!")
    print(f"  Check: outputs/plots/shap_bar_*.png and shap_beeswarm_*.png\n")


if __name__ == '__main__':
    main()