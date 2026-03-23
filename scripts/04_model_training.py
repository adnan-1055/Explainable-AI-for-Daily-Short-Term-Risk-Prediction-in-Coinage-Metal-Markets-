"""
04_model_training.py
====================
Phase 3 - Model Development
Explainable AI for Daily Short-Term Risk Prediction in Coinage Metal Markets

Mohammed Adnan Osman | Student ID: 33114153
Supervisor: Dr Nasim Dadashi | University of West London
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
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    brier_score_loss, log_loss, classification_report
)
from sklearn.utils.class_weight import compute_class_weight

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

RISK_THRESHOLD = -0.02
TRAIN_RATIO    = 0.60
VAL_RATIO      = 0.20
METALS         = {1: 'gold', 2: 'silver', 3: 'copper'}

MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
PLOTS_DIR  = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'plots')

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR,  exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION
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

def load_feature_matrix(engine, metal_id: int) -> pd.DataFrame:
    """Pull and join all features from PostgreSQL. Column names match actual DB schema."""
    metal_name = METALS[metal_id]
    print(f"\n  Loading data for {metal_name.upper()} (metal_id={metal_id})...")

    sql_tech = text("""
        SELECT
            tf.date,
            tf.daily_return,
            tf.log_return,
            tf.sma_5, tf.sma_10, tf.sma_20, tf.sma_50,
            tf.ema_12, tf.ema_26,
            tf.rsi_14,
            tf.macd, tf.macd_signal, tf.macd_histogram,
            tf.bollinger_upper, tf.bollinger_middle, tf.bollinger_lower, tf.bollinger_width,
            tf.high_low_ratio,
            tf.high_low_range,
            tf.volume_change,
            tf.volume_sma_20
        FROM technical_features tf
        WHERE tf.metal_id = :metal_id
        ORDER BY tf.date
    """)

    sql_macro = text("""
        SELECT
            date,
            usd_index,
            vix,
            treasury_yield_10y,
            sp500_close,
            sp500_return
        FROM macroeconomic_data
        ORDER BY date
    """)

    sql_sent = text("""
        SELECT
            date,
            avg_sentiment,
            avg_positive,
            avg_negative,
            avg_neutral,
            headline_count,
            positive_ratio,
            negative_ratio,
            sentiment_std
        FROM daily_sentiment
        WHERE metal_id = :metal_id
        ORDER BY date
    """)

    with engine.connect() as conn:
        df_tech  = pd.read_sql(sql_tech,  conn, params={'metal_id': metal_id}, parse_dates=['date'])
        df_macro = pd.read_sql(sql_macro, conn, parse_dates=['date'])
        df_sent  = pd.read_sql(sql_sent,  conn, params={'metal_id': metal_id}, parse_dates=['date'])

    print(f"    Technical rows  : {len(df_tech)}")
    print(f"    Macro rows      : {len(df_macro)}")
    print(f"    Sentiment rows  : {len(df_sent)}")

    df = df_tech.merge(df_macro, on='date', how='left')
    df = df.merge(df_sent, on='date', how='left', suffixes=('', '_sent'))
    df = df.sort_values('date').reset_index(drop=True)

    # Fill sentiment NaN with 0 — missing sentiment = neutral signal
    sentiment_cols = ['avg_sentiment', 'avg_positive', 'avg_negative', 'avg_neutral',
                      'headline_count', 'positive_ratio', 'negative_ratio', 'sentiment_std']
    for col in sentiment_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    print(f"    Merged rows     : {len(df)}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────

def build_target_and_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create binary target and lag features. Only drop rows with NaN in core columns."""
    df = df.copy()

    # Target: 1 = next-day return < -2% (downside risk event)
    df['target'] = (df['daily_return'].shift(-1) < RISK_THRESHOLD).astype(int)

    # Lag features for key technical columns
    lag_cols = ['daily_return', 'log_return', 'volume_change', 'rsi_14',
                'macd_histogram', 'bollinger_width']
    for col in lag_cols:
        if col in df.columns:
            df[f'{col}_lag1'] = df[col].shift(1)
            df[f'{col}_lag2'] = df[col].shift(2)
            df[f'{col}_lag3'] = df[col].shift(3)

    # Only drop rows where core technical columns are NaN (not sentiment)
    core_cols = ['daily_return', 'log_return', 'rsi_14', 'macd', 'target',
                 'daily_return_lag1', 'daily_return_lag2', 'daily_return_lag3']
    existing_core = [c for c in core_cols if c in df.columns]
    df = df.dropna(subset=existing_core).reset_index(drop=True)

    # Fill any remaining NaN in macro with forward fill then 0
    df = df.ffill().fillna(0)

    return df


def get_feature_sets(df: pd.DataFrame):
    """Returns baseline (no sentiment) and enhanced (with sentiment) feature sets."""
    exclude = {'date', 'target', 'daily_return', 'log_return'}
    sentiment_cols = {
        'avg_sentiment', 'avg_positive', 'avg_negative', 'avg_neutral',
        'headline_count', 'positive_ratio', 'negative_ratio', 'sentiment_std'
    }
    all_cols = set(df.columns) - exclude
    baseline_features = sorted(list(all_cols - sentiment_cols))
    enhanced_features = sorted(list(all_cols))
    return baseline_features, enhanced_features


# ─────────────────────────────────────────────────────────────────────────────
# TRAIN / VAL / TEST SPLIT
# ─────────────────────────────────────────────────────────────────────────────

def temporal_split(df: pd.DataFrame):
    """Chronological 60/20/20 split — no shuffling."""
    n = len(df)
    train_end = int(n * TRAIN_RATIO)
    val_end   = int(n * (TRAIN_RATIO + VAL_RATIO))

    train = df.iloc[:train_end].copy()
    val   = df.iloc[train_end:val_end].copy()
    test  = df.iloc[val_end:].copy()

    print(f"\n    Split sizes → Train: {len(train)} | Val: {len(val)} | Test: {len(test)}")
    print(f"    Train period: {train['date'].min().date()} → {train['date'].max().date()}")
    print(f"    Val period  : {val['date'].min().date()}   → {val['date'].max().date()}")
    print(f"    Test period : {test['date'].min().date()}  → {test['date'].max().date()}")

    return train, val, test


# ─────────────────────────────────────────────────────────────────────────────
# MODEL TRAINING
# ─────────────────────────────────────────────────────────────────────────────

def train_random_forest(X_train, y_train, model_label: str) -> RandomForestClassifier:
    """GridSearchCV with TimeSeriesSplit and balanced class weights."""
    print(f"\n    Training {model_label}...")
    print(f"    Features: {X_train.shape[1]} | Samples: {X_train.shape[0]}")
    print(f"    Label distribution: {dict(y_train.value_counts().sort_index())}")

    classes = np.array([0, 1])
    weights = compute_class_weight('balanced', classes=classes, y=y_train)
    class_weight_dict = {0: weights[0], 1: weights[1]}
    print(f"    Class weights: {class_weight_dict}")

    param_grid = {
        'n_estimators':      [100, 200, 300],
        'max_depth':         [5, 10, 15, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf':  [1, 2, 4],
        'max_features':      ['sqrt', 'log2']
    }

    base_rf = RandomForestClassifier(
        class_weight=class_weight_dict,
        random_state=42,
        n_jobs=-1
    )

    tscv = TimeSeriesSplit(n_splits=5)

    grid_search = GridSearchCV(
        estimator=base_rf,
        param_grid=param_grid,
        cv=tscv,
        scoring='roc_auc',
        n_jobs=-1,
        verbose=1,
        refit=True
    )

    grid_search.fit(X_train, y_train)

    print(f"\n    Best CV ROC-AUC : {grid_search.best_score_:.4f}")
    print(f"    Best params     : {grid_search.best_params_}")

    return grid_search.best_estimator_


def evaluate_model(model, X, y, split_name: str, metal_name: str, model_label: str):
    """Evaluate and print metrics for a given split."""
    y_pred      = model.predict(X)
    y_pred_prob = model.predict_proba(X)[:, 1]

    roc_auc   = roc_auc_score(y, y_pred_prob)
    f1        = f1_score(y, y_pred, zero_division=0)
    precision = precision_score(y, y_pred, zero_division=0)
    recall    = recall_score(y, y_pred, zero_division=0)
    brier     = brier_score_loss(y, y_pred_prob)
    logloss   = log_loss(y, y_pred_prob)

    print(f"\n    [{metal_name.upper()} | {model_label} | {split_name}]")
    print(f"      ROC-AUC   : {roc_auc:.4f}")
    print(f"      F1-Score  : {f1:.4f}")
    print(f"      Precision : {precision:.4f}")
    print(f"      Recall    : {recall:.4f}")
    print(f"      Brier     : {brier:.4f}")
    print(f"      Log Loss  : {logloss:.4f}")
    print(classification_report(y, y_pred, zero_division=0))

    return {
        'metal': metal_name, 'model': model_label, 'split': split_name,
        'roc_auc': roc_auc, 'f1': f1, 'precision': precision,
        'recall': recall, 'brier': brier, 'log_loss': logloss
    }


# ─────────────────────────────────────────────────────────────────────────────
# SHAP EXPLANATIONS
# ─────────────────────────────────────────────────────────────────────────────

def generate_shap_explanations(model, X_train, X_test, feature_names,
                                metal_name: str, model_label: str):
    """Generate and save SHAP bar, beeswarm plots and raw SHAP values."""
    print(f"\n    Generating SHAP values for {model_label}...")

    explainer   = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)

    if isinstance(shap_values, list):
        sv = shap_values[1]
    else:
        sv = shap_values

    label = f"{metal_name}_{model_label.replace(' ', '_').lower()}"

    # Bar chart
    plt.figure(figsize=(10, 8))
    shap.summary_plot(sv, X_test, feature_names=feature_names,
                      plot_type='bar', show=False)
    plt.title(f'SHAP Feature Importance\n{metal_name.upper()} | {model_label}',
              fontsize=13, fontweight='bold')
    plt.tight_layout()
    bar_path = os.path.join(PLOTS_DIR, f'shap_bar_{label}.png')
    plt.savefig(bar_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"      Saved: {bar_path}")

    # Beeswarm plot
    plt.figure(figsize=(10, 8))
    shap.summary_plot(sv, X_test, feature_names=feature_names, show=False)
    plt.title(f'SHAP Summary Plot\n{metal_name.upper()} | {model_label}',
              fontsize=13, fontweight='bold')
    plt.tight_layout()
    bee_path = os.path.join(PLOTS_DIR, f'shap_beeswarm_{label}.png')
    plt.savefig(bee_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"      Saved: {bee_path}")

    # Save raw SHAP values
    shap_path = os.path.join(MODELS_DIR, f'shap_values_{label}.npy')
    np.save(shap_path, sv)
    print(f"      SHAP values saved: {shap_path}")

    return sv, explainer


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  PHASE 3: MODEL TRAINING")
    print("  Explainable AI for Metal Market Risk Prediction")
    print("  Mohammed Adnan Osman | 33114153")
    print("=" * 70)

    engine = get_engine()
    all_results = []

    for metal_id, metal_name in METALS.items():
        print(f"\n{'─' * 70}")
        print(f"  PROCESSING: {metal_name.upper()}")
        print(f"{'─' * 70}")

        df = load_feature_matrix(engine, metal_id)
        df = build_target_and_lag_features(df)
        print(f"\n    Total samples after feature eng. : {len(df)}")
        print(f"    Downside risk events (label=1)   : {df['target'].sum()} "
              f"({df['target'].mean()*100:.1f}%)")

        baseline_features, enhanced_features = get_feature_sets(df)
        train, val, test = temporal_split(df)

        # ── BASELINE MODEL ────────────────────────────────────────────────────
        print(f"\n  {'─'*30} BASELINE MODEL {'─'*23}")
        X_train_b = train[baseline_features]
        y_train_b = train['target']
        X_val_b   = val[baseline_features]
        y_val_b   = val['target']
        X_test_b  = test[baseline_features]
        y_test_b  = test['target']

        model_baseline = train_random_forest(X_train_b, y_train_b, f'{metal_name}_Baseline')

        r1 = evaluate_model(model_baseline, X_train_b, y_train_b, 'Train', metal_name, 'Baseline')
        r2 = evaluate_model(model_baseline, X_val_b,   y_val_b,   'Val',   metal_name, 'Baseline')
        r3 = evaluate_model(model_baseline, X_test_b,  y_test_b,  'Test',  metal_name, 'Baseline')
        all_results.extend([r1, r2, r3])

        generate_shap_explanations(model_baseline, X_train_b, X_test_b,
                                   baseline_features, metal_name, 'Baseline')

        # ── ENHANCED MODEL ────────────────────────────────────────────────────
        print(f"\n  {'─'*30} ENHANCED MODEL {'─'*23}")
        X_train_e = train[enhanced_features]
        y_train_e = train['target']
        X_val_e   = val[enhanced_features]
        y_val_e   = val['target']
        X_test_e  = test[enhanced_features]
        y_test_e  = test['target']

        model_enhanced = train_random_forest(X_train_e, y_train_e, f'{metal_name}_Enhanced')

        r4 = evaluate_model(model_enhanced, X_train_e, y_train_e, 'Train', metal_name, 'Enhanced')
        r5 = evaluate_model(model_enhanced, X_val_e,   y_val_e,   'Val',   metal_name, 'Enhanced')
        r6 = evaluate_model(model_enhanced, X_test_e,  y_test_e,  'Test',  metal_name, 'Enhanced')
        all_results.extend([r4, r5, r6])

        generate_shap_explanations(model_enhanced, X_train_e, X_test_e,
                                   enhanced_features, metal_name, 'Enhanced')

        # Save models
        enhanced_path = os.path.join(MODELS_DIR, f'rf_enhanced_{metal_name}.joblib')
        joblib.dump(model_enhanced, enhanced_path)
        print(f"\n    Enhanced model saved: {enhanced_path}")

        baseline_path = os.path.join(MODELS_DIR, f'rf_baseline_{metal_name}.joblib')
        joblib.dump(model_baseline, baseline_path)
        print(f"    Baseline model saved: {baseline_path}")

        feat_path = os.path.join(MODELS_DIR, f'features_{metal_name}.joblib')
        joblib.dump({
            'baseline': baseline_features,
            'enhanced': enhanced_features,
            'risk_threshold': RISK_THRESHOLD
        }, feat_path)
        print(f"    Feature lists saved : {feat_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print("  TRAINING COMPLETE — RESULTS SUMMARY")
    print(f"{'=' * 70}")
    results_df = pd.DataFrame(all_results)
    test_results = results_df[results_df['split'] == 'Test'].copy()
    print(test_results[[
        'metal', 'model', 'roc_auc', 'f1', 'precision', 'recall', 'brier', 'log_loss'
    ]].to_string(index=False))

    summary_path = os.path.join(MODELS_DIR, 'training_results_summary.csv')
    results_df.to_csv(summary_path, index=False)
    print(f"\n  Full results saved: {summary_path}")
    print("\n  Phase 3 training complete. Run 05_model_evaluation.py next.\n")


if __name__ == '__main__':
    main()