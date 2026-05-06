import os
import warnings
import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from sklearn.metrics import (
    roc_auc_score, roc_curve,
    precision_recall_curve, average_precision_score,
    brier_score_loss, log_loss,
    confusion_matrix, f1_score, precision_score, recall_score
)
from sklearn.calibration import calibration_curve

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

MODELS_DIR  = os.path.join(os.path.dirname(__file__), '..', 'models')
PLOTS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'plots')
METRICS_DIR = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'metrics')

os.makedirs(PLOTS_DIR,   exist_ok=True)
os.makedirs(METRICS_DIR, exist_ok=True)

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
# DATA LOADING  (mirrors 04_model_training.py exactly)
# ─────────────────────────────────────────────────────────────────────────────

def load_and_prepare_data(engine, metal_id: int) -> pd.DataFrame:
    """Reload the same feature matrix used during training."""

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

    # Build target and lag features
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


def get_test_split(df: pd.DataFrame):
    """Return only the test portion (last 20%)."""
    n = len(df)
    val_end = int(n * (TRAIN_RATIO + VAL_RATIO))
    return df.iloc[val_end:].copy()


# ─────────────────────────────────────────────────────────────────────────────
# PLOTTING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

COLOURS = {'Baseline': '#2196F3', 'Enhanced': '#FF5722'}


def plot_roc_curve(ax, y_true, y_prob_base, y_prob_enh, metal_name):
    fpr_b, tpr_b, _ = roc_curve(y_true, y_prob_base)
    fpr_e, tpr_e, _ = roc_curve(y_true, y_prob_enh)
    auc_b = roc_auc_score(y_true, y_prob_base)
    auc_e = roc_auc_score(y_true, y_prob_enh)

    ax.plot(fpr_b, tpr_b, color=COLOURS['Baseline'],
            label=f'Baseline (AUC={auc_b:.3f})', lw=2)
    ax.plot(fpr_e, tpr_e, color=COLOURS['Enhanced'],
            label=f'Enhanced (AUC={auc_e:.3f})', lw=2)
    ax.plot([0, 1], [0, 1], 'k--', lw=1, alpha=0.5)
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.set_title(f'ROC Curve — {metal_name.upper()}')
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)


def plot_pr_curve(ax, y_true, y_prob_base, y_prob_enh, metal_name):
    prec_b, rec_b, _ = precision_recall_curve(y_true, y_prob_base)
    prec_e, rec_e, _ = precision_recall_curve(y_true, y_prob_enh)
    ap_b = average_precision_score(y_true, y_prob_base)
    ap_e = average_precision_score(y_true, y_prob_enh)

    ax.plot(rec_b, prec_b, color=COLOURS['Baseline'],
            label=f'Baseline (AP={ap_b:.3f})', lw=2)
    ax.plot(rec_e, prec_e, color=COLOURS['Enhanced'],
            label=f'Enhanced (AP={ap_e:.3f})', lw=2)
    baseline_rate = y_true.mean()
    ax.axhline(baseline_rate, color='k', linestyle='--', lw=1, alpha=0.5,
               label=f'Baseline rate ({baseline_rate:.3f})')
    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')
    ax.set_title(f'Precision-Recall — {metal_name.upper()}')
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)


def plot_calibration(ax, y_true, y_prob_base, y_prob_enh, metal_name):
    frac_b, mean_b = calibration_curve(y_true, y_prob_base, n_bins=10, strategy='quantile')
    frac_e, mean_e = calibration_curve(y_true, y_prob_enh,  n_bins=10, strategy='quantile')

    ax.plot(mean_b, frac_b, 's-', color=COLOURS['Baseline'],
            label='Baseline', lw=2, markersize=5)
    ax.plot(mean_e, frac_e, 'o-', color=COLOURS['Enhanced'],
            label='Enhanced', lw=2, markersize=5)
    ax.plot([0, 1], [0, 1], 'k--', lw=1, alpha=0.5, label='Perfect')
    ax.set_xlabel('Mean Predicted Probability')
    ax.set_ylabel('Fraction of Positives')
    ax.set_title(f'Calibration Plot — {metal_name.upper()}')
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)


def plot_confusion(ax, y_true, y_pred, metal_name, model_label):
    cm = confusion_matrix(y_true, y_pred)
    im = ax.imshow(cm, interpolation='nearest', cmap='Blues')
    ax.set_title(f'Confusion Matrix\n{metal_name.upper()} | {model_label}', fontsize=9)
    ax.set_xlabel('Predicted')
    ax.set_ylabel('Actual')
    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(['No Risk', 'Risk'])
    ax.set_yticklabels(['No Risk', 'Risk'])
    thresh = cm.max() / 2.0
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], 'd'),
                    ha='center', va='center',
                    color='white' if cm[i, j] > thresh else 'black',
                    fontsize=11)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  PHASE 3: MODEL EVALUATION")
    print("  Explainable AI for Metal Market Risk Prediction")
    print("  Mohammed Adnan Osman | 33114153")
    print("=" * 70)

    engine   = get_engine()
    all_metrics = []

    for metal_id, metal_name in METALS.items():
        print(f"\n{'─' * 70}")
        print(f"  EVALUATING: {metal_name.upper()}")
        print(f"{'─' * 70}")

        # Load data & models
        df   = load_and_prepare_data(engine, metal_id)
        test = get_test_split(df)

        feat_info = joblib.load(os.path.join(MODELS_DIR, f'features_{metal_name}.joblib'))
        baseline_features = feat_info['baseline']
        enhanced_features = feat_info['enhanced']

        model_b = joblib.load(os.path.join(MODELS_DIR, f'rf_baseline_{metal_name}.joblib'))
        model_e = joblib.load(os.path.join(MODELS_DIR, f'rf_enhanced_{metal_name}.joblib'))

        X_test_b = test[baseline_features]
        X_test_e = test[enhanced_features]
        y_test   = test['target']

        y_prob_b = model_b.predict_proba(X_test_b)[:, 1]
        y_prob_e = model_e.predict_proba(X_test_e)[:, 1]
        y_pred_b = model_b.predict(X_test_b)
        y_pred_e = model_e.predict(X_test_e)

        print(f"    Test samples : {len(y_test)}")
        print(f"    Risk events  : {y_test.sum()} ({y_test.mean()*100:.1f}%)")
        print(f"    Test period  : {test['date'].min().date()} → {test['date'].max().date()}")

        # ── Per-metal evaluation plots ────────────────────────────────────────
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle(f'Model Evaluation — {metal_name.upper()}',
                     fontsize=14, fontweight='bold')

        plot_roc_curve(axes[0, 0], y_test, y_prob_b, y_prob_e, metal_name)
        plot_pr_curve(axes[0, 1],  y_test, y_prob_b, y_prob_e, metal_name)
        plot_calibration(axes[0, 2], y_test, y_prob_b, y_prob_e, metal_name)
        plot_confusion(axes[1, 0], y_test, y_pred_b, metal_name, 'Baseline')
        plot_confusion(axes[1, 1], y_test, y_pred_e, metal_name, 'Enhanced')

        # Risk probability distribution
        axes[1, 2].hist(y_prob_b, bins=30, alpha=0.6, color=COLOURS['Baseline'],
                        label='Baseline', density=True)
        axes[1, 2].hist(y_prob_e, bins=30, alpha=0.6, color=COLOURS['Enhanced'],
                        label='Enhanced', density=True)
        axes[1, 2].set_xlabel('Predicted Risk Probability')
        axes[1, 2].set_ylabel('Density')
        axes[1, 2].set_title(f'Risk Score Distribution — {metal_name.upper()}')
        axes[1, 2].legend(fontsize=8)
        axes[1, 2].grid(alpha=0.3)

        plt.tight_layout()
        eval_path = os.path.join(PLOTS_DIR, f'evaluation_{metal_name}.png')
        plt.savefig(eval_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Evaluation plot saved: {eval_path}")

        # ── Collect metrics ───────────────────────────────────────────────────
        for model, y_prob, y_pred, label in [
            (model_b, y_prob_b, y_pred_b, 'Baseline'),
            (model_e, y_prob_e, y_pred_e, 'Enhanced')
        ]:
            all_metrics.append({
                'metal':      metal_name,
                'model':      label,
                'roc_auc':    roc_auc_score(y_test, y_prob),
                'avg_prec':   average_precision_score(y_test, y_prob),
                'brier':      brier_score_loss(y_test, y_prob),
                'log_loss':   log_loss(y_test, y_prob),
                'f1':         f1_score(y_test, y_pred, zero_division=0),
                'precision':  precision_score(y_test, y_pred, zero_division=0),
                'recall':     recall_score(y_test, y_pred, zero_division=0),
                'n_test':     len(y_test),
                'n_risk':     int(y_test.sum()),
            })

    # ── Summary comparison chart ──────────────────────────────────────────────
    metrics_df = pd.DataFrame(all_metrics)
    metrics_csv = os.path.join(METRICS_DIR, 'evaluation_metrics.csv')
    metrics_df.to_csv(metrics_csv, index=False)
    print(f"\n  Metrics saved: {metrics_csv}")

    # Multi-metal ROC-AUC comparison bar chart
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle('Baseline vs Enhanced Model Comparison — All Metals',
                 fontsize=13, fontweight='bold')

    metric_pairs = [
        ('roc_auc',  'ROC-AUC'),
        ('brier',    'Brier Score (lower = better)'),
        ('avg_prec', 'Average Precision'),
    ]

    for ax, (metric, title) in zip(axes, metric_pairs):
        metals_list = list(METALS.values())
        x = np.arange(len(metals_list))
        width = 0.35

        base_vals = [metrics_df[(metrics_df['metal'] == m) &
                                (metrics_df['model'] == 'Baseline')][metric].values[0]
                     for m in metals_list]
        enh_vals  = [metrics_df[(metrics_df['metal'] == m) &
                                (metrics_df['model'] == 'Enhanced')][metric].values[0]
                     for m in metals_list]

        bars_b = ax.bar(x - width/2, base_vals, width, label='Baseline',
                        color=COLOURS['Baseline'], alpha=0.85)
        bars_e = ax.bar(x + width/2, enh_vals,  width, label='Enhanced',
                        color=COLOURS['Enhanced'], alpha=0.85)

        ax.set_xticks(x)
        ax.set_xticklabels([m.capitalize() for m in metals_list])
        ax.set_title(title)
        ax.legend(fontsize=9)
        ax.grid(axis='y', alpha=0.3)

        # Value labels on bars
        for bar in bars_b:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                    f'{bar.get_height():.3f}', ha='center', va='bottom', fontsize=8)
        for bar in bars_e:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                    f'{bar.get_height():.3f}', ha='center', va='bottom', fontsize=8)

    plt.tight_layout()
    comparison_path = os.path.join(PLOTS_DIR, 'comparison_all_metals.png')
    plt.savefig(comparison_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Comparison chart saved: {comparison_path}")

    # Print summary table
    print(f"\n{'=' * 70}")
    print("  EVALUATION COMPLETE — SUMMARY")
    print(f"{'=' * 70}")
    print(metrics_df[['metal', 'model', 'roc_auc', 'avg_prec', 'brier',
                       'log_loss']].to_string(index=False))
    print("\n  Phase 3 evaluation complete. Run 06_backtesting.py next.\n")


if __name__ == '__main__':
    main()