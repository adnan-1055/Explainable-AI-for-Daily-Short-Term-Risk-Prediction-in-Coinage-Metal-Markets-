import os
import warnings
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import shap

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Metal Risk XAI",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_DIR = r"C:\Users\User\OneDrive\CS\Dissertation\Explainable-AI-for-Daily-Short-Term-Risk-Prediction-in-Coinage-Metal-Markets-"
BASE_DIR    = os.path.join(PROJECT_DIR, 'streamlit')
MODELS_DIR  = os.path.join(PROJECT_DIR, 'models')
PLOTS_DIR   = os.path.join(PROJECT_DIR, 'outputs', 'plots')
METRICS_CSV = os.path.join(PROJECT_DIR, 'outputs', 'metrics', 'evaluation_metrics.csv')

st.write(f"DEBUG PATH: {METRICS_CSV}")
st.write(f"EXISTS: {os.path.exists(METRICS_CSV)}")

METALS = ['gold', 'silver', 'copper']
TICKERS = {'gold': 'GC=F', 'silver': 'SI=F', 'copper': 'HG=F'}
MACRO_TICKERS = {
    'usd_index':        'DX-Y.NYB',
    'vix':              '^VIX',
    'treasury_yield_10y': '^TNX',
    'sp500_close':      '^GSPC'
}
RISK_THRESHOLD = -0.02

# ─────────────────────────────────────────────────────────────────────────────
# STYLING
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

.main { background-color: #0a0e1a; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1320 0%, #111827 100%);
    border-right: 1px solid #1e2d45;
}

/* Title block */
.hero-title {
    font-family: 'Space Mono', monospace;
    font-size: 2rem;
    font-weight: 700;
    color: #f0c040;
    letter-spacing: -0.5px;
    line-height: 1.2;
    margin-bottom: 0.2rem;
}
.hero-sub {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.95rem;
    color: #8899aa;
    letter-spacing: 0.5px;
    margin-bottom: 1.5rem;
}

/* Risk cards */
.risk-card {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    transition: border-color 0.2s;
}
.risk-card:hover { border-color: #f0c040; }
.risk-card .metal-name {
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    color: #8899aa;
    text-transform: uppercase;
    letter-spacing: 2px;
    margin-bottom: 0.5rem;
}
.risk-card .risk-score {
    font-family: 'Space Mono', monospace;
    font-size: 2.4rem;
    font-weight: 700;
    line-height: 1;
    margin-bottom: 0.4rem;
}
.risk-card .risk-label {
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 1px;
    text-transform: uppercase;
    padding: 0.2rem 0.6rem;
    border-radius: 20px;
    display: inline-block;
}
.risk-high  { color: #ef4444; }
.risk-med   { color: #f59e0b; }
.risk-low   { color: #22c55e; }
.badge-high { background: rgba(239,68,68,0.15); color: #ef4444; }
.badge-med  { background: rgba(245,158,11,0.15); color: #f59e0b; }
.badge-low  { background: rgba(34,197,94,0.15);  color: #22c55e; }

/* Metric row */
.metric-box {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    text-align: center;
}
.metric-box .m-label {
    font-size: 0.72rem;
    color: #8899aa;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 0.3rem;
}
.metric-box .m-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    color: #f0c040;
}
.metric-box .m-delta {
    font-size: 0.72rem;
    margin-top: 0.2rem;
}
.delta-pos { color: #22c55e; }
.delta-neg { color: #ef4444; }

/* Section headers */
.section-header {
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    color: #f0c040;
    text-transform: uppercase;
    letter-spacing: 3px;
    border-bottom: 1px solid #1e2d45;
    padding-bottom: 0.5rem;
    margin-bottom: 1.2rem;
    margin-top: 1.5rem;
}

/* Info banner */
.info-banner {
    background: rgba(240,192,64,0.08);
    border: 1px solid rgba(240,192,64,0.25);
    border-radius: 8px;
    padding: 0.8rem 1.2rem;
    font-size: 0.85rem;
    color: #d4a820;
    margin-bottom: 1rem;
}

/* Price row */
.price-row {
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
    flex-wrap: wrap;
}
.price-chip {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 8px;
    padding: 0.5rem 1rem;
    font-family: 'Space Mono', monospace;
    font-size: 0.85rem;
    color: #e2e8f0;
}
.price-chip span { color: #8899aa; font-size: 0.72rem; margin-right: 0.4rem; }

/* Override streamlit button */
.stButton > button {
    background: linear-gradient(135deg, #f0c040, #d4a820) !important;
    color: #0a0e1a !important;
    font-family: 'Space Mono', monospace !important;
    font-weight: 700 !important;
    font-size: 0.85rem !important;
    letter-spacing: 1px !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.6rem 1.5rem !important;
    width: 100% !important;
}
.stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 20px rgba(240,192,64,0.3) !important;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    background: #111827;
    border-radius: 10px;
    gap: 0.2rem;
    padding: 0.3rem;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    letter-spacing: 1px;
    color: #8899aa;
    border-radius: 8px;
}
.stTabs [aria-selected="true"] {
    background: #f0c040 !important;
    color: #0a0e1a !important;
}

/* Dark background for plots */
.plot-container {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 12px;
    padding: 1rem;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# LOAD MODELS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def load_models():
    models = {}
    features = {}
    for metal in METALS:
        try:
            models[metal] = {
                'baseline': joblib.load(os.path.join(MODELS_DIR, f'rf_baseline_{metal}.joblib')),
                'enhanced': joblib.load(os.path.join(MODELS_DIR, f'rf_enhanced_{metal}.joblib'))
            }
            feat_data = joblib.load(os.path.join(MODELS_DIR, f'features_{metal}.joblib'))
            features[metal] = feat_data
        except Exception as e:
            st.error(f"Error loading {metal} model: {e}")
    return models, features

models, features = load_models()

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING (mirrors Scripts 02 & 04)
# ─────────────────────────────────────────────────────────────────────────────

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.clip(0, 100)

def build_features_live(df_price, df_macro):
    df = df_price.copy()
    df = df.sort_values('date').reset_index(drop=True)

    close = df['close']
    high  = df['high']
    low   = df['low']
    vol   = df['volume']

    df['daily_return']  = close.pct_change()
    df['log_return']    = np.log(close / close.shift(1))
    df['sma_5']         = close.rolling(5).mean()
    df['sma_10']        = close.rolling(10).mean()
    df['sma_20']        = close.rolling(20).mean()
    df['sma_50']        = close.rolling(50).mean()
    df['ema_12']        = close.ewm(span=12).mean()
    df['ema_26']        = close.ewm(span=26).mean()
    df['rsi_14']        = calculate_rsi(close)
    df['macd']          = df['ema_12'] - df['ema_26']
    ema9                = df['macd'].ewm(span=9).mean()
    df['macd_signal']   = ema9
    df['macd_histogram']= df['macd'] - ema9
    df['bollinger_middle'] = close.rolling(20).mean()
    bb_std              = close.rolling(20).std()
    df['bollinger_upper'] = df['bollinger_middle'] + 2*bb_std
    df['bollinger_lower'] = df['bollinger_middle'] - 2*bb_std
    df['bollinger_width'] = (df['bollinger_upper'] - df['bollinger_lower']) / df['bollinger_middle']
    df['high_low_ratio']  = high / low.replace(0, np.nan)
    df['high_low_range']  = high - low
    df['volume_change']   = vol.pct_change()
    df['volume_sma_20']   = vol.rolling(20).mean()

    # Replace inf/-inf
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Merge macro
    df = df.merge(df_macro, on='date', how='left')
    df['sp500_return'] = df['sp500_close'].pct_change()

    # Lag features
    lag_cols = ['daily_return', 'log_return', 'volume_change', 'rsi_14',
                'macd_histogram', 'bollinger_width']
    for col in lag_cols:
        if col in df.columns:
            df[f'{col}_lag1'] = df[col].shift(1)
            df[f'{col}_lag2'] = df[col].shift(2)
            df[f'{col}_lag3'] = df[col].shift(3)

    df = df.ffill().fillna(0)
    return df

@st.cache_data(ttl=3600)
def fetch_live_data():
    """Fetch recent price and macro data from yFinance."""
    results = {}
    try:
        # Download macro first
        macro_data = {}
        for col, ticker in MACRO_TICKERS.items():
            raw = yf.download(ticker, period='3mo', interval='1d', progress=False, auto_adjust=True)
            if not raw.empty:
                raw = raw.reset_index()
                raw.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in raw.columns]
                macro_data[col] = raw[['date', 'close']].rename(columns={'close': col})

        df_macro = macro_data.get('usd_index', pd.DataFrame())
        for col in ['vix', 'treasury_yield_10y', 'sp500_close']:
            if col in macro_data:
                df_macro = df_macro.merge(macro_data[col], on='date', how='outer') if not df_macro.empty else macro_data[col]
        df_macro = df_macro.sort_values('date').ffill().fillna(0)

        # Download metals
        for metal, ticker in TICKERS.items():
            raw = yf.download(ticker, period='3mo', interval='1d', progress=False, auto_adjust=True)
            if raw.empty:
                continue
            raw = raw.reset_index()
            raw.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in raw.columns]
            raw = raw.rename(columns={'adj close': 'adj_close'})
            for col_needed in ['open', 'high', 'low', 'close', 'volume']:
                if col_needed not in raw.columns:
                    raw[col_needed] = 0
            df_feat = build_features_live(raw, df_macro)
            results[metal] = {
                'features': df_feat,
                'latest_price': float(raw['close'].iloc[-1]),
                'latest_date':  raw['date'].iloc[-1]
            }
    except Exception as e:
        st.error(f"Error fetching live data: {e}")
    return results

def get_risk_color_class(prob):
    if prob >= 0.5:   return 'risk-high', 'badge-high', '⚠ HIGH RISK'
    elif prob >= 0.3: return 'risk-med',  'badge-med',  '◈ ELEVATED'
    else:             return 'risk-low',  'badge-low',  '✓ LOW RISK'

def run_live_prediction(metal, live_data):
    """Run enhanced model prediction on latest available features."""
    if metal not in live_data or metal not in models:
        return None, None, None

    data      = live_data[metal]
    df        = data['features']
    model     = models[metal]['enhanced']
    feat_list = features[metal]['enhanced']

    # Get last complete row
    available = [f for f in feat_list if f in df.columns]
    row = df[available].iloc[-1:].copy()

    # Fill any missing features with 0
    for f in feat_list:
        if f not in row.columns:
            row[f] = 0
    row = row[feat_list]
    row = row.fillna(0)

    prob    = float(model.predict_proba(row)[0, 1])
    pred    = int(model.predict(row)[0])

    # SHAP
    try:
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(row)
        if isinstance(shap_values, list):
            sv = shap_values[1][0]
        elif shap_values.ndim == 3:
            sv = shap_values[0, :, 1]
        else:
            sv = shap_values[0]
        shap_df = pd.DataFrame({'feature': feat_list, 'shap': sv})
        shap_df['abs_shap'] = shap_df['shap'].abs()
        shap_df = shap_df.sort_values('abs_shap', ascending=False).head(10)
    except Exception:
        shap_df = None

    return prob, pred, shap_df

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 0.5rem;'>
        <div style='font-family: Space Mono, monospace; font-size: 0.65rem;
                    color: #f0c040; letter-spacing: 3px; text-transform: uppercase;
                    margin-bottom: 0.3rem;'>XAI Dashboard</div>
        <div style='font-size: 1.1rem; font-weight: 600; color: #e2e8f0;
                    line-height: 1.3;'>Coinage Metal<br>Risk Prediction</div>
        <div style='font-size: 0.75rem; color: #8899aa; margin-top: 0.4rem;'>
            Mohammed Adnan Osman · 33114153
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color: #1e2d45; margin: 1rem 0;'>", unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["🏠  Live Prediction", "📊  Model Performance", "🔍  SHAP Analysis", "📈  Backtesting"],
        label_visibility="collapsed"
    )

    st.markdown("<hr style='border-color: #1e2d45; margin: 1rem 0;'>", unsafe_allow_html=True)

    st.markdown("""
    <div style='font-size: 0.72rem; color: #8899aa; line-height: 1.7;'>
        <div style='color: #f0c040; font-family: Space Mono, monospace;
                    font-size: 0.65rem; letter-spacing: 2px; margin-bottom: 0.5rem;'>
            MODEL INFO
        </div>
        🤖 Random Forest (Enhanced)<br>
        📅 Trained: 2020–2024<br>
        🎯 Target: >2% daily decline<br>
        📐 Split: 60/20/20 temporal<br>
        🔬 XAI: SHAP TreeExplainer
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color: #1e2d45; margin: 1rem 0;'>", unsafe_allow_html=True)

    st.markdown("""
    <div style='font-size: 0.7rem; color: #4a5568; line-height: 1.5;'>
        ⚠️ For academic research only.<br>
        Not financial advice.
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: LIVE PREDICTION
# ─────────────────────────────────────────────────────────────────────────────

if "🏠" in page:
    st.markdown("""
    <div class='hero-title'>⚡ Live Downside Risk Prediction</div>
    <div class='hero-sub'>EXPLAINABLE AI · COINAGE METAL MARKETS · REAL-TIME</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class='info-banner'>
        Fetches today's price and macroeconomic data from Yahoo Finance, engineers all 49 model features,
        and runs the Enhanced Random Forest classifier to output a downside risk probability (>2% decline)
        for gold, silver, and copper.
    </div>
    """, unsafe_allow_html=True)

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        run_pred = st.button("🔮  RUN PREDICTION")

    if run_pred:
        with st.spinner("Fetching live market data..."):
            live_data = fetch_live_data()

        if live_data:
            # Price row
            price_html = "<div class='price-row'>"
            for metal in METALS:
                if metal in live_data:
                    p = live_data[metal]['latest_price']
                    d = live_data[metal]['latest_date']
                    date_str = pd.to_datetime(d).strftime('%d %b %Y') if hasattr(d, 'strftime') or isinstance(d, str) else str(d)
                    price_html += f"<div class='price-chip'><span>{metal.upper()}</span>${p:,.2f} <span style='font-size:0.65rem;color:#4a5568;'>· {date_str}</span></div>"
            price_html += "</div>"
            st.markdown(price_html, unsafe_allow_html=True)

            # Run predictions and show cards
            st.markdown("<div class='section-header'>RISK SCORES — ENHANCED MODEL</div>", unsafe_allow_html=True)
            cols = st.columns(3)
            shap_results = {}

            for i, metal in enumerate(METALS):
                prob, pred, shap_df = run_live_prediction(metal, live_data)
                shap_results[metal] = shap_df
                if prob is None:
                    with cols[i]:
                        st.warning(f"No data for {metal}")
                    continue

                score_class, badge_class, label = get_risk_color_class(prob)
                pct = int(prob * 100)

                with cols[i]:
                    st.markdown(f"""
                    <div class='risk-card'>
                        <div class='metal-name'>{metal}</div>
                        <div class='risk-score {score_class}'>{pct}%</div>
                        <div style='font-size:0.72rem;color:#8899aa;margin-bottom:0.5rem;'>
                            downside risk probability
                        </div>
                        <div class='risk-label {badge_class}'>{label}</div>
                    </div>
                    """, unsafe_allow_html=True)

            # SHAP explanations
            st.markdown("<div class='section-header'>TOP RISK DRIVERS — TODAY</div>", unsafe_allow_html=True)
            shap_cols = st.columns(3)
            for i, metal in enumerate(METALS):
                shap_df = shap_results.get(metal)
                with shap_cols[i]:
                    st.markdown(f"**{metal.upper()}**")
                    if shap_df is not None:
                        fig, ax = plt.subplots(figsize=(5, 4))
                        fig.patch.set_facecolor('#111827')
                        ax.set_facecolor('#111827')
                        colors = ['#ef4444' if v > 0 else '#22c55e' for v in shap_df['shap']]
                        bars = ax.barh(shap_df['feature'][::-1], shap_df['shap'][::-1], color=colors[::-1])
                        ax.axvline(0, color='#4a5568', linewidth=0.8)
                        ax.tick_params(colors='#8899aa', labelsize=7)
                        ax.spines[:].set_color('#1e2d45')
                        ax.set_xlabel('SHAP Value', color='#8899aa', fontsize=7)
                        ax.set_title(f'{metal.upper()} — Today\'s Drivers', color='#f0c040',
                                     fontsize=8, fontfamily='monospace')
                        plt.tight_layout()
                        st.pyplot(fig)
                        plt.close()
                    else:
                        st.info("SHAP unavailable")
    else:
        # Placeholder cards
        st.markdown("<div class='section-header'>AWAITING PREDICTION</div>", unsafe_allow_html=True)
        cols = st.columns(3)
        for i, metal in enumerate(METALS):
            with cols[i]:
                st.markdown(f"""
                <div class='risk-card'>
                    <div class='metal-name'>{metal}</div>
                    <div class='risk-score' style='color:#4a5568;'>--%</div>
                    <div style='font-size:0.72rem;color:#4a5568;margin-bottom:0.5rem;'>
                        click run prediction
                    </div>
                    <div class='risk-label' style='background:#1a2535;color:#4a5568;'>PENDING</div>
                </div>
                """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: MODEL PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

elif "📊" in page:
    st.markdown("""
    <div class='hero-title'>📊 Model Performance</div>
    <div class='hero-sub'>BASELINE VS ENHANCED · TEST SET METRICS</div>
    """, unsafe_allow_html=True)

    # Load metrics
    try:
        metrics_df = pd.read_csv(METRICS_CSV)
        if 'split' in metrics_df.columns:
            test_df = metrics_df[metrics_df['split'] == 'Test'].copy()
        else:
            test_df = metrics_df.copy()
    except Exception:
        st.error("Could not load metrics CSV. Ensure training has been run.")
        st.stop()

    # Metal selector
    selected_metal = st.selectbox("Select Metal", METALS, format_func=str.upper)

    metal_df   = test_df[test_df['metal'] == selected_metal]
    baseline_r = metal_df[metal_df['model'] == 'Baseline'].iloc[0] if len(metal_df[metal_df['model'] == 'Baseline']) > 0 else None
    enhanced_r = metal_df[metal_df['model'] == 'Enhanced'].iloc[0] if len(metal_df[metal_df['model'] == 'Enhanced']) > 0 else None

    if baseline_r is not None and enhanced_r is not None:
        st.markdown("<div class='section-header'>KEY METRICS COMPARISON</div>", unsafe_allow_html=True)

        metrics_to_show = [
            ('ROC-AUC', 'roc_auc', True),
            ('Avg Precision', 'avg_prec', True),
            ('Brier Score', 'brier', False),
            ('Log Loss', 'log_loss', False),
        ]

        cols = st.columns(4)
        for j, (label, key, higher_is_better) in enumerate(metrics_to_show):
            with cols[j]:
                b_val = baseline_r.get(key, baseline_r.get('roc_auc', 0))
                e_val = enhanced_r.get(key, enhanced_r.get('roc_auc', 0))
                delta = e_val - b_val
                if higher_is_better:
                    delta_class = 'delta-pos' if delta >= 0 else 'delta-neg'
                    delta_sym   = '▲' if delta >= 0 else '▼'
                else:
                    delta_class = 'delta-pos' if delta <= 0 else 'delta-neg'
                    delta_sym   = '▼' if delta <= 0 else '▲'

                st.markdown(f"""
                <div class='metric-box'>
                    <div class='m-label'>{label}</div>
                    <div class='m-value'>{e_val:.4f}</div>
                    <div class='m-delta {delta_class}'>
                        {delta_sym} {abs(delta):.4f} vs baseline
                    </div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<div class='section-header'>EVALUATION PLOTS</div>", unsafe_allow_html=True)

    # Show pre-generated plots
    plot_types = [
        (f'roc_curve_{selected_metal}',         'ROC Curve'),
        (f'pr_curve_{selected_metal}',           'Precision-Recall'),
        (f'calibration_{selected_metal}',        'Calibration'),
        (f'confusion_matrix_{selected_metal}_enhanced', 'Confusion Matrix'),
    ]

    plot_cols = st.columns(2)
    shown = 0
    for plot_key, plot_title in plot_types:
        path = os.path.join(PLOTS_DIR, f'{plot_key}.png')
        if os.path.exists(path):
            with plot_cols[shown % 2]:
                st.markdown(f"**{plot_title}**")
                st.image(path, use_column_width=True)
                shown += 1

    if shown == 0:
        # Fallback: show metrics table
        st.dataframe(
            test_df[['metal', 'model', 'roc_auc', 'avg_prec', 'brier', 'log_loss', 'f1']]
            .round(4)
            .style.background_gradient(cmap='YlOrRd', subset=['roc_auc'])
        )

    # Full comparison table
    st.markdown("<div class='section-header'>ALL METALS SUMMARY TABLE</div>", unsafe_allow_html=True)
    display_cols = ['metal', 'model', 'roc_auc', 'avg_prec', 'brier', 'log_loss', 'f1', 'n_test', 'n_risk']
    available_cols = [c for c in display_cols if c in test_df.columns]
    st.dataframe(
        test_df[available_cols].round(4).reset_index(drop=True),
        use_container_width=True
    )

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: SHAP ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

elif "🔍" in page:
    st.markdown("""
    <div class='hero-title'>🔍 SHAP Interpretability</div>
    <div class='hero-sub'>SHAPLEY ADDITIVE EXPLANATIONS · FEATURE IMPORTANCE</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class='info-banner'>
        SHAP (SHapley Additive exPlanations) quantifies each feature's contribution to the model's
        risk prediction. Bar charts show global feature importance; beeswarms show directionality —
        red indicates high feature values push risk up, blue indicates they push risk down.
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        selected_metal  = st.selectbox("Metal", METALS, format_func=str.upper, key='shap_metal')
    with col2:
        selected_model  = st.selectbox("Model", ['Baseline', 'Enhanced'], key='shap_model')

    label     = f"{selected_metal}_{selected_model.lower()}"
    bar_path  = os.path.join(PLOTS_DIR, f'shap_bar_{label}.png')
    bee_path  = os.path.join(PLOTS_DIR, f'shap_beeswarm_{label}.png')

    st.markdown("<div class='section-header'>GLOBAL FEATURE IMPORTANCE (BAR)</div>", unsafe_allow_html=True)
    if os.path.exists(bar_path):
        st.image(bar_path, use_column_width=True)
    else:
        st.warning(f"Plot not found: {bar_path}")

    st.markdown("<div class='section-header'>FEATURE DIRECTIONALITY (BEESWARM)</div>", unsafe_allow_html=True)
    if os.path.exists(bee_path):
        st.image(bee_path, use_column_width=True)
    else:
        st.warning(f"Plot not found: {bee_path}")

    # Key findings
    st.markdown("<div class='section-header'>KEY FINDINGS</div>", unsafe_allow_html=True)
    findings = {
        'gold':   "**Treasury Yield (10y)** dominates gold predictions. High yields push risk **down**, consistent with yield-driven safe-haven demand reducing downside probability. This aligns with established macroeconomic theory.",
        'silver': "**VIX** and **Treasury Yields** co-dominate silver. Silver's hybrid role as both monetary asset and industrial metal creates dual sensitivity to fear indices and rate expectations.",
        'copper': "**S&P 500 Return** dominates copper as a procyclical industrial barometer. Positive equity returns reduce copper downside risk, reflecting the strong link between global growth expectations and industrial demand."
    }
    st.markdown(findings.get(selected_metal, ""))

    st.markdown("""
    > **Note:** Sentiment features (`avg_sentiment`, `avg_positive` etc.) do not appear in the
    > top-20 SHAP rankings despite improving calibration metrics. This is attributed to sparse
    > sentiment coverage — only 31 trading days had associated headlines, limiting the model's
    > ability to learn reliable sentiment-risk relationships. This represents the primary
    > limitation of the study.
    """)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: BACKTESTING
# ─────────────────────────────────────────────────────────────────────────────

elif "📈" in page:
    st.markdown("""
    <div class='hero-title'>📈 Backtesting Results</div>
    <div class='hero-sub'>WALK-FORWARD VALIDATION · 2025 UNSEEN DATA</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class='info-banner'>
        Walk-forward backtesting was conducted on unseen 2025 data — entirely outside the
        training and validation windows. Each day's prediction uses only the features available
        at that point in time, simulating real-world sequential deployment without look-ahead bias.
    </div>
    """, unsafe_allow_html=True)

    # Backtest metrics summary
    st.markdown("<div class='section-header'>BACKTEST ROC-AUC SUMMARY</div>", unsafe_allow_html=True)

    backtest_results = {
        'gold':   {'baseline': 0.7140, 'enhanced': 0.8468},
        'silver': {'baseline': 0.7140, 'enhanced': 0.7214},
        'copper': {'baseline': 0.6257, 'enhanced': 0.6215},
    }

    bt_cols = st.columns(3)
    for i, metal in enumerate(METALS):
        b = backtest_results[metal]['baseline']
        e = backtest_results[metal]['enhanced']
        delta = e - b
        delta_class = 'delta-pos' if delta >= 0 else 'delta-neg'
        delta_sym   = '▲' if delta >= 0 else '▼'
        with bt_cols[i]:
            st.markdown(f"""
            <div class='metric-box'>
                <div class='m-label'>{metal.upper()} ENHANCED</div>
                <div class='m-value'>{e:.4f}</div>
                <div class='m-delta {delta_class}'>
                    {delta_sym} {abs(delta):.4f} vs baseline ({b:.4f})
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Backtest plots
    st.markdown("<div class='section-header'>WALK-FORWARD TIMELINES</div>", unsafe_allow_html=True)
    selected_metal = st.selectbox("Select Metal", METALS, format_func=str.upper, key='bt_metal')

    bt_plot_path = os.path.join(PLOTS_DIR, f'backtest_{selected_metal}.png')
    if os.path.exists(bt_plot_path):
        st.image(bt_plot_path, use_column_width=True)
    else:
        # Try alternative naming
        for fname in os.listdir(PLOTS_DIR):
            if 'backtest' in fname.lower() and selected_metal in fname.lower():
                st.image(os.path.join(PLOTS_DIR, fname), use_column_width=True)
                break
        else:
            st.info(f"Backtest plot for {selected_metal} not found in outputs/plots/")

    st.markdown("<div class='section-header'>INTERPRETATION</div>", unsafe_allow_html=True)
    bt_interp = {
        'gold':   "Gold's enhanced model achieved a backtest ROC-AUC of **0.847** on unseen 2025 data — substantially higher than the test set score of 0.752. This suggests strong generalisation to recent market conditions, likely reflecting that the macroeconomic regime in 2025 (elevated Treasury yields, geopolitical tensions) resembles the training patterns the model learned.",
        'silver': "Silver's enhanced model achieved **0.721** on the backtest vs 0.680 on test, showing consistent generalisation. The modest improvement over baseline (0.714) reflects the complexity of silver's dual monetary-industrial nature.",
        'copper': "Copper's enhanced model achieved **0.622** on the backtest, marginally below the baseline of 0.626. This reflects copper's sensitivity to global macro conditions that may have shifted between the training and backtest periods."
    }
    st.markdown(bt_interp.get(selected_metal, ""))
