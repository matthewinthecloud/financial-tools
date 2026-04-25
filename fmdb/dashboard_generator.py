#!/usr/bin/env python3
"""
Market Pulse Dashboard Generator
Queries fmdb Postgres, outputs self-contained HTML with charts.
Usage: python dashboard_generator.py [--output path/to/index.html]
"""
import sys, os, json, argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config import DB_URL

# ─────────────────────────────────────────────
# Tab definitions
# ─────────────────────────────────────────────
TABS = {
    "us_sectors": {
        "label": "US Sectors",
        "tickers": ["SPY","QQQ","XLK","XLC","XLY","XLP","XLV","XLF","XLI","XLE",
                    "XLU","XLRE","XLB","XRT","IYT","ITB","XTL","XBI","SMH","IGV",
                    "JETS","XHB","PPH","ITA","KRE"],
        "default_chart": ["SPY","XLK","XLC","XLY","XLP","XLV","XLF","XLI","XLE","XLU","XLRE"],
    },
    "country_etfs": {
        "label": "Country ETFs",
        "tickers": ["SPY","QQQ","VEA","VWO","IEMG","EMXC",
                    "EWZ","EWW","ECH","COLO","ARGT","FXI","EWH","KWEB","EWJ","EWT",
                    "EWY","KDEF","EWS","EWM","THD","EIDO","VNM","EWA","INDA","SMIN",
                    "TUR","KSA","UAE","EIS","CEE","FEZ","EWG","EWQ","EWP","EWO",
                    "EWD","EWN","EWL","EPOL","EUAD"],
        "default_chart": ["SPY","VEA","VWO","EWZ","FXI","EWJ","EWY","INDA","EWG","FEZ","IEMG"],
    },
    "thematic": {
        "label": "Thematic",
        "tickers": ["GLD","SLV","PPLT","PALL","SIL","SILJ","GDX","GDXJ","NIKL","CPER",
                    "XME","PICK","COPX","COPJ","TAN","REMX","SRUUF","URA","XLE","XOP",
                    "OIH","DBA","CMDY","IBIT","ETHA","BSOL","BLOK","WGMI","HYG","JNK",
                    "TLT","LQD","TIP","EMB","UVXY","VXX","VNQ","VNQI"],
        "default_chart": ["GLD","SLV","GDX","XLE","IBIT","TLT","HYG","VNQ","TAN","URA"],
    },
}

# Dedupe all tickers for data loading
_all = []
seen = set()
for t in TABS:
    for tick in TABS[t]["tickers"]:
        if tick not in seen:
            _all.append(tick)
            seen.add(tick)
ALL_TICKERS = _all

NAMES = {
    "SPY":"S&P 500 (US)","QQQ":"Nasdaq 100 (US)",
    "XLK":"Technology","XLC":"Comm Services","XLY":"Consumer Disc","XLP":"Consumer Staples",
    "XLV":"Health Care","XLF":"Financials","XLI":"Industrials","XLE":"Energy",
    "XLU":"Utilities","XLRE":"Real Estate","XLB":"Materials",
    "XRT":"Retail","IYT":"Transportation","ITB":"Homebuilders","XTL":"Telecom",
    "XBI":"Biotech","SMH":"Semiconductors","IGV":"Software","JETS":"Airlines",
    "XHB":"Homebuilders ETF","PPH":"Pharma","ITA":"Aerospace/Defense","KRE":"Regional Banks",
    "EWZ":"Brazil","EWW":"Mexico","ECH":"Chile","COLO":"Colombia","ARGT":"Argentina",
    "FXI":"China Large Cap","EWH":"Hong Kong","KWEB":"China Internet","EWJ":"Japan",
    "EWT":"Taiwan","EWY":"South Korea","KDEF":"Korea Defense","EWS":"Singapore",
    "EWM":"Malaysia","THD":"Thailand","EIDO":"Indonesia","VNM":"Vietnam",
    "EWA":"Australia","INDA":"India","SMIN":"India Small Cap","TUR":"Turkey",
    "KSA":"Saudi Arabia","UAE":"UAE","EIS":"Israel","CEE":"Central & Eastern Europe",
    "FEZ":"Euro Stoxx 50","EWG":"Germany","EWQ":"France","EWP":"Spain","EWO":"Austria",
    "EWD":"Sweden","EWN":"Netherlands","EWL":"Switzerland","EPOL":"Poland",
    "EUAD":"Europe Aero/Defense","VEA":"Developed Mkts","VWO":"Emerging Mkts",
    "IEMG":"Core EM","EMXC":"EM ex-China",
    "GLD":"Gold","SLV":"Silver","PPLT":"Platinum","PALL":"Palladium",
    "SIL":"Silver Miners","SILJ":"Jr Silver Miners","GDX":"Gold Miners",
    "GDXJ":"Jr Gold Miners","NIKL":"Nickel","CPER":"Copper","XME":"Metals & Mining",
    "PICK":"Global Mining","COPX":"Copper Miners","COPJ":"Jr Copper Miners",
    "TAN":"Solar","REMX":"Rare Earth","SRUUF":"Skyharbour Res","URA":"Uranium",
    "XOP":"Oil & Gas E&P","OIH":"Oil Services","DBA":"Agriculture","CMDY":"Commodities",
    "IBIT":"Bitcoin ETF","ETHA":"Ethereum ETF","BSOL":"Solana ETF",
    "BLOK":"Blockchain","WGMI":"Bitcoin Miners","HYG":"High Yield Corp",
    "JNK":"High Yield Bond","TLT":"20yr Treasury","LQD":"IG Corp Bond",
    "TIP":"TIPS","EMB":"EM Bond","UVXY":"Ultra VIX","VXX":"VIX Futures",
    "VNQ":"US REIT","VNQI":"Global REIT",
}

# 11 core SPDR sectors (default chart selection for US Sectors tab)
CORE_SPDRS = ["XLK","XLC","XLY","XLP","XLV","XLF","XLI","XLE","XLU","XLRE","XLB"]

# Chart.js palette — enough colors for up to 40 series
CHART_COLORS = [
    "#58a6ff","#2ea043","#f85149","#e3b341","#f0883e","#bc8cff","#56d364",
    "#ff7b72","#79c0ff","#ffa657","#d2a8ff","#3fb950","#ff6e64","#a5f3fc",
    "#fbbf24","#a78bfa","#34d399","#fb7185","#60a5fa","#facc15","#c084fc",
    "#4ade80","#f87171","#38bdf8","#fde68a","#818cf8","#86efac","#fca5a5",
    "#7dd3fc","#fef08a","#c4b5fd","#a3e635","#fda4af","#93c5fd","#fde047",
    "#ddd6fe","#bbf7d0","#fecaca","#bae6fd","#fef9c3",
]


def _f(v):
    if v is None: return None
    try:
        import math
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 4)
    except: return None


def load_analytics(engine):
    q = """
        SELECT ap.instrument_id, ap.date,
               ap.ret_1d,ap.ret_1w,ap.ret_1m,ap.ret_3m,ap.ret_6m,ap.ret_12m,ap.ret_ytd,
               ap.vol_20d,ap.rsi_14,ap.dist_ma50,ap.dist_ma200,
               ap.rmi_signal,ap.rmi_value,ap.golden_cross
        FROM analytics_prices ap
        WHERE ap.date = (SELECT MAX(date) FROM analytics_prices ap2 WHERE ap2.instrument_id=ap.instrument_id)
    """
    return pd.read_sql(text(q), engine)


def load_price_history(engine, tickers):
    """Load up to 2 years of daily closes for charting + z-score."""
    tpl = "','".join(tickers)
    q = f"""
        SELECT instrument_id, date, adj_close
        FROM raw_prices
        WHERE instrument_id IN ('{tpl}')
          AND date >= CURRENT_DATE - INTERVAL '730 days'
        ORDER BY instrument_id, date
    """
    return pd.read_sql(text(q), engine)


def build_chart_series(hist_df, tickers, tf_days, normalized=True):
    """
    Build price series for the given lookback.
    normalized=True: base 100. normalized=False: real prices.
    Returns {ticker: [{x: 'YYYY-MM-DD', y: float}, ...]}
    """
    result = {}
    for ticker in tickers:
        sub = hist_df[hist_df['instrument_id'] == ticker].sort_values('date')
        if sub.empty or len(sub) < 2:
            continue
        if tf_days == 0:  # YTD
            ytd_start = pd.Timestamp(sub['date'].max().year, 1, 1)
            sub = sub[pd.to_datetime(sub['date']) >= ytd_start]
        else:
            sub = sub.tail(tf_days + 5).tail(tf_days) if tf_days <= len(sub) else sub
        if sub.empty or len(sub) < 2:
            continue
        if normalized:
            base = sub['adj_close'].iloc[0]
            if base == 0: continue
            points = [{"x": str(row['date']), "y": round(float(row['adj_close']) / base * 100, 3)}
                      for _, row in sub.iterrows()]
        else:
            points = [{"x": str(row['date']), "y": round(float(row['adj_close']), 4)}
                      for _, row in sub.iterrows()]
        result[ticker] = points
    return result


def build_rsi_series(hist_df, tickers, tf_days, period=14):
    """Build RSI(14) series for the modal indicator panel."""
    result = {}
    for ticker in tickers:
        sub = hist_df[hist_df['instrument_id'] == ticker].sort_values('date').copy()
        if len(sub) < period + 5:
            continue
        close = sub['adj_close']
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta).clip(lower=0).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = (100 - (100 / (1 + rs))).round(2)
        sub = sub.copy()
        sub['rsi'] = rsi
        # trim to tf window
        if tf_days == 0:
            ytd_start = pd.Timestamp(sub['date'].max().year, 1, 1)
            sub = sub[pd.to_datetime(sub['date']) >= ytd_start]
        elif tf_days <= len(sub):
            sub = sub.tail(tf_days)
        sub = sub.dropna(subset=['rsi'])
        if len(sub) < 2:
            continue
        result[ticker] = [{"x": str(row['date']), "y": float(row['rsi'])}
                          for _, row in sub.iterrows()]
    return result


def compute_zscores(hist_df, tickers):
    zscores = {}
    for ticker in tickers:
        sub = hist_df[hist_df['instrument_id'] == ticker].sort_values('date')
        if len(sub) < 30: continue
        prices = sub['adj_close']
        ret_1w = prices.pct_change(5) * 100
        ret_1m = prices.pct_change(21) * 100
        def zs_latest(s):
            s = s.dropna()
            if len(s) < 10 or s.std() == 0: return None
            return round(float((s.iloc[-1] - s.mean()) / s.std()), 3)
        price_std = prices.std()
        zscores[ticker] = {
            'zscore_ret1w': zs_latest(ret_1w),
            'zscore_ret1m': zs_latest(ret_1m),
            'zscore_price': round(float((prices.iloc[-1]-prices.mean())/price_std), 3) if price_std>0 else None,
        }
    return zscores


def build_data(engine):
    ana = load_analytics(engine)
    if ana.empty:
        return {}, {}, {}, {}, datetime.now().strftime('%Y-%m-%d')

    hist = load_price_history(engine, ALL_TICKERS)
    zscores = compute_zscores(hist, ALL_TICKERS)

    # Build per-timeframe chart series (normalized for group chart)
    TF_DAYS = {"ret_1w":5, "ret_1m":21, "ret_3m":63, "ret_6m":126, "ret_12m":252, "ret_ytd":0}
    chart_series = {tf: build_chart_series(hist, ALL_TICKERS, days, normalized=True)
                    for tf, days in TF_DAYS.items()}
    # Real price series for modal (use longest window — 2yr)
    real_series = build_chart_series(hist, ALL_TICKERS, 504, normalized=False)
    # RSI series for modal indicator panel
    rsi_series  = build_rsi_series(hist, ALL_TICKERS, 504)

    instruments = {}
    for _, row in ana.iterrows():
        iid = row['instrument_id']
        zs = zscores.get(iid, {})
        instruments[iid] = {
            'ticker':      iid,
            'name':        NAMES.get(iid, iid),
            'ret_1w':      _f(row['ret_1w']),
            'ret_1m':      _f(row['ret_1m']),
            'ret_3m':      _f(row['ret_3m']),
            'ret_6m':      _f(row['ret_6m']),
            'ret_12m':     _f(row['ret_12m']),
            'ret_ytd':     _f(row['ret_ytd']),
            'vol_20d':     _f(row['vol_20d']),
            'rsi_14':      _f(row['rsi_14']),
            'dist_ma50':   _f(row['dist_ma50']),
            'dist_ma200':  _f(row['dist_ma200']),
            'rmi_signal':  row['rmi_signal'] or 'neutral',
            'zscore_ret1w': _f(zs.get('zscore_ret1w')),
            'zscore_ret1m': _f(zs.get('zscore_ret1m')),
            'zscore_price': _f(zs.get('zscore_price')),
            'golden_cross': int(row['golden_cross']) if row['golden_cross'] is not None else 0,
        }

    as_of = str(ana['date'].max())
    return instruments, chart_series, real_series, rsi_series, as_of


def generate_html(instruments, chart_series, real_series, rsi_series, as_of, tabs):
    tabs_json    = json.dumps({k: v['tickers'] for k, v in tabs.items()})
    tab_labels   = json.dumps({k: v['label']   for k, v in tabs.items()})
    tab_defaults = json.dumps({k: v.get('default_chart', v['tickers'][:10]) for k, v in tabs.items()})
    data_json    = json.dumps(instruments)
    names_json   = json.dumps(NAMES)
    chart_json   = json.dumps(chart_series)
    real_json    = json.dumps(real_series)
    rsi_json     = json.dumps(rsi_series)
    colors_json  = json.dumps(CHART_COLORS)
    core_json    = json.dumps(CORE_SPDRS)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Market Pulse</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"><\/script>
<style>
:root{{
  --bg:#0d1117;--card:#161b22;--border:#30363d;--text:#f0f6fc;--muted:#8b949e;
  --green:#2ea043;--green-bg:#0d2818;--red:#f85149;--red-bg:#2d1117;
  --yellow:#e3b341;--yellow-bg:#2d2000;--orange:#f0883e;--blue:#58a6ff;
  --accent:#58a6ff;
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;}}
.header{{padding:16px 16px 0;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px;}}
.header h1{{font-size:22px;font-weight:700;}}
.header .subtitle{{color:var(--muted);font-size:11px;margin-top:2px;}}
/* Timeframe */
.tf-bar{{display:flex;gap:6px;padding:12px 16px;overflow-x:auto;-webkit-overflow-scrolling:touch;}}
.tf-btn{{background:var(--card);border:1px solid var(--border);color:var(--muted);padding:7px 14px;border-radius:20px;cursor:pointer;font-size:13px;white-space:nowrap;transition:all .15s;}}
.tf-btn.active{{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600;}}
/* Tabs */
.tab-nav{{display:flex;padding:0 16px;border-bottom:1px solid var(--border);overflow-x:auto;-webkit-overflow-scrolling:touch;}}
.tab-btn{{background:none;border:none;color:var(--muted);padding:10px 14px;cursor:pointer;font-size:13px;white-space:nowrap;border-bottom:2px solid transparent;transition:color .15s;}}
.tab-btn.active{{color:var(--accent);border-bottom-color:var(--accent);font-weight:600;}}
/* Content */
.tab-panel{{display:none;padding:16px;}}
.tab-panel.active{{display:block;}}
/* Section headers */
.section-hdr{{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin:20px 0 10px;}}
/* Heatmap */
.heatmap{{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:4px;}}
.heat-tile{{border-radius:7px;padding:7px 9px;min-width:66px;flex:1 1 66px;max-width:92px;text-align:center;cursor:pointer;transition:transform .1s,opacity .1s;}}
.heat-tile:hover{{transform:scale(1.06);opacity:.9;}}
.heat-tile .ht-t{{font-size:11px;font-weight:700;}}
.heat-tile .ht-r{{font-size:10px;margin-top:1px;opacity:.9;}}
/* Chart area */
.chart-area{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:4px;}}
.chart-toolbar{{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:10px;}}
.chart-title-txt{{font-size:12px;font-weight:600;color:var(--muted);}}
.quick-btns{{display:flex;gap:5px;flex-wrap:wrap;}}
.quick-btn{{background:var(--bg);border:1px solid var(--border);color:var(--muted);padding:4px 10px;border-radius:14px;cursor:pointer;font-size:11px;transition:all .15s;}}
.quick-btn:hover{{border-color:var(--accent);color:var(--accent);}}
canvas#groupChart{{width:100%!important;height:280px!important;}}
/* Checkbox panel */
.cb-panel{{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px;max-height:120px;overflow-y:auto;padding:2px;}}
.cb-label{{display:flex;align-items:center;gap:4px;background:var(--bg);border:1px solid var(--border);border-radius:14px;padding:4px 10px;cursor:pointer;font-size:11px;color:var(--muted);transition:all .15s;user-select:none;}}
.cb-label.checked{{border-color:var(--accent);color:var(--text);}}
.cb-label input{{display:none;}}
.cb-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;}}
/* Table */
.tbl-wrap{{overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:4px;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
th{{background:var(--card);color:var(--muted);padding:8px 9px;text-align:left;white-space:nowrap;cursor:pointer;user-select:none;position:sticky;top:0;z-index:1;border-bottom:1px solid var(--border);}}
th:hover{{color:var(--text);}}
.sort-arrow{{font-size:9px;margin-left:2px;opacity:.4;}}
th.sorted .sort-arrow{{opacity:1;}}
td{{padding:7px 9px;border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;}}
tr:hover td{{background:rgba(255,255,255,.03);}}
.rmi-bull{{color:#2ea043;font-weight:600;}}
.rmi-bear{{color:#f85149;font-weight:600;}}
.rmi-neut{{color:#e3b341;font-weight:600;}}
.ret-pos{{color:#2ea043;}}
.ret-neg{{color:#f85149;}}
.z-high{{color:#f0883e;font-weight:600;}}
.z-low{{color:#58a6ff;font-weight:600;}}
/* Bar chart */
.bars{{display:flex;flex-direction:column;gap:4px;}}
.bar-row{{display:flex;align-items:center;gap:7px;cursor:pointer;}}
.bar-row:hover .bar-label{{color:var(--text);}}
.bar-label{{width:60px;font-size:10px;color:var(--muted);text-align:right;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;}}
.bar-track{{flex:1;background:var(--card);border-radius:3px;height:16px;overflow:hidden;position:relative;}}
.bar-fill{{height:100%;border-radius:3px;min-width:2px;}}
.bar-val{{position:absolute;right:5px;top:50%;transform:translateY(-50%);font-size:10px;font-weight:600;}}
/* Modal */
.modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;align-items:center;justify-content:center;padding:16px;}}
.modal-overlay.open{{display:flex;}}
.modal{{background:var(--card);border:1px solid var(--border);border-radius:12px;width:100%;max-width:680px;max-height:90vh;overflow-y:auto;padding:20px;position:relative;}}
.modal-close{{position:absolute;top:12px;right:14px;background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;line-height:1;}}
.modal-ticker{{font-size:24px;font-weight:700;}}
.modal-name{{color:var(--muted);font-size:13px;margin-top:2px;margin-bottom:16px;}}
.modal-stats{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px;}}
.stat-card{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px 14px;min-width:90px;}}
.stat-label{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;}}
.stat-val{{font-size:16px;font-weight:700;margin-top:3px;}}
.modal-chart-label{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin:12px 0 6px;}}
canvas#modalPriceChart{{width:100%!important;height:200px!important;}}
canvas#modalRsiChart{{width:100%!important;height:120px!important;}}
@media(max-width:480px){{
  .header h1{{font-size:18px;}}
  th,td{{padding:6px 7px;font-size:11px;}}
  .heat-tile{{min-width:58px;}}
  .modal{{padding:14px;}}
  .modal-ticker{{font-size:20px;}}
  canvas#groupChart{{height:220px!important;}}
}}
</style>
</head>
<body>
<div class="header">
  <div><h1>📊 Market Pulse</h1><div class="subtitle">As of {as_of} · fmdb</div></div>
</div>
<div class="tf-bar" id="tf-bar">
  <button class="tf-btn" data-tf="ret_1w">1W</button>
  <button class="tf-btn active" data-tf="ret_1m">1M</button>
  <button class="tf-btn" data-tf="ret_3m">3M</button>
  <button class="tf-btn" data-tf="ret_6m">6M</button>
  <button class="tf-btn" data-tf="ret_12m">12M</button>
  <button class="tf-btn" data-tf="ret_ytd">YTD</button>
</div>
<div class="tab-nav" id="tab-nav">
  <button class="tab-btn active" data-tab="us_sectors">US Sectors</button>
  <button class="tab-btn" data-tab="country_etfs">Country ETFs</button>
  <button class="tab-btn" data-tab="thematic">Thematic</button>
</div>
<div id="panels"></div>

<!-- Modal -->
<div class="modal-overlay" id="modal">
  <div class="modal">
    <button class="modal-close" id="modal-close">✕</button>
    <div class="modal-ticker" id="modal-ticker"></div>
    <div class="modal-name" id="modal-name"></div>
    <div class="modal-stats" id="modal-stats"></div>
    <div class="modal-chart-label">Price (2Y)</div>
    <canvas id="modalPriceChart"></canvas>
    <div class="modal-chart-label">RSI (14)</div>
    <canvas id="modalRsiChart"></canvas>
  </div>
</div>

<script>
const DATA      = {data_json};
const TABS      = {tabs_json};
const TAB_DEF   = {{tab_defaults}};
const NAMES     = {names_json};
const CHART_SRS = {chart_json};
const REAL_SRS  = {real_json};
const RSI_SRS   = {rsi_json};
const COLORS    = {colors_json};

let currentTf   = 'ret_1m';
let currentTab  = 'us_sectors';
let sortState   = {{}};
let groupCharts = {{}};   // tabId -> Chart instance
let modalPChart = null;
let modalRChart = null;
let activeChecks = {{}};  // tabId -> Set

// ── Helpers ────────────────────────────────────────────────────────
function retColor(v, intensity=true) {{
  if(v==null) return '#333';
  const abs=Math.min(Math.abs(v)/15,1);
  if(!intensity) return v>=0?'#2ea043':'#f85149';
  if(v>=0){{const r=Math.round(14+abs*32),g=Math.round(100+abs*60),b=Math.round(56+abs*16);return `rgb(${{r}},${{g}},${{b}})`;}}
  else{{const r=Math.round(100+abs*148),g=Math.round(30+abs*51),b=Math.round(30+abs*43);return `rgb(${{r}},${{g}},${{b}})`;}}
}}
function fmt(v,d=2){{
  if(v==null) return '<span style="color:#555">—</span>';
  return `<span class="${{v>=0?'ret-pos':'ret-neg'}}">${{(v>=0?'+':'')+v.toFixed(d)+'%'}}</span>`;
}}
function fmtPlain(v){{ return v==null?'—':(v>=0?'+':'')+v.toFixed(2)+'%'; }}
function rmiHtml(s){{
  if(s==='bullish') return '<span class="rmi-bull">▲ BULL</span>';
  if(s==='bearish') return '<span class="rmi-bear">▼ BEAR</span>';
  return '<span class="rmi-neut">◆ NEUT</span>';
}}
function zsHtml(v){{
  if(v==null) return '<span style="color:#555">—</span>';
  const s=(v>=0?'+':'')+v.toFixed(2);
  if(v>2)  return `<span class="z-high">${{s}} ⚡</span>`;
  if(v<-2) return `<span class="z-low">${{s}} 💧</span>`;
  return s;
}}
function getTabRows(tabId){{ return (TABS[tabId]||[]).map(t=>DATA[t]).filter(Boolean); }}
function ranked(rows,tf){{
  const sorted=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  const rm={{}};sorted.forEach((r,i)=>rm[r.ticker]=i+1);
  return rows.map(r=>Object.assign({{}},r,{{rank:rm[r.ticker]??999}}));
}}
function tfUnit(tf){{ return (tf==='ret_1w'||tf==='ret_1m')?'day':'month'; }}

function baseChartOpts(unit){{
  return {{
    responsive:true, maintainAspectRatio:false, animation:{{duration:200}},
    interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{display:false}},
      tooltip:{{backgroundColor:'#1c2128',borderColor:'#30363d',borderWidth:1,
               titleColor:'#f0f6fc',bodyColor:'#8b949e'}},
    }},
    scales:{{
      x:{{type:'time',time:{{unit}},ticks:{{color:'#8b949e',maxTicksLimit:unit==='day'?10:8}},grid:{{color:'#21262d'}}}},
      y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#21262d'}}}},
    }},
  }};
}}

// ── Heatmap ────────────────────────────────────────────────────────
function buildHeatmap(rows,tf){{
  const s=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  return '<div class="heatmap">'+s.map(r=>{{
    const v=r[tf],bg=retColor(v),tc=v==null?'#888':(Math.abs(v)>4?'#fff':'#eee');
    return `<div class="heat-tile" style="background:${{bg}};color:${{tc}}" onclick="openModal('${{r.ticker}}')" title="${{r.name}}">
      <div class="ht-t">${{r.ticker}}</div>
      <div class="ht-r">${{v!=null?(v>=0?'+':'')+v.toFixed(1)+'%':'—'}}</div></div>`;
  }}).join('')+'</div>';
}}

// ── Group chart (each tab gets its own canvas id) ─────────────────
function buildGroupChart(tabId,tf){{
  const tickers=TABS[tabId]||[];
  const active=[...activeChecks[tabId]];
  const series=CHART_SRS[tf]||{{}};
  const datasets=active.filter(t=>series[t]).map(t=>{{
    const idx=tickers.indexOf(t);
    const color=COLORS[(idx>=0?idx:0)%COLORS.length];
    return {{label:t,data:series[t],borderColor:color,backgroundColor:color+'18',
            borderWidth:1.8,pointRadius:0,tension:.3,parsing:{{xAxisKey:'x',yAxisKey:'y'}}}};
  }});
  const canvasId='gc-'+tabId;
  const ctx=document.getElementById(canvasId);
  if(!ctx) return;
  if(groupCharts[tabId]){{groupCharts[tabId].destroy();delete groupCharts[tabId];}}
  const opts=baseChartOpts(tfUnit(tf));
  opts.plugins.tooltip.callbacks={{label:c=>`${{c.dataset.label}}: ${{c.parsed.y?.toFixed(1)}}`}};
  opts.scales.y.title={{display:true,text:'Normalized (base=100)',color:'#8b949e',font:{{size:10}}}};
  opts.scales.y.ticks={{...opts.scales.y.ticks,callback:v=>v.toFixed(0)}};
  groupCharts[tabId]=new Chart(ctx,{{type:'line',data:{{datasets}},options:opts}});
}}

// ── Checkbox panel ─────────────────────────────────────────────────
function buildCbPanel(tabId){{
  const tickers=TABS[tabId]||[];
  const active=activeChecks[tabId];
  return tickers.map((t,i)=>{{
    const color=COLORS[i%COLORS.length];
    const chk=active.has(t);
    return `<label class="cb-label${{chk?' checked':''}}" data-ticker="${{t}}" data-tab="${{tabId}}">
      <input type="checkbox"${{chk?' checked':''}}/>
      <span class="cb-dot" style="background:${{color}}"></span>${{t}}</label>`;
  }}).join('');
}}

function buildChartSection(tabId){{
  return `<div class="chart-area">
    <div class="chart-toolbar">
      <span class="chart-title-txt">Normalized Performance (base 100)</span>
      <div class="quick-btns">
        <button class="quick-btn" data-action="default" data-tab="${{tabId}}">Default</button>
        <button class="quick-btn" data-action="all"     data-tab="${{tabId}}">All</button>
        <button class="quick-btn" data-action="none"    data-tab="${{tabId}}">None</button>
      </div>
    </div>
    <canvas id="gc-${{tabId}}" style="width:100%;height:280px;"></canvas>
    <div class="cb-panel" id="cb-${{tabId}}">${{buildCbPanel(tabId)}}</div>
  </div>`;
}}

// ── Table ──────────────────────────────────────────────────────────
const COLS=[
  {{id:'rank',      label:'#',        sv:r=>r.rank,             html:r=>`<b>${{r.rank}}</b>`}},
  {{id:'ticker',    label:'Ticker',   sv:r=>r.ticker,           html:r=>`<b>${{r.ticker}}</b>`}},
  {{id:'name',      label:'Name',     sv:r=>r.name,             html:r=>`<span style="color:var(--muted)">${{r.name}}</span>`}},
  {{id:'ret',       label:'Return',   sv:r=>r[currentTf]??-999, html:r=>fmt(r[currentTf])}},
  {{id:'rmi_signal',label:'RMI',      sv:r=>r.rmi_signal,       html:r=>rmiHtml(r.rmi_signal)}},
  {{id:'rsi_14',    label:'RSI',      sv:r=>r.rsi_14??-1,       html:r=>r.rsi_14!=null?r.rsi_14.toFixed(1):'—'}},
  {{id:'dist_ma50', label:'vs 50d',   sv:r=>r.dist_ma50??-999,  html:r=>fmt(r.dist_ma50)}},
  {{id:'dist_ma200',label:'vs 200d',  sv:r=>r.dist_ma200??-999, html:r=>fmt(r.dist_ma200)}},
  {{id:'vol_20d',   label:'Vol 20d',  sv:r=>r.vol_20d??-1,      html:r=>r.vol_20d!=null?r.vol_20d.toFixed(1)+'%':'—'}},
  {{id:'zscore',    label:'Z-Score',  sv:r=>r.zscore_ret1m??-99,html:r=>zsHtml(r.zscore_ret1m)}},
];

function buildTable(rows,tabId,tf){{
  const st=sortState[tabId]||{{col:'rank',dir:1}};
  const s=[...rows].sort((a,b)=>{{
    const col=COLS.find(c=>c.id===st.col);
    if(!col) return 0;
    const va=col.sv(a),vb=col.sv(b);
    return typeof va==='string'?st.dir*va.localeCompare(vb):st.dir*(va-vb);
  }});
  const thead='<thead><tr>'+COLS.map(c=>{{
    const act=st.col===c.id;
    return `<th class="${{act?'sorted':''}}" data-col="${{c.id}}" data-tab="${{tabId}}">${{c.label}}<span class="sort-arrow">${{act?(st.dir===1?'▲':'▼'):'⇅'}}</span></th>`;
  }}).join('')+'</tr></thead>';
  const tbody='<tbody>'+s.map(r=>`<tr onclick="openModal('${{r.ticker}}')">${{COLS.map(c=>`<td>${{c.html(r)}}</td>`).join('')}}</tr>`).join('')+'</tbody>';
  return `<div class="tbl-wrap" data-tbl="${{tabId}}"><table>${{thead}}${{tbody}}</table></div>`;
}}

// ── Bars ───────────────────────────────────────────────────────────
function buildBars(rows,tf){{
  const s=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  const maxA=Math.max(...s.map(r=>Math.abs(r[tf]??0)),0.01);
  return '<div class="bars">'+s.map(r=>{{
    const v=r[tf],pct=v!=null?Math.abs(v)/maxA*100:0,c=retColor(v,false);
    return `<div class="bar-row" onclick="openModal('${{r.ticker}}')">
      <div class="bar-label" title="${{r.name}}">${{r.ticker}}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${{pct.toFixed(1)}}%;background:${{c}}"></div>
      <span class="bar-val" style="color:${{c}}">${{fmtPlain(v)}}</span></div></div>`;
  }}).join('')+'</div>';
}}

// ── Modal ──────────────────────────────────────────────────────────
function openModal(ticker){{
  const d=DATA[ticker]; if(!d) return;
  document.getElementById('modal-ticker').textContent=ticker;
  document.getElementById('modal-name').textContent=d.name;

  const stats=[
    {{label:'1M',    val:fmtPlain(d.ret_1m),  cls:d.ret_1m>=0?'ret-pos':'ret-neg'}},
    {{label:'3M',    val:fmtPlain(d.ret_3m),  cls:d.ret_3m>=0?'ret-pos':'ret-neg'}},
    {{label:'12M',   val:fmtPlain(d.ret_12m), cls:(d.ret_12m??0)>=0?'ret-pos':'ret-neg'}},
    {{label:'YTD',   val:fmtPlain(d.ret_ytd), cls:(d.ret_ytd??0)>=0?'ret-pos':'ret-neg'}},
    {{label:'RSI 14',val:d.rsi_14!=null?d.rsi_14.toFixed(1):'—',cls:''}},
    {{label:'RMI',   val:(d.rmi_signal||'neutral').toUpperCase(),
      cls:d.rmi_signal==='bullish'?'rmi-bull':d.rmi_signal==='bearish'?'rmi-bear':'rmi-neut'}},
    {{label:'vs 50d', val:fmtPlain(d.dist_ma50), cls:(d.dist_ma50??0)>=0?'ret-pos':'ret-neg'}},
    {{label:'Vol 20d',val:d.vol_20d!=null?d.vol_20d.toFixed(1)+'%':'—',cls:''}},
    {{label:'Z-Score',val:d.zscore_ret1m!=null?(d.zscore_ret1m>=0?'+':'')+d.zscore_ret1m.toFixed(2):'—',
      cls:d.zscore_ret1m>2?'z-high':d.zscore_ret1m<-2?'z-low':''}},
  ];
  document.getElementById('modal-stats').innerHTML=stats.map(s=>
    `<div class="stat-card"><div class="stat-label">${{s.label}}</div><div class="stat-val ${{s.cls}}">${{s.val}}</div></div>`
  ).join('');

  if(modalPChart){{modalPChart.destroy();modalPChart=null;}}
  if(modalRChart){{modalRChart.destroy();modalRChart=null;}}

  const unit=tfUnit(currentTf);
  const pricePts=REAL_SRS[ticker]||[];
  const rsiPts  =RSI_SRS[ticker]  ||[];

  if(pricePts.length>1){{
    const pOpts=baseChartOpts('month');  // real price always shows 2yr
    pOpts.scales.y.ticks.callback=v=>'$'+v.toFixed(2);
    pOpts.plugins.tooltip.callbacks={{label:c=>`$${{c.parsed.y?.toFixed(2)}}`}};
    modalPChart=new Chart(document.getElementById('modalPriceChart'),{{
      type:'line',
      data:{{datasets:[{{label:ticker,data:pricePts,borderColor:'#58a6ff',
        backgroundColor:'#58a6ff22',borderWidth:2,pointRadius:0,tension:.3,
        parsing:{{xAxisKey:'x',yAxisKey:'y'}},fill:true}}]}},
      options:pOpts,
    }});
  }}

  if(rsiPts.length>1){{
    const rOpts=baseChartOpts('month');
    rOpts.scales.y={{...rOpts.scales.y,min:0,max:100,ticks:{{...rOpts.scales.y.ticks,callback:v=>v}}}};
    rOpts.plugins.tooltip.callbacks={{label:c=>`RSI: ${{c.parsed.y?.toFixed(1)}}`}};
    modalRChart=new Chart(document.getElementById('modalRsiChart'),{{
      type:'line',
      data:{{datasets:[
        {{label:'RSI',data:rsiPts,borderColor:'#f0883e',backgroundColor:'#f0883e18',
         borderWidth:1.5,pointRadius:0,tension:.3,parsing:{{xAxisKey:'x',yAxisKey:'y'}},fill:true}},
        {{label:'OB70',data:rsiPts.map(p=>({{x:p.x,y:70}})),borderColor:'#f8514944',
         borderWidth:1,pointRadius:0,borderDash:[4,4],parsing:{{xAxisKey:'x',yAxisKey:'y'}}}},
        {{label:'OS30',data:rsiPts.map(p=>({{x:p.x,y:30}})),borderColor:'#2ea04344',
         borderWidth:1,pointRadius:0,borderDash:[4,4],parsing:{{xAxisKey:'x',yAxisKey:'y'}}}},
      ]}},
      options:rOpts,
    }});
  }}

  document.getElementById('modal').classList.add('open');
}}

function closeModal(){{
  document.getElementById('modal').classList.remove('open');
  if(modalPChart){{modalPChart.destroy();modalPChart=null;}}
  if(modalRChart){{modalRChart.destroy();modalRChart=null;}}
}}

// ── Render tab ─────────────────────────────────────────────────────
function renderTab(tabId,tf){{
  const rows=ranked(getTabRows(tabId),tf);
  const panel=document.getElementById('panel-'+tabId);
  if(!panel) return;
  if(groupCharts[tabId]){{groupCharts[tabId].destroy();delete groupCharts[tabId];}}

  panel.innerHTML=
    '<div class="section-hdr">Heatmap</div>'+buildHeatmap(rows,tf)+
    '<div class="section-hdr">Performance Chart</div>'+buildChartSection(tabId)+
    '<div class="section-hdr">Rankings</div>'+buildTable(rows,tabId,tf)+
    '<div class="section-hdr" style="margin-top:16px">Relative Strength</div>'+buildBars(rows,tf);

  // Checkbox: event delegation on panel (not per-element) — fixes click bug
  panel.addEventListener('click', e => {{
    // Quick buttons
    const btn=e.target.closest('[data-action]');
    if(btn && btn.dataset.tab===tabId){{
      const action=btn.dataset.action;
      if(action==='default') activeChecks[tabId]=new Set(TAB_DEF[tabId]||[]);
      else if(action==='all')  activeChecks[tabId]=new Set(TABS[tabId]||[]);
      else if(action==='none') activeChecks[tabId]=new Set();
      document.getElementById('cb-'+tabId).innerHTML=buildCbPanel(tabId);
      buildGroupChart(tabId,currentTf);
      return;
    }}
    // Checkbox labels
    const lbl=e.target.closest('.cb-label');
    if(lbl && lbl.dataset.tab===tabId){{
      e.preventDefault();
      const t=lbl.dataset.ticker;
      if(activeChecks[tabId].has(t)) activeChecks[tabId].delete(t);
      else activeChecks[tabId].add(t);
      lbl.classList.toggle('checked',activeChecks[tabId].has(t));
      const cb=lbl.querySelector('input');
      if(cb) cb.checked=activeChecks[tabId].has(t);
      buildGroupChart(tabId,currentTf);
      return;
    }}
    // Table sort
    const th=e.target.closest('th[data-col]');
    if(th && th.dataset.tab===tabId){{
      const col=th.dataset.col;
      const st=sortState[tabId]||{{col:'rank',dir:1}};
      sortState[tabId]=col===st.col?{{col,dir:-st.dir}}:{{col,dir:1}};
      const tbl=panel.querySelector('[data-tbl]');
      if(tbl) tbl.outerHTML=buildTable(ranked(getTabRows(tabId),currentTf),tabId,currentTf);
      return;
    }}
  }});

  buildGroupChart(tabId,tf);
}}

// ── Init ───────────────────────────────────────────────────────────
function init(){{
  Object.keys(TABS).forEach(id=>{{ activeChecks[id]=new Set(TAB_DEF[id]||[]); }});

  const panels=document.getElementById('panels');
  Object.keys(TABS).forEach(id=>{{
    const div=document.createElement('div');
    div.className='tab-panel'+(id===currentTab?' active':'');
    div.id='panel-'+id;
    panels.appendChild(div);
  }});

  document.getElementById('tf-bar').addEventListener('click',e=>{{
    const btn=e.target.closest('.tf-btn'); if(!btn) return;
    currentTf=btn.dataset.tf;
    document.querySelectorAll('.tf-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    renderTab(currentTab,currentTf);
  }});

  document.getElementById('tab-nav').addEventListener('click',e=>{{
    const btn=e.target.closest('.tab-btn'); if(!btn) return;
    currentTab=btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    const panel=document.getElementById('panel-'+currentTab);
    panel.classList.add('active');
    renderTab(currentTab,currentTf);
  }});

  document.getElementById('modal-close').addEventListener('click',closeModal);
  document.getElementById('modal').addEventListener('click',e=>{{ if(e.target.id==='modal') closeModal(); }});

  renderTab(currentTab,currentTf);
}}

document.addEventListener('DOMContentLoaded',init);
</script>
</body>
</html>"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default=os.path.join(
        os.path.dirname(__file__), '..', 'dashboard', 'index.html'))
    args = parser.parse_args()

    print("Connecting to database...")
    engine = create_engine(DB_URL)
    print("Loading data...")
    instruments, chart_series, real_series, rsi_series, as_of = build_data(engine)
    print(f"  {len(instruments)} instruments, as of {as_of}")
    print("Generating HTML...")
    html = generate_html(instruments, chart_series, real_series, rsi_series, as_of, TABS)
    out = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        f.write(html)
    print(f"  Written: {out} ({len(html)//1024}KB)")

if __name__ == '__main__':
    main()
