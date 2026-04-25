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
    "full_universe": {
        "label": "All",
        "tickers": [],  # populated below
        "default_chart": ["SPY","QQQ","GLD","TLT","IBIT","XLK","EWZ","FXI","EWJ","VWO"],
    },
}

# Populate full universe
_all = []
seen = set()
for t in TABS:
    if t == "full_universe": continue
    for tick in TABS[t]["tickers"]:
        if tick not in seen:
            _all.append(tick)
            seen.add(tick)
TABS["full_universe"]["tickers"] = _all

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


def build_chart_series(hist_df, tickers, tf_days):
    """
    For each ticker, build normalized-to-100 price series for the given lookback.
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
        base = sub['adj_close'].iloc[0]
        if base == 0: continue
        points = [{"x": str(row['date']), "y": round(float(row['adj_close']) / base * 100, 3)}
                  for _, row in sub.iterrows()]
        result[ticker] = points
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
        return {}, {}, {}, datetime.now().strftime('%Y-%m-%d')

    all_tickers = [t for tab in TABS.values() for t in tab['tickers']]
    all_tickers = list(dict.fromkeys(all_tickers))  # dedupe preserving order

    hist = load_price_history(engine, all_tickers)
    zscores = compute_zscores(hist, all_tickers)

    # Build per-timeframe chart series
    TF_DAYS = {"ret_1w":5, "ret_1m":21, "ret_3m":63, "ret_6m":126, "ret_12m":252, "ret_ytd":0}
    chart_series = {tf: build_chart_series(hist, all_tickers, days)
                    for tf, days in TF_DAYS.items()}

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
    return instruments, chart_series, as_of


def generate_html(instruments, chart_series, as_of, tabs):
    tabs_json    = json.dumps({k: v['tickers'] for k, v in tabs.items()})
    tab_labels   = json.dumps({k: v['label']   for k, v in tabs.items()})
    tab_defaults = json.dumps({k: v.get('default_chart', v['tickers'][:10]) for k, v in tabs.items()})
    data_json    = json.dumps(instruments)
    names_json   = json.dumps(NAMES)
    chart_json   = json.dumps(chart_series)
    colors_json  = json.dumps(CHART_COLORS)
    core_json    = json.dumps(CORE_SPDRS)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Market Pulse</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
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
canvas#modalChart{{width:100%!important;height:220px!important;}}
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
  <button class="tab-btn" data-tab="full_universe">All</button>
</div>
<div id="panels"></div>

<!-- Modal -->
<div class="modal-overlay" id="modal">
  <div class="modal">
    <button class="modal-close" id="modal-close">✕</button>
    <div class="modal-ticker" id="modal-ticker"></div>
    <div class="modal-name" id="modal-name"></div>
    <div class="modal-stats" id="modal-stats"></div>
    <canvas id="modalChart"></canvas>
  </div>
</div>

<script>
const DATA       = {data_json};
const TABS       = {tabs_json};
const TAB_LABELS = {tab_labels};
const TAB_DEF    = {tab_defaults};
const NAMES      = {names_json};
const CHART_SRS  = {chart_json};
const COLORS     = {colors_json};
const CORE       = {core_json};

let currentTf  = 'ret_1m';
let currentTab = 'us_sectors';
let sortState  = {{}};
let groupChartInst  = null;
let modalChartInst  = null;
let activeChecks    = {{}};  // tabId -> Set of tickers

// ── Helpers ──────────────────────────────────────────────────────
function retColor(v, intensity=true) {{
  if (v==null) return '#333';
  const abs = Math.min(Math.abs(v)/15, 1);
  if (!intensity) return v>=0?'#2ea043':'#f85149';
  if (v>=0) {{ const r=Math.round(14+abs*32),g=Math.round(100+abs*60),b=Math.round(56+abs*16); return `rgb(${{r}},${{g}},${{b}})`; }}
  else {{ const r=Math.round(100+abs*148),g=Math.round(30+abs*51),b=Math.round(30+abs*43); return `rgb(${{r}},${{g}},${{b}})`; }}
}}
function fmt(v,d=2){{
  if(v==null) return '<span style="color:#555">—</span>';
  const s=(v>=0?'+':'')+v.toFixed(d)+'%';
  return `<span class="${{v>=0?'ret-pos':'ret-neg'}}">${{s}}</span>`;
}}
function fmtPlain(v){{
  if(v==null) return '—';
  return (v>=0?'+':'')+v.toFixed(2)+'%';
}}
function rmiHtml(sig){{
  if(sig==='bullish') return '<span class="rmi-bull">▲ BULL</span>';
  if(sig==='bearish') return '<span class="rmi-bear">▼ BEAR</span>';
  return '<span class="rmi-neut">◆ NEUT</span>';
}}
function zsHtml(v){{
  if(v==null) return '<span style="color:#555">—</span>';
  const s=(v>=0?'+':'')+v.toFixed(2);
  if(v>2)  return `<span class="z-high">${{s}} ⚡</span>`;
  if(v<-2) return `<span class="z-low">${{s}} 💧</span>`;
  return s;
}}
function getTabRows(tabId){{
  return (TABS[tabId]||[]).map(t=>DATA[t]).filter(Boolean);
}}
function ranked(rows,tf){{
  const sorted=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  const rm={{}};sorted.forEach((r,i)=>rm[r.ticker]=i+1);
  const out=[];rows.forEach(r=>{{const copy=Object.assign({{}},r);copy.rank=rm[r.ticker]??999;out.push(copy);}});return out;
}}

// ── Heatmap ──────────────────────────────────────────────────────
function buildHeatmap(rows,tf){{
  const s=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  return '<div class="heatmap">'+s.map(r=>{{
    const v=r[tf],bg=retColor(v),tc=v==null?'#888':(Math.abs(v)>4?'#fff':'#eee');
    return `<div class="heat-tile" style="background:${{bg}};color:${{tc}}" onclick="openModal('${{r.ticker}}')" title="${{r.name}}">
      <div class="ht-t">${{r.ticker}}</div>
      <div class="ht-r">${{v!=null?(v>=0?'+':'')+v.toFixed(1)+'%':'—'}}</div>
    </div>`;
  }}).join('')+'</div>';
}}

// ── Group chart ──────────────────────────────────────────────────
function getColorForTicker(ticker, tickers){{
  const i=tickers.indexOf(ticker);
  return COLORS[i%COLORS.length];
}}

function buildGroupChart(tabId,tf){{
  const tickers=TABS[tabId]||[];
  const active=[...activeChecks[tabId]];
  const series=CHART_SRS[tf]||{{}};
  const datasets=active.filter(t=>series[t]).map(t=>{{
    const color=getColorForTicker(t,tickers);
    return {{
      label:t,
      data:series[t],
      borderColor:color,
      backgroundColor:color+'22',
      borderWidth:1.8,
      pointRadius:0,
      tension:.3,
      parsing:{{xAxisKey:'x',yAxisKey:'y'}},
    }};
  }});

  const ctx=document.getElementById('groupChart');
  if(!ctx) return;
  if(groupChartInst){{groupChartInst.destroy();groupChartInst=null;}}
  groupChartInst=new Chart(ctx,{{
    type:'line',
    data:{{datasets}},
    options:{{
      responsive:true,maintainAspectRatio:false,
      animation:{{duration:200}},
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{display:false}},
        tooltip:{{
          callbacks:{{
            label:ctx=>`${{ctx.dataset.label}}: ${{ctx.parsed.y?.toFixed(1)}}`,
          }},
          backgroundColor:'#1c2128',borderColor:'#30363d',borderWidth:1,
          titleColor:'#f0f6fc',bodyColor:'#8b949e',
        }},
      }},
      scales:{{
        x:{{type:'time',time:{{unit:'month'}},ticks:{{color:'#8b949e',maxTicksLimit:8}},grid:{{color:'#21262d'}}}},
        y:{{ticks:{{color:'#8b949e',callback:v=>`${{v.toFixed(0)}}`}},grid:{{color:'#21262d'}},
            title:{{display:true,text:'Normalized (base=100)',color:'#8b949e',font:{{size:10}}}}}},
      }},
    }},
  }});
}}

function buildCbPanel(tabId){{
  const tickers=TABS[tabId]||[];
  const active=activeChecks[tabId];
  return tickers.map((t,i)=>{{
    const color=COLORS[i%COLORS.length];
    const checked=active.has(t);
    return `<label class="cb-label${{checked?' checked':''}}" data-ticker="${{t}}" data-tab="${{tabId}}">
      <input type="checkbox" ${{checked?'checked':''}}/>
      <span class="cb-dot" style="background:${{color}}"></span>
      ${{t}}
    </label>`;
  }}).join('');
}}

function buildChartSection(tabId,tf){{
  return `<div class="chart-area">
    <div class="chart-toolbar">
      <span class="chart-title-txt">Normalized Performance (base 100)</span>
      <div class="quick-btns">
        <button class="quick-btn" onclick="selectDefault('${{tabId}}')">Default</button>
        <button class="quick-btn" onclick="selectAll('${{tabId}}')">All</button>
        <button class="quick-btn" onclick="selectNone('${{tabId}}')">None</button>
      </div>
    </div>
    <canvas id="groupChart"></canvas>
    <div class="cb-panel" id="cb-panel-${{tabId}}">${{buildCbPanel(tabId)}}</div>
  </div>`;
}}

// ── Table ────────────────────────────────────────────────────────
const COLS=[
  {{id:'rank',      label:'#',         sv:r=>r.rank,      html:r=>`<b>${{r.rank}}</b>`}},
  {{id:'ticker',    label:'Ticker',    sv:r=>r.ticker,    html:r=>`<b>${{r.ticker}}</b>`}},
  {{id:'name',      label:'Name',      sv:r=>r.name,      html:r=>`<span style="color:var(--muted)">${{r.name}}</span>`}},
  {{id:'ret',       label:'Return',    sv:r=>r[currentTf]??-999, html:r=>fmt(r[currentTf])}},
  {{id:'rmi_signal',label:'RMI',       sv:r=>r.rmi_signal,html:r=>rmiHtml(r.rmi_signal)}},
  {{id:'rsi_14',    label:'RSI',       sv:r=>r.rsi_14??-1,html:r=>r.rsi_14!=null?r.rsi_14.toFixed(1):'—'}},
  {{id:'dist_ma50', label:'vs 50d',    sv:r=>r.dist_ma50??-999, html:r=>fmt(r.dist_ma50)}},
  {{id:'dist_ma200',label:'vs 200d',   sv:r=>r.dist_ma200??-999,html:r=>fmt(r.dist_ma200)}},
  {{id:'vol_20d',   label:'Vol 20d',   sv:r=>r.vol_20d??-1,    html:r=>r.vol_20d!=null?r.vol_20d.toFixed(1)+'%':'—'}},
  {{id:'zscore',    label:'Z-Score',   sv:r=>r.zscore_ret1m??-99,html:r=>zsHtml(r.zscore_ret1m)}},
];

function buildTable(rows,tabId,tf){{
  const st=sortState[tabId]||{{col:'rank',dir:1}};
  const s=[...rows].sort((a,b)=>{{
    const col=COLS.find(c=>c.id===st.col);
    if(!col) return 0;
    const va=col.sv(a),vb=col.sv(b);
    if(typeof va==='string') return st.dir*va.localeCompare(vb);
    return st.dir*(va-vb);
  }});
  const thead='<thead><tr>'+COLS.map(c=>{{
    const active=st.col===c.id;
    return `<th class="${{active?'sorted':''}}" data-col="${{c.id}}" data-tab="${{tabId}}">${{c.label}}<span class="sort-arrow">${{active?(st.dir===1?'▲':'▼'):'⇅'}}</span></th>`;
  }}).join('')+'</tr></thead>';
  const tbody='<tbody>'+s.map(r=>{{
    const cells=COLS.map(c=>`<td>${{c.html(r)}}</td>`).join('');
    return `<tr onclick="openModal('${{r.ticker}}')">${{cells}}</tr>`;
  }}).join('')+'</tbody>';
  return '<div class="tbl-wrap"><table>'+thead+tbody+'</table></div>';
}}

// ── Bars ─────────────────────────────────────────────────────────
function buildBars(rows,tf){{
  const s=[...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  const vals=s.map(r=>r[tf]).filter(v=>v!=null);
  const maxA=Math.max(...vals.map(Math.abs),0.01);
  return '<div class="bars">'+s.map(r=>{{
    const v=r[tf],pct=v!=null?Math.abs(v)/maxA*100:0,c=retColor(v,false);
    return `<div class="bar-row" onclick="openModal('${{r.ticker}}')">
      <div class="bar-label" title="${{r.name}}">${{r.ticker}}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${{pct.toFixed(1)}}%;background:${{c}}"></div>
      <span class="bar-val" style="color:${{c}}">${{fmtPlain(v)}}</span></div>
    </div>`;
  }}).join('')+'</div>';
}}

// ── Modal ────────────────────────────────────────────────────────
function openModal(ticker){{
  const d=DATA[ticker];
  if(!d) return;
  document.getElementById('modal-ticker').textContent=ticker;
  document.getElementById('modal-name').textContent=d.name;

  const stats=[
    {{label:'1M Return',val:fmtPlain(d.ret_1m),cls:d.ret_1m>=0?'ret-pos':'ret-neg'}},
    {{label:'3M Return',val:fmtPlain(d.ret_3m),cls:d.ret_3m>=0?'ret-pos':'ret-neg'}},
    {{label:'RSI (14)',val:d.rsi_14!=null?d.rsi_14.toFixed(1):'—',cls:''}},
    {{label:'RMI Signal',val:(d.rmi_signal||'neutral').toUpperCase(),
      cls:d.rmi_signal==='bullish'?'rmi-bull':d.rmi_signal==='bearish'?'rmi-bear':'rmi-neut'}},
    {{label:'vs 50d MA',val:fmtPlain(d.dist_ma50),cls:d.dist_ma50>=0?'ret-pos':'ret-neg'}},
    {{label:'Vol 20d',val:d.vol_20d!=null?d.vol_20d.toFixed(1)+'%':'—',cls:''}},
    {{label:'Z-Score',val:d.zscore_ret1m!=null?(d.zscore_ret1m>=0?'+':'')+d.zscore_ret1m.toFixed(2):'—',
      cls:d.zscore_ret1m>2?'z-high':d.zscore_ret1m<-2?'z-low':''}},
    {{label:'YTD',val:fmtPlain(d.ret_ytd),cls:d.ret_ytd>=0?'ret-pos':'ret-neg'}},
  ];
  document.getElementById('modal-stats').innerHTML=stats.map(s=>
    `<div class="stat-card"><div class="stat-label">${{s.label}}</div><div class="stat-val ${{s.cls}}">${{s.val}}</div></div>`
  ).join('');

  // Modal chart — use current tf series
  const series=CHART_SRS[currentTf]||{{}};
  const pts=series[ticker]||[];
  if(modalChartInst){{modalChartInst.destroy();modalChartInst=null;}}
  const mCtx=document.getElementById('modalChart');
  if(pts.length>1){{
    const color='#58a6ff';
    modalChartInst=new Chart(mCtx,{{
      type:'line',
      data:{{datasets:[{{
        label:ticker,data:pts,
        borderColor:color,backgroundColor:color+'22',
        borderWidth:2,pointRadius:0,tension:.3,
        parsing:{{xAxisKey:'x',yAxisKey:'y'}},
        fill:true,
      }}]}},
      options:{{
        responsive:true,maintainAspectRatio:false,animation:{{duration:150}},
        plugins:{{legend:{{display:false}},tooltip:{{
          backgroundColor:'#1c2128',borderColor:'#30363d',borderWidth:1,
          titleColor:'#f0f6fc',bodyColor:'#8b949e',
          callbacks:{{label:c=>`${{c.parsed.y?.toFixed(2)}}`}},
        }}}},
        scales:{{
          x:{{type:'time',time:{{unit:'month'}},ticks:{{color:'#8b949e',maxTicksLimit:6}},grid:{{color:'#21262d'}}}},
          y:{{ticks:{{color:'#8b949e',callback:v=>v.toFixed(0)}},grid:{{color:'#21262d'}}}},
        }},
      }},
    }});
  }}

  document.getElementById('modal').classList.add('open');
}}

function closeModal(){{
  document.getElementById('modal').classList.remove('open');
  if(modalChartInst){{modalChartInst.destroy();modalChartInst=null;}}
}}

// ── Checkbox logic ───────────────────────────────────────────────
function selectDefault(tabId){{
  activeChecks[tabId]=new Set(TAB_DEF[tabId]||[]);
  refreshCbPanel(tabId);buildGroupChart(tabId,currentTf);
}}
function selectAll(tabId){{
  activeChecks[tabId]=new Set(TABS[tabId]||[]);
  refreshCbPanel(tabId);buildGroupChart(tabId,currentTf);
}}
function selectNone(tabId){{
  activeChecks[tabId]=new Set();
  refreshCbPanel(tabId);buildGroupChart(tabId,currentTf);
}}
function refreshCbPanel(tabId){{
  const panel=document.getElementById('cb-panel-'+tabId);
  if(panel) panel.innerHTML=buildCbPanel(tabId);
  attachCbListeners(tabId);
}}
function attachCbListeners(tabId){{
  const panel=document.getElementById('cb-panel-'+tabId);
  if(!panel) return;
  panel.querySelectorAll('.cb-label').forEach(lbl=>{{
    lbl.addEventListener('click',()=>{{
      const t=lbl.dataset.ticker;
      if(activeChecks[tabId].has(t)) activeChecks[tabId].delete(t);
      else activeChecks[tabId].add(t);
      lbl.classList.toggle('checked',activeChecks[tabId].has(t));
      buildGroupChart(tabId,currentTf);
    }});
  }});
}}

// ── Render tab ───────────────────────────────────────────────────
function renderTab(tabId,tf){{
  const rows=ranked(getTabRows(tabId),tf);
  const panel=document.getElementById('panel-'+tabId);
  if(!panel) return;

  panel.innerHTML=
    '<div class="section-hdr">Heatmap</div>'+
    buildHeatmap(rows,tf)+
    '<div class="section-hdr">Performance Chart</div>'+
    buildChartSection(tabId,tf)+
    '<div class="section-hdr">Rankings</div>'+
    buildTable(rows,tabId,tf)+
    '<div class="section-hdr" style="margin-top:16px">Relative Strength</div>'+
    buildBars(rows,tf);

  // Sort listeners
  panel.querySelectorAll('th[data-col]').forEach(th=>{{
    th.addEventListener('click',()=>{{
      const col=th.dataset.col,tab=th.dataset.tab;
      const st=sortState[tab]||{{col:'rank',dir:1}};
      sortState[tab]=col===st.col?{{col,dir:-st.dir}}:{{col,dir:1}};
      const rows2=ranked(getTabRows(tab),currentTf);
      const tbl=panel.querySelector('.tbl-wrap');
      if(tbl) tbl.outerHTML=buildTable(rows2,tab,currentTf);
      // re-attach after rerender
      panel.querySelectorAll('th[data-col]').forEach(th2=>{{
        th2.addEventListener('click',()=>{{
          const c=th2.dataset.col,t=th2.dataset.tab;
          const s=sortState[t]||{{col:'rank',dir:1}};
          sortState[t]=c===s.col?{{col:c,dir:-s.dir}}:{{col:c,dir:1}};
          renderTab(t,currentTf);
        }});
      }});
    }});
  }});

  attachCbListeners(tabId);
  buildGroupChart(tabId,tf);
}}

// ── Init ─────────────────────────────────────────────────────────
function init(){{
  // Init activeChecks from defaults
  Object.keys(TABS).forEach(tabId=>{{
    activeChecks[tabId]=new Set(TAB_DEF[tabId]||[]);
  }});

  // Build panels
  const panels=document.getElementById('panels');
  Object.keys(TABS).forEach(tabId=>{{
    const div=document.createElement('div');
    div.className='tab-panel'+(tabId===currentTab?' active':'');
    div.id='panel-'+tabId;
    panels.appendChild(div);
  }});

  // TF buttons
  document.getElementById('tf-bar').addEventListener('click',e=>{{
    const btn=e.target.closest('.tf-btn');
    if(!btn) return;
    currentTf=btn.dataset.tf;
    document.querySelectorAll('.tf-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    renderTab(currentTab,currentTf);
  }});

  // Tab buttons
  document.getElementById('tab-nav').addEventListener('click',e=>{{
    const btn=e.target.closest('.tab-btn');
    if(!btn) return;
    currentTab=btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    const panel=document.getElementById('panel-'+currentTab);
    panel.classList.add('active');
    if(!panel.querySelector('canvas')) renderTab(currentTab,currentTf);
    else buildGroupChart(currentTab,currentTf);
  }});

  // Modal close
  document.getElementById('modal-close').addEventListener('click',closeModal);
  document.getElementById('modal').addEventListener('click',e=>{{
    if(e.target===document.getElementById('modal')) closeModal();
  }});

  renderTab(currentTab,currentTf);
}}

// Chart.js date adapter via CDN
const s=document.createElement('script');
s.src='https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js';
s.onload=()=>{{ document.addEventListener('DOMContentLoaded',init); if(document.readyState!=='loading') init(); }};
document.head.appendChild(s);
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
    instruments, chart_series, as_of = build_data(engine)
    print(f"  {len(instruments)} instruments, as of {as_of}")
    print("Generating HTML...")
    html = generate_html(instruments, chart_series, as_of, TABS)
    out = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        f.write(html)
    print(f"  Written: {out} ({len(html)//1024}KB)")

if __name__ == '__main__':
    main()
