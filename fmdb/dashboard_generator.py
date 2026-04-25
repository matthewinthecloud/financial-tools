#!/usr/bin/env python3
"""
Market Pulse Dashboard Generator
Queries fmdb Postgres, computes z-scores, outputs self-contained HTML.
Usage: python dashboard_generator.py [--output path/to/index.html]
"""
import sys
import os
import json
import argparse
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, select, text

from config import DB_URL
from db.schema import analytics_prices, analytics_economics, raw_prices

# ─────────────────────────────────────────────
# Tab definitions
# ─────────────────────────────────────────────

TABS = {
    "us_sectors": {
        "label": "US Sectors",
        "tickers": ["XLY","XRT","XLU","XLI","IYT","ITB","XLRE","XTL","XBI","SMH",
                    "IGV","JETS","XHB","XLP","XLV","PPH","ITA","XLF","KRE"],
    },
    "country_etfs": {
        "label": "Country ETFs",
        "tickers": ["EWZ","EWW","ECH","COLO","ARGT","FXI","EWH","KWEB","EWJ","EWT",
                    "EWY","KDEF","EWS","EWM","THD","EIDO","VNM","EWA","INDA","SMIN",
                    "TUR","KSA","UAE","EIS","CEE","FEZ","EWG","EWQ","EWP","EWO",
                    "EWD","EWN","EWL","EPOL","EUAD","VEA","VWO","IEMG","EMXC"],
    },
    "thematic": {
        "label": "Thematic / Other",
        "tickers": ["GLD","SLV","PPLT","PALL","SIL","SILJ","GDX","GDXJ","NIKL","CPER",
                    "XME","PICK","COPX","COPJ","TAN","REMX","SRUUF","URA","XLE","XOP",
                    "OIH","DBA","CMDY","IBIT","ETHA","BSOL","BLOK","WGMI","HYG","JNK",
                    "TLT","LQD","TIP","EMB","UVXY","VXX","VNQ","VNQI"],
    },
    "full_universe": {
        "label": "Full Universe",
        "tickers": [],  # populated dynamically
    },
}

# Friendly names
NAMES = {
    "XLY":"Consumer Disc","XRT":"Retail","XLU":"Utilities","XLI":"Industrials",
    "IYT":"Transportation","ITB":"Homebuilders","XLRE":"Real Estate","XTL":"Telecom",
    "XBI":"Biotech","SMH":"Semiconductors","IGV":"Software","JETS":"Airlines",
    "XHB":"Homebuilders ETF","XLP":"Consumer Staples","XLV":"Health Care",
    "PPH":"Pharma","ITA":"Aerospace/Defense","XLF":"Financials","KRE":"Regional Banks",
    "EWZ":"Brazil","EWW":"Mexico","ECH":"Chile","COLO":"Colombia","ARGT":"Argentina",
    "FXI":"China Large Cap","EWH":"Hong Kong","KWEB":"China Internet","EWJ":"Japan",
    "EWT":"Taiwan","EWY":"South Korea","KDEF":"Korea Defense","EWS":"Singapore",
    "EWM":"Malaysia","THD":"Thailand","EIDO":"Indonesia","VNM":"Vietnam",
    "EWA":"Australia","INDA":"India","SMIN":"India Small Cap","TUR":"Turkey",
    "KSA":"Saudi Arabia","UAE":"UAE","EIS":"Israel","CEE":"Central & Eastern Europe",
    "FEZ":"Euro Stoxx 50","EWG":"Germany","EWQ":"France","EWP":"Spain","EWO":"Austria",
    "EWD":"Sweden","EWN":"Netherlands","EWL":"Switzerland","EPOL":"Poland",
    "EUAD":"Europe Aero/Defense","VEA":"Developed Markets","VWO":"Emerging Markets",
    "IEMG":"Core EM","EMXC":"EM ex-China",
    "GLD":"Gold","SLV":"Silver","PPLT":"Platinum","PALL":"Palladium",
    "SIL":"Silver Miners","SILJ":"Jr Silver Miners","GDX":"Gold Miners",
    "GDXJ":"Jr Gold Miners","NIKL":"Nickel","CPER":"Copper","XME":"Metals & Mining",
    "PICK":"Global Mining","COPX":"Copper Miners","COPJ":"Jr Copper Miners",
    "TAN":"Solar","REMX":"Rare Earth","SRUUF":"Skyharbour Res","URA":"Uranium",
    "XLE":"Energy","XOP":"Oil & Gas E&P","OIH":"Oil Services","DBA":"Agriculture",
    "CMDY":"Commodities","IBIT":"Bitcoin ETF","ETHA":"Ethereum ETF","BSOL":"BTC+ETH ETF",
    "BLOK":"Blockchain","WGMI":"Bitcoin Miners","HYG":"High Yield Corp",
    "JNK":"High Yield Bond","TLT":"20yr Treasury","LQD":"IG Corp Bond",
    "TIP":"TIPS","EMB":"EM Bond","UVXY":"Ultra VIX","VXX":"VIX Futures",
    "VNQ":"US REIT","VNQI":"Global REIT",
}


def load_data(engine):
    """Load analytics_prices for all ETF instruments."""
    query = """
        SELECT
            ap.instrument_id,
            ap.date,
            ap.ret_1d, ap.ret_1w, ap.ret_1m, ap.ret_3m, ap.ret_6m, ap.ret_12m, ap.ret_ytd,
            ap.vol_20d, ap.rsi_14, ap.mfi_14,
            ap.dist_ma50, ap.dist_ma200,
            ap.rmi_signal, ap.rmi_value,
            ap.golden_cross,
            ap.macd_hist,
            ap.atr_14,
            ap.bb_width
        FROM analytics_prices ap
        WHERE ap.date = (
            SELECT MAX(date) FROM analytics_prices ap2
            WHERE ap2.instrument_id = ap.instrument_id
        )
        ORDER BY ap.instrument_id
    """
    df = pd.read_sql(text(query), engine)
    return df


def load_price_history(engine, tickers, lookback_days=365):
    """Load raw price history for z-score computation."""
    tickers_str = "','".join(tickers)
    query = f"""
        SELECT instrument_id, date, adj_close
        FROM raw_prices
        WHERE instrument_id IN ('{tickers_str}')
          AND date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        ORDER BY instrument_id, date
    """
    return pd.read_sql(text(query), engine)


def compute_zscores(df, hist_df):
    """
    Compute z-scores for ret_1w and ret_1m vs 1-year rolling history.
    Returns dict: {instrument_id: {zscore_ret1w, zscore_ret1m, zscore_price}}
    """
    zscores = {}
    for iid, group in hist_df.groupby('instrument_id'):
        group = group.sort_values('date')
        prices = group['adj_close']
        if len(prices) < 20:
            continue
        # Weekly returns
        ret_1w_series = prices.pct_change(5) * 100
        ret_1m_series = prices.pct_change(21) * 100
        # Price z-score (current vs 1y)
        price_mean = prices.mean()
        price_std  = prices.std()
        current_price = prices.iloc[-1]

        def zscore_latest(series):
            s = series.dropna()
            if len(s) < 10 or s.std() == 0:
                return None
            return float((s.iloc[-1] - s.mean()) / s.std())

        zscores[iid] = {
            'zscore_ret1w': zscore_latest(ret_1w_series),
            'zscore_ret1m': zscore_latest(ret_1m_series),
            'zscore_price': float((current_price - price_mean) / price_std) if price_std > 0 else None,
        }
    return zscores


def build_instrument_data(engine):
    """Build complete instrument data dict for dashboard."""
    df = load_data(engine)
    if df.empty:
        print("WARNING: No analytics data found. Run analytics first.")
        return {}, datetime.now().isoformat()

    all_tickers = df['instrument_id'].tolist()
    hist_df = load_price_history(engine, all_tickers)
    zscores = compute_zscores(df, hist_df)

    # Populate full universe tab
    etf_tickers = (
        TABS['us_sectors']['tickers'] +
        TABS['country_etfs']['tickers'] +
        TABS['thematic']['tickers']
    )
    TABS['full_universe']['tickers'] = etf_tickers

    result = {}
    for _, row in df.iterrows():
        iid = row['instrument_id']
        zs  = zscores.get(iid, {})
        result[iid] = {
            'ticker':     iid,
            'name':       NAMES.get(iid, iid),
            'ret_1w':     _f(row['ret_1w']),
            'ret_1m':     _f(row['ret_1m']),
            'ret_3m':     _f(row['ret_3m']),
            'ret_6m':     _f(row['ret_6m']),
            'ret_12m':    _f(row['ret_12m']),
            'ret_ytd':    _f(row['ret_ytd']),
            'vol_20d':    _f(row['vol_20d']),
            'rsi_14':     _f(row['rsi_14']),
            'dist_ma50':  _f(row['dist_ma50']),
            'dist_ma200': _f(row['dist_ma200']),
            'rmi_signal': row['rmi_signal'] or 'neutral',
            'zscore_ret1w':  _f(zs.get('zscore_ret1w')),
            'zscore_ret1m':  _f(zs.get('zscore_ret1m')),
            'zscore_price':  _f(zs.get('zscore_price')),
            'golden_cross': int(row['golden_cross']) if row['golden_cross'] is not None else 0,
        }

    as_of = df['date'].max()
    as_of_str = str(as_of) if as_of else datetime.now().strftime('%Y-%m-%d')
    return result, as_of_str


def _f(v):
    if v is None:
        return None
    try:
        import math
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 4)
    except Exception:
        return None


def generate_html(data, as_of, tabs):
    tabs_json   = json.dumps({k: v['tickers'] for k, v in tabs.items()})
    tab_labels  = json.dumps({k: v['label'] for k, v in tabs.items()})
    data_json   = json.dumps(data)
    names_json  = json.dumps(NAMES)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Market Pulse</title>
<style>
:root{{
  --bg:#0d1117;--card:#161b22;--border:#30363d;--text:#f0f6fc;--muted:#8b949e;
  --green:#2ea043;--green-bg:#0d2818;--red:#f85149;--red-bg:#2d1117;
  --yellow:#e3b341;--yellow-bg:#2d2000;--orange:#f0883e;--blue:#58a6ff;
  --tab-active:#58a6ff;
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;}}
/* Header */
.header{{padding:16px 16px 0;}}
.header h1{{font-size:22px;font-weight:700;letter-spacing:-.5px;}}
.header .subtitle{{color:var(--muted);font-size:12px;margin-top:2px;}}
/* Timeframe selector */
.tf-bar{{display:flex;gap:6px;padding:12px 16px;overflow-x:auto;-webkit-overflow-scrolling:touch;}}
.tf-btn{{background:var(--card);border:1px solid var(--border);color:var(--muted);
  padding:7px 14px;border-radius:20px;cursor:pointer;font-size:13px;white-space:nowrap;
  transition:all .15s;-webkit-tap-highlight-color:transparent;}}
.tf-btn.active{{background:var(--tab-active);border-color:var(--tab-active);color:#fff;font-weight:600;}}
/* Tab nav */
.tab-nav{{display:flex;gap:0;padding:0 16px;border-bottom:1px solid var(--border);overflow-x:auto;-webkit-overflow-scrolling:touch;}}
.tab-btn{{background:none;border:none;color:var(--muted);padding:10px 14px;cursor:pointer;
  font-size:13px;white-space:nowrap;border-bottom:2px solid transparent;
  -webkit-tap-highlight-color:transparent;transition:color .15s;}}
.tab-btn.active{{color:var(--tab-active);border-bottom-color:var(--tab-active);font-weight:600;}}
/* Content */
.tab-panel{{display:none;padding:16px;}}
.tab-panel.active{{display:block;}}
/* Heatmap */
.heatmap{{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:20px;}}
.heat-tile{{border-radius:8px;padding:8px 10px;min-width:72px;flex:1 1 72px;max-width:100px;
  text-align:center;cursor:default;transition:transform .1s;}}
.heat-tile:hover{{transform:scale(1.05);}}
.heat-tile .ht-ticker{{font-size:12px;font-weight:700;}}
.heat-tile .ht-ret{{font-size:11px;margin-top:2px;}}
/* Table */
.tbl-wrap{{overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:20px;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th{{background:var(--card);color:var(--muted);padding:8px 10px;text-align:left;
   white-space:nowrap;cursor:pointer;user-select:none;position:sticky;top:0;z-index:1;
   border-bottom:1px solid var(--border);}}
th:hover{{color:var(--text);}}
th .sort-arrow{{font-size:10px;margin-left:3px;opacity:.5;}}
th.sorted .sort-arrow{{opacity:1;}}
td{{padding:8px 10px;border-bottom:1px solid var(--border);white-space:nowrap;}}
tr:hover td{{background:rgba(255,255,255,.03);}}
.rmi-bull{{background:var(--green-bg)!important;color:var(--green);font-weight:600;}}
.rmi-bear{{background:var(--red-bg)!important;color:var(--red);font-weight:600;}}
.rmi-neut{{background:var(--yellow-bg)!important;color:var(--yellow);font-weight:600;}}
.ret-pos{{color:var(--green);}}
.ret-neg{{color:var(--red);}}
.z-high{{color:var(--orange);font-weight:600;}}
.z-low{{color:var(--blue);font-weight:600;}}
/* Bar chart */
.chart-title{{font-size:13px;font-weight:600;color:var(--muted);margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px;}}
.bars{{display:flex;flex-direction:column;gap:5px;}}
.bar-row{{display:flex;align-items:center;gap:8px;}}
.bar-label{{width:64px;font-size:11px;color:var(--muted);text-align:right;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;}}
.bar-track{{flex:1;background:var(--card);border-radius:3px;height:18px;overflow:hidden;position:relative;}}
.bar-fill{{height:100%;border-radius:3px;min-width:2px;transition:width .3s;}}
.bar-val{{position:absolute;right:6px;top:50%;transform:translateY(-50%);font-size:11px;font-weight:600;}}
/* Responsive */
@media(max-width:480px){{
  .header h1{{font-size:18px;}}
  th,td{{padding:6px 8px;font-size:12px;}}
  .heat-tile{{min-width:60px;padding:6px 8px;}}
  .heat-tile .ht-ticker{{font-size:11px;}}
  .heat-tile .ht-ret{{font-size:10px;}}
}}
</style>
</head>
<body>
<div class="header">
  <h1>📊 Market Pulse</h1>
  <div class="subtitle">As of {as_of} · Data from fmdb</div>
</div>

<!-- Timeframe selector -->
<div class="tf-bar" id="tf-bar">
  <button class="tf-btn" data-tf="ret_1w">1W</button>
  <button class="tf-btn active" data-tf="ret_1m">1M</button>
  <button class="tf-btn" data-tf="ret_3m">3M</button>
  <button class="tf-btn" data-tf="ret_6m">6M</button>
  <button class="tf-btn" data-tf="ret_12m">12M</button>
  <button class="tf-btn" data-tf="ret_ytd">YTD</button>
</div>

<!-- Tab nav -->
<div class="tab-nav" id="tab-nav">
  <button class="tab-btn active" data-tab="us_sectors">US Sectors</button>
  <button class="tab-btn" data-tab="country_etfs">Country ETFs</button>
  <button class="tab-btn" data-tab="thematic">Thematic</button>
  <button class="tab-btn" data-tab="full_universe">All</button>
</div>

<!-- Tab panels -->
<div id="panels"></div>

<script>
const DATA = {data_json};
const TABS = {tabs_json};
const TAB_LABELS = {tab_labels};
const NAMES = {names_json};

let currentTf = 'ret_1m';
let currentTab = 'us_sectors';
let sortState = {{}};  // tabId -> {{col, dir}}

// ── Colour helpers ───────────────────────────────────────────────
function retColor(v, intensity=true) {{
  if (v == null) return '#444';
  const abs = Math.min(Math.abs(v) / 15, 1);
  if (!intensity) return v >= 0 ? '#2ea043' : '#f85149';
  if (v >= 0) {{
    const r = Math.round(14 + abs*(46-14)), g = Math.round(100+abs*(160-100)), b = Math.round(56+abs*(72-56));
    return `rgb(${{r}},${{g}},${{b}})`;
  }} else {{
    const r = Math.round(100+abs*(248-100)), g = Math.round(30+abs*(81-30)), b = Math.round(30+abs*(73-30));
    return `rgb(${{r}},${{g}},${{b}})`;
  }}
}}
function fmt(v, digits=2, suffix='%') {{
  if (v == null) return '<span style="color:#555">—</span>';
  const s = (v >= 0 ? '+' : '') + v.toFixed(digits) + suffix;
  const cls = v >= 0 ? 'ret-pos' : 'ret-neg';
  return `<span class="${{cls}}">${{s}}</span>`;
}}
function fmtPlain(v, digits=2, suffix='%') {{
  if (v == null) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(digits) + suffix;
}}
function rmiCell(sig) {{
  if (!sig || sig === 'neutral') return '<span class="rmi-neut">◆ NEUT</span>';
  if (sig === 'bullish') return '<span class="rmi-bull">▲ BULL</span>';
  return '<span class="rmi-bear">▼ BEAR</span>';
}}
function rmiRowClass(sig) {{
  if (sig === 'bullish') return 'rmi-bull';
  if (sig === 'bearish') return 'rmi-bear';
  return '';
}}
function zsCell(v) {{
  if (v == null) return '<span style="color:#555">—</span>';
  const s = (v>=0?'+':'')+v.toFixed(2);
  if (v > 2)  return `<span class="z-high">${{s}} ⚡</span>`;
  if (v < -2) return `<span class="z-low">${{s}} 💧</span>`;
  return `<span>${{s}}</span>`;
}}

// ── Data helpers ─────────────────────────────────────────────────
function getTabData(tabId) {{
  const tickers = TABS[tabId] || [];
  return tickers.map(t => DATA[t]).filter(Boolean);
}}

function ranked(rows, tf) {{
  const sorted = [...rows].sort((a,b) => {{
    const va = a[tf] ?? -999, vb = b[tf] ?? -999;
    return vb - va;
  }});
  const rankMap = {{}};
  sorted.forEach((r,i) => rankMap[r.ticker] = i+1);
  return rows.map(r => ({{...r, rank: rankMap[r.ticker] ?? '—'}}));
}}

// ── Heatmap ──────────────────────────────────────────────────────
function buildHeatmap(rows, tf) {{
  const sorted = [...rows].sort((a,b) => (b[tf]??-999)-(a[tf]??-999));
  return '<div class="heatmap">' + sorted.map(r => {{
    const v = r[tf];
    const bg = retColor(v);
    const textCol = v == null ? '#888' : (Math.abs(v)>5 ? '#fff' : '#ddd');
    return `<div class="heat-tile" style="background:${{bg}};color:${{textCol}}">
      <div class="ht-ticker">${{r.ticker}}</div>
      <div class="ht-ret">${{v != null ? (v>=0?'+':'')+v.toFixed(1)+'%' : '—'}}</div>
    </div>`;
  }}).join('') + '</div>';
}}

// ── Table ────────────────────────────────────────────────────────
const COLS = [
  {{id:'rank',      label:'Rank',      sort:true,  html:r=>`<b>${{r.rank}}</b>`}},
  {{id:'ticker',    label:'Ticker',    sort:true,  html:r=>`<b>${{r.ticker}}</b>`}},
  {{id:'name',      label:'Name',      sort:true,  html:r=>r.name}},
  {{id:'ret',       label:'Return',    sort:true,  html:r=>fmt(r[currentTf])}},
  {{id:'rmi',       label:'RMI',       sort:true,  html:r=>rmiCell(r.rmi_signal)}},
  {{id:'rsi_14',    label:'RSI',       sort:true,  html:r=>r.rsi_14!=null?r.rsi_14.toFixed(1):'—'}},
  {{id:'dist_ma50', label:'vs 50d MA', sort:true,  html:r=>fmt(r.dist_ma50)}},
  {{id:'dist_ma200',label:'vs 200d MA',sort:true,  html:r=>fmt(r.dist_ma200)}},
  {{id:'vol_20d',   label:'Vol 20d',   sort:true,  html:r=>r.vol_20d!=null?r.vol_20d.toFixed(1)+'%':'—'}},
  {{id:'zscore',    label:'Z-Score',   sort:true,  html:r=>zsCell(r.zscore_ret1m)}},
];

function buildTable(rows, tabId, tf) {{
  const st = sortState[tabId] || {{col:'rank', dir:1}};
  const sorted = [...rows].sort((a,b) => {{
    let va = st.col === 'ret' ? (a[tf]??-999) :
             st.col === 'rank' ? (a.rank||999) :
             st.col === 'zscore' ? (a.zscore_ret1m??-999) :
             (a[st.col]??-999);
    let vb = st.col === 'ret' ? (b[tf]??-999) :
             st.col === 'rank' ? (b.rank||999) :
             st.col === 'zscore' ? (b.zscore_ret1m??-999) :
             (b[st.col]??-999);
    if (typeof va === 'string') return st.dir * va.localeCompare(vb);
    return st.dir * (va - vb);
  }});

  const thead = '<thead><tr>' + COLS.map(c => {{
    const isSorted = st.col === c.id;
    const arrow = isSorted ? (st.dir===1?'▲':'▼') : '⇅';
    return `<th class="${{isSorted?'sorted':''}}" data-col="${{c.id}}" data-tab="${{tabId}}">${{c.label}} <span class="sort-arrow">${{arrow}}</span></th>`;
  }}).join('') + '</tr></thead>';

  const tbody = '<tbody>' + sorted.map(r => {{
    const rowCls = rmiRowClass(r.rmi_signal);
    const cells = COLS.map(c => `<td class="${{rowCls}}">${{c.html(r)}}</td>`).join('');
    return `<tr>${{cells}}</tr>`;
  }}).join('') + '</tbody>';

  return '<div class="tbl-wrap"><table>' + thead + tbody + '</table></div>';
}}

// ── Bar chart ────────────────────────────────────────────────────
function buildBars(rows, tf) {{
  const sorted = [...rows].sort((a,b)=>(b[tf]??-999)-(a[tf]??-999));
  const vals = sorted.map(r=>r[tf]).filter(v=>v!=null);
  const maxAbs = Math.max(...vals.map(Math.abs), 0.01);

  const bars = sorted.map(r => {{
    const v = r[tf];
    const pct = v != null ? Math.abs(v)/maxAbs*100 : 0;
    const color = retColor(v, false);
    return `<div class="bar-row">
      <div class="bar-label" title="${{r.name}}">${{r.ticker}}</div>
      <div class="bar-track">
        <div class="bar-fill" style="width:${{pct.toFixed(1)}}%;background:${{color}}"></div>
        <span class="bar-val" style="color:${{color}}">${{fmtPlain(v)}}</span>
      </div>
    </div>`;
  }}).join('');

  return `<div class="chart-title">Relative Strength Ranking</div><div class="bars">${{bars}}</div>`;
}}

// ── Render ───────────────────────────────────────────────────────
function renderTab(tabId, tf) {{
  const rows = ranked(getTabData(tabId), tf);
  const panel = document.getElementById('panel-'+tabId);
  if (!panel) return;
  panel.innerHTML =
    buildHeatmap(rows, tf) +
    buildTable(rows, tabId, tf) +
    buildBars(rows, tf);

  // Re-attach sort listeners
  panel.querySelectorAll('th[data-col]').forEach(th => {{
    th.addEventListener('click', () => {{
      const col = th.dataset.col, tab = th.dataset.tab;
      const st = sortState[tab] || {{col:'rank', dir:1}};
      sortState[tab] = col === st.col ? {{col, dir: -st.dir}} : {{col, dir: 1}};
      renderTab(tab, currentTf);
    }});
  }});
}}

function renderAll() {{
  renderTab(currentTab, currentTf);
}}

// ── Init ─────────────────────────────────────────────────────────
function init() {{
  const panels = document.getElementById('panels');
  Object.keys(TABS).forEach(tabId => {{
    const div = document.createElement('div');
    div.className = 'tab-panel' + (tabId===currentTab?' active':'');
    div.id = 'panel-'+tabId;
    panels.appendChild(div);
  }});

  // TF buttons
  document.getElementById('tf-bar').addEventListener('click', e => {{
    const btn = e.target.closest('.tf-btn');
    if (!btn) return;
    currentTf = btn.dataset.tf;
    document.querySelectorAll('.tf-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    renderAll();
  }});

  // Tab buttons
  document.getElementById('tab-nav').addEventListener('click', e => {{
    const btn = e.target.closest('.tab-btn');
    if (!btn) return;
    currentTab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    const panel = document.getElementById('panel-'+currentTab);
    if (panel) panel.classList.add('active');
    if (!panel.innerHTML) renderTab(currentTab, currentTf);
  }});

  renderAll();
}}

document.addEventListener('DOMContentLoaded', init);
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

    print("Loading analytics data...")
    data, as_of = build_instrument_data(engine)
    print(f"  Loaded {len(data)} instruments (as of {as_of})")

    print("Generating HTML...")
    html = generate_html(data, as_of, TABS)

    out_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        f.write(html)
    print(f"  Written to {out_path} ({len(html)//1024}KB)")


if __name__ == '__main__':
    main()
