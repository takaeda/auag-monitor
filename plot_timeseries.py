#!/usr/bin/env python3
"""
plot_timeseries.py - 蓄積データからインタラクティブな時系列チャートHTMLを生成
================================================================================

Usage:
    python3 plot_timeseries.py                              # 全期間・全シンボル
    python3 plot_timeseries.py --start 2026-01-29 --end 2026-02-03
    python3 plot_timeseries.py --resample 5min              # 5分足にリサンプル
    python3 plot_timeseries.py --db /path/to/metals.db -o chart.html

生成された HTML ファイルをブラウザ（iPad Safari 等）で開くと、
インタラクティブなチャートが表示される。
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).parent / "precious_metals.db"

# Y軸スケール設定 (min, max)
Y_SCALE_GOLD = (4400, 5600)
Y_SCALE_SILVER = (70, 120)
Y_SCALE_RATIO = (40, 80)


def load_data(db_path, symbol, start=None, end=None):
    """SQLite から OHLCV データを読み込む"""
    conn = sqlite3.connect(str(db_path))
    query = "SELECT ts, open, high, low, close, volume FROM ohlcv_1m WHERE symbol = ?"
    params = [symbol]

    if start:
        query += " AND ts >= ?"
        params.append(start + "T00:00:00+00:00")
    if end:
        query += " AND ts <= ?"
        params.append(end + "T23:59:59+00:00")

    query += " ORDER BY ts"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def resample_data(rows, interval_minutes):
    """指定分数でリサンプル（OHLCV 集約）"""
    if not rows or interval_minutes <= 1:
        return rows

    resampled = []
    bucket = []
    bucket_start = None

    for ts_str, o, h, l, c, v in rows:
        ts = datetime.fromisoformat(ts_str.replace("+00:00", "+00:00"))
        bucket_key = ts.replace(
            minute=(ts.minute // interval_minutes) * interval_minutes,
            second=0, microsecond=0
        )

        if bucket_start is None:
            bucket_start = bucket_key

        if bucket_key != bucket_start and bucket:
            # バケットを集約
            resampled.append((
                bucket_start.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                bucket[0][1],                              # open
                max(r[2] for r in bucket if r[2]),         # high
                min(r[3] for r in bucket if r[3]),         # low
                bucket[-1][4],                             # close
                sum(r[5] for r in bucket if r[5]),         # volume
            ))
            bucket = []
            bucket_start = bucket_key

        bucket.append((ts_str, o, h, l, c, v or 0))

    # 最後のバケット
    if bucket:
        resampled.append((
            bucket_start.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            bucket[0][1],
            max(r[2] for r in bucket if r[2]),
            min(r[3] for r in bucket if r[3]),
            bucket[-1][4],
            sum(r[5] for r in bucket if r[5]),
        ))

    return resampled


def generate_html(gold_data, silver_data, title, resample_label, y_scales=None):
    """インタラクティブな HTML チャートを生成

    y_scales: dict with keys 'gold', 'silver', 'ratio', each containing (min, max)
    """
    if y_scales is None:
        y_scales = {
            'gold': Y_SCALE_GOLD,
            'silver': Y_SCALE_SILVER,
            'ratio': Y_SCALE_RATIO,
        }

    def to_json_arrays(rows):
        ts_list = []
        for r in rows:
            # DB 格納値は UTC — JST (+9h) の ISO 形式で返す（Chart.js time scale 用）
            raw = r[0][:19]  # "2026-01-27T10:00:00"
            try:
                dt = datetime.fromisoformat(raw)
                dt_jst = dt + __import__('datetime').timedelta(hours=9)
                ts_list.append(dt_jst.strftime("%Y-%m-%dT%H:%M:%S"))
            except Exception:
                ts_list.append(raw)
        opn   = [round(r[1], 2) if r[1] else None for r in rows]
        high  = [round(r[2], 2) if r[2] else None for r in rows]
        low   = [round(r[3], 2) if r[3] else None for r in rows]
        close = [round(r[4], 2) if r[4] else None for r in rows]
        volume = [r[5] if r[5] else 0 for r in rows]
        return ts_list, opn, high, low, close, volume

    g_ts, g_open, g_high, g_low, g_close, g_vol = to_json_arrays(gold_data) if gold_data else ([], [], [], [], [], [])
    s_ts, s_open, s_high, s_low, s_close, s_vol = to_json_arrays(silver_data) if silver_data else ([], [], [], [], [], [])

    # 金銀比率の計算（タイムスタンプが一致する点のみ）
    gold_map = dict(zip(g_ts, g_close))
    ratio_ts, ratio_vals = [], []
    for t, sc in zip(s_ts, s_close):
        if t in gold_map and gold_map[t] and sc and sc > 0:
            ratio_ts.append(t)
            ratio_vals.append(round(gold_map[t] / sc, 2))

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Noto+Sans+JP:wght@300;400;700&display=swap');

  :root {{
    --bg-primary: #0a0e17;
    --bg-card: #111827;
    --bg-card-hover: #1a2332;
    --border: #1e293b;
    --text-primary: #e2e8f0;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --gold: #f59e0b;
    --gold-dim: rgba(245, 158, 11, 0.15);
    --silver: #a8b4c4;
    --silver-dim: rgba(168, 180, 196, 0.15);
    --ratio: #06b6d4;
    --ratio-dim: rgba(6, 182, 212, 0.15);
    --vol-gold: rgba(245, 158, 11, 0.35);
    --vol-silver: rgba(168, 180, 196, 0.35);
    --accent: #3b82f6;
    --danger: #ef4444;
    --success: #22c55e;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'Noto Sans JP', 'JetBrains Mono', sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    min-height: 100vh;
    overflow-x: hidden;
  }}

  .header {{
    padding: 24px 32px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: baseline;
    gap: 16px;
    flex-wrap: wrap;
  }}

  .header h1 {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 18px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.5px;
  }}

  .header .meta {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: var(--text-muted);
  }}

  .stats-row {{
    display: flex;
    gap: 12px;
    padding: 16px 32px;
    overflow-x: auto;
    border-bottom: 1px solid var(--border);
  }}

  .stat-card {{
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
    min-width: 140px;
    flex-shrink: 0;
  }}

  .stat-separator {{
    width: 1px;
    height: 50px;
    background: var(--border);
    margin: 0 12px;
    flex-shrink: 0;
    align-self: center;
  }}

  .stat-card .label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--text-muted);
    margin-bottom: 4px;
  }}

  .stat-card .value {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 18px;
    font-weight: 700;
  }}

  .stat-card .change {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    margin-top: 2px;
  }}

  .change.up {{ color: var(--success); }}
  .change.down {{ color: var(--danger); }}

  .chart-container {{
    padding: 20px 32px;
  }}

  .chart-box {{
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 16px;
    position: relative;
  }}

  .chart-box .chart-title {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    font-weight: 500;
    color: var(--text-secondary);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  .chart-box .chart-title .dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
  }}

  .chart-box .chart-wrap {{
    position: relative;
    width: 100%;
  }}

  .chart-box .chart-wrap.tall {{ height: 220px; }}
  .chart-box .chart-wrap.short {{ height: 160px; }}

  .chart-box canvas {{
    position: absolute;
    top: 0;
    left: 0;
    width: 100% !important;
    height: 100% !important;
  }}

  .controls {{
    display: flex;
    gap: 8px;
    padding: 8px 32px 0;
    flex-wrap: wrap;
    align-items: center;
  }}

  .controls button {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    padding: 6px 14px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg-card);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
  }}

  .controls button:hover {{
    background: var(--bg-card-hover);
    color: var(--text-primary);
  }}

  .controls button.active {{
    background: var(--accent);
    border-color: var(--accent);
    color: #fff;
  }}

  .controls button:disabled {{
    opacity: 0.3;
    cursor: not-allowed;
  }}

  .controls .sep {{
    width: 1px;
    height: 24px;
    background: var(--border);
    margin: 0 4px;
  }}

  .nav-group {{
    display: flex;
    gap: 4px;
    align-items: center;
  }}

  .nav-group input[type="datetime-local"] {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    padding: 5px 8px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg-card);
    color: var(--text-primary);
    color-scheme: dark;
    outline: none;
    width: 190px;
  }}

  .nav-group input[type="datetime-local"]:focus {{
    border-color: var(--accent);
  }}

  .nav-group .nav-label {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: var(--text-muted);
    margin-right: 2px;
  }}

  .nav-group button {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg-card);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
    line-height: 1;
  }}

  .nav-group button:hover {{
    background: var(--bg-card-hover);
    color: var(--text-primary);
  }}

  .window-info {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: var(--text-muted);
    padding: 2px 32px 0;
  }}

  .tooltip-custom {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 11px !important;
  }}

  .footer {{
    padding: 16px 32px;
    border-top: 1px solid var(--border);
    font-size: 11px;
    color: var(--text-muted);
    font-family: 'JetBrains Mono', monospace;
    text-align: center;
  }}

  .scale-settings {{
    padding: 8px 32px;
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    align-items: center;
  }}

  .scale-settings .scale-group {{
    display: flex;
    gap: 6px;
    align-items: center;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 10px;
  }}

  .scale-settings .scale-label {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: var(--text-muted);
    min-width: 45px;
  }}

  .scale-settings input[type="number"] {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    width: 65px;
    padding: 3px 6px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-primary);
    color: var(--text-primary);
    outline: none;
  }}

  .scale-settings input[type="number"]:focus {{
    border-color: var(--accent);
  }}

  .scale-settings input[type="number"]:disabled {{
    opacity: 0.4;
    cursor: not-allowed;
  }}

  .scale-settings input[type="checkbox"] {{
    accent-color: var(--accent);
    cursor: pointer;
  }}

  .scale-settings .auto-label {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: var(--text-secondary);
    cursor: pointer;
  }}

  .scale-settings .scale-btn {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-card);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
  }}

  .scale-settings .scale-btn:hover {{
    background: var(--bg-card-hover);
    color: var(--text-primary);
  }}

  .scale-settings .scale-btn.apply {{
    background: var(--accent);
    border-color: var(--accent);
    color: #fff;
  }}

  .scale-settings .scale-btn.apply:hover {{
    background: #2563eb;
  }}

  @media (max-width: 768px) {{
    .header, .stats-row, .chart-container, .controls, .footer, .scale-settings {{
      padding-left: 16px;
      padding-right: 16px;
    }}
    .header h1 {{ font-size: 15px; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Au / Ag Minute-Resolution Monitor</h1>
  <span class="meta">{resample_label} &bull; 時刻は JST (UTC+9) &bull; Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</span>
</div>

<div class="stats-row" id="statsRow"></div>

<div class="controls">
  <button onclick="setRange('all')" id="btn-all">全期間</button>
  <button onclick="setRange('1w')" id="btn-1w">1週</button>
  <button onclick="setRange('5d')" id="btn-5d">5日</button>
  <button onclick="setRange('3d')" id="btn-3d">3日</button>
  <button onclick="setRange('1d')" class="active" id="btn-1d">1日</button>
  <button onclick="setRange('12h')" id="btn-12h">12h</button>
  <button onclick="setRange('6h')" id="btn-6h">6h</button>
  <button onclick="setRange('1h')" id="btn-1h">1h</button>
  <div class="sep"></div>
  <div class="nav-group" id="navGroup" style="display:none">
    <button onclick="navStep(-1)" title="前へ" id="btn-prev">◀</button>
    <span class="nav-label">終点:</span>
    <input type="datetime-local" id="navEnd" step="60" onchange="onNavEndChange()">
    <button onclick="navStep(+1)" title="次へ" id="btn-next">▶</button>
    <button onclick="navLatest()" title="最新へ" id="btn-latest" style="font-size:11px">最新▶▶</button>
  </div>
  <div class="sep"></div>
  <button onclick="toggleVolume()" id="btn-vol">出来高 ON/OFF</button>
  <button onclick="toggleVolLog()" id="btn-vollog">出来高 Log</button>
  <div class="sep"></div>
  <button onclick="toggleScalePanel()" id="btn-scale">Y軸設定</button>
</div>
<div class="window-info" id="windowInfo"></div>

<div class="scale-settings" id="scaleSettings" style="display:none">
  <div class="scale-group">
    <span class="scale-label" style="color:var(--gold)">Gold:</span>
    <input type="checkbox" id="goldAuto" onchange="onScaleAutoChange('gold')">
    <label class="auto-label" for="goldAuto">自動</label>
    <input type="number" id="goldYMin" value="{y_scales['gold'][0]}" step="100">
    <span style="color:var(--text-muted)">-</span>
    <input type="number" id="goldYMax" value="{y_scales['gold'][1]}" step="100">
  </div>
  <div class="scale-group">
    <span class="scale-label" style="color:var(--silver)">Silver:</span>
    <input type="checkbox" id="silverAuto" onchange="onScaleAutoChange('silver')">
    <label class="auto-label" for="silverAuto">自動</label>
    <input type="number" id="silverYMin" value="{y_scales['silver'][0]}" step="5">
    <span style="color:var(--text-muted)">-</span>
    <input type="number" id="silverYMax" value="{y_scales['silver'][1]}" step="5">
  </div>
  <div class="scale-group">
    <span class="scale-label" style="color:var(--ratio)">Ratio:</span>
    <input type="checkbox" id="ratioAuto" onchange="onScaleAutoChange('ratio')">
    <label class="auto-label" for="ratioAuto">自動</label>
    <input type="number" id="ratioYMin" value="{y_scales['ratio'][0]}" step="5">
    <span style="color:var(--text-muted)">-</span>
    <input type="number" id="ratioYMax" value="{y_scales['ratio'][1]}" step="5">
  </div>
  <button class="scale-btn apply" onclick="applyScaleSettings()">適用</button>
  <button class="scale-btn" onclick="resetScaleDefaults()">デフォルトに戻す</button>
</div>

<div class="chart-container">
  <div class="chart-box">
    <div class="chart-title"><span class="dot" style="background:var(--gold)"></span>Gold Futures (GC=F) — USD/oz</div>
    <div class="chart-wrap tall"><canvas id="chartGold"></canvas></div>
  </div>

  <div class="chart-box">
    <div class="chart-title"><span class="dot" style="background:var(--silver)"></span>Silver Futures (SI=F) — USD/oz</div>
    <div class="chart-wrap tall"><canvas id="chartSilver"></canvas></div>
  </div>

  <div class="chart-box">
    <div class="chart-title"><span class="dot" style="background:var(--ratio)"></span>Gold / Silver Ratio</div>
    <div class="chart-wrap short"><canvas id="chartRatio"></canvas></div>
  </div>
</div>

<div class="footer">
  precious_metals_collector &bull; yfinance 1-min data &bull; SQLite WAL
  &bull; <span id="reloadStatus"></span>
  <button onclick="toggleAutoReload()" id="btn-reload"
    style="font-family:'JetBrains Mono';font-size:10px;padding:2px 8px;
           border:1px solid var(--border);border-radius:4px;
           background:var(--bg-card);color:var(--text-muted);cursor:pointer;
           margin-left:4px;vertical-align:middle;">
    自動更新 ON
  </button>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script>
// ── データ埋め込み ──
const goldTs    = {json.dumps(g_ts)};
const goldOpen  = {json.dumps(g_open)};
const goldClose = {json.dumps(g_close)};
const goldHigh  = {json.dumps(g_high)};
const goldLow   = {json.dumps(g_low)};
const goldVol   = {json.dumps(g_vol)};

const silverTs    = {json.dumps(s_ts)};
const silverOpen  = {json.dumps(s_open)};
const silverClose = {json.dumps(s_close)};
const silverHigh  = {json.dumps(s_high)};
const silverLow   = {json.dumps(s_low)};
const silverVol   = {json.dumps(s_vol)};

const ratioTs   = {json.dumps(ratio_ts)};
const ratioVals = {json.dumps(ratio_vals)};

// ── Y軸スケール設定 ──
const defaultScaleSettings = {{
  gold:   {{ auto: true, min: {y_scales['gold'][0]}, max: {y_scales['gold'][1]} }},
  silver: {{ auto: true, min: {y_scales['silver'][0]}, max: {y_scales['silver'][1]} }},
  ratio:  {{ auto: true, min: {y_scales['ratio'][0]}, max: {y_scales['ratio'][1]} }}
}};

let scaleSettings = JSON.parse(JSON.stringify(defaultScaleSettings));

function toggleScalePanel() {{
  const panel = document.getElementById('scaleSettings');
  const btn = document.getElementById('btn-scale');
  const isHidden = panel.style.display === 'none';
  panel.style.display = isHidden ? 'flex' : 'none';
  btn.classList.toggle('active', isHidden);
}}

function onScaleAutoChange(type) {{
  const auto = document.getElementById(type + 'Auto').checked;
  document.getElementById(type + 'YMin').disabled = auto;
  document.getElementById(type + 'YMax').disabled = auto;
}}

function applyScaleSettings() {{
  scaleSettings.gold.auto = document.getElementById('goldAuto').checked;
  scaleSettings.gold.min = parseFloat(document.getElementById('goldYMin').value);
  scaleSettings.gold.max = parseFloat(document.getElementById('goldYMax').value);

  scaleSettings.silver.auto = document.getElementById('silverAuto').checked;
  scaleSettings.silver.min = parseFloat(document.getElementById('silverYMin').value);
  scaleSettings.silver.max = parseFloat(document.getElementById('silverYMax').value);

  scaleSettings.ratio.auto = document.getElementById('ratioAuto').checked;
  scaleSettings.ratio.min = parseFloat(document.getElementById('ratioYMin').value);
  scaleSettings.ratio.max = parseFloat(document.getElementById('ratioYMax').value);

  buildCharts();
  saveSettingsToStorage();
}}

function resetScaleDefaults() {{
  scaleSettings = JSON.parse(JSON.stringify(defaultScaleSettings));
  updateScaleInputs();
  buildCharts();
  saveSettingsToStorage();
}}

function updateScaleInputs() {{
  document.getElementById('goldAuto').checked = scaleSettings.gold.auto;
  document.getElementById('goldYMin').value = scaleSettings.gold.min;
  document.getElementById('goldYMax').value = scaleSettings.gold.max;
  document.getElementById('goldYMin').disabled = scaleSettings.gold.auto;
  document.getElementById('goldYMax').disabled = scaleSettings.gold.auto;

  document.getElementById('silverAuto').checked = scaleSettings.silver.auto;
  document.getElementById('silverYMin').value = scaleSettings.silver.min;
  document.getElementById('silverYMax').value = scaleSettings.silver.max;
  document.getElementById('silverYMin').disabled = scaleSettings.silver.auto;
  document.getElementById('silverYMax').disabled = scaleSettings.silver.auto;

  document.getElementById('ratioAuto').checked = scaleSettings.ratio.auto;
  document.getElementById('ratioYMin').value = scaleSettings.ratio.min;
  document.getElementById('ratioYMax').value = scaleSettings.ratio.max;
  document.getElementById('ratioYMin').disabled = scaleSettings.ratio.auto;
  document.getElementById('ratioYMax').disabled = scaleSettings.ratio.auto;
}}

// ── 統計カード ──
function calcStats(close, vol, label, color) {{
  if (!close || !close.length) return null;
  const valid = close.filter(v => v !== null);
  if (!valid.length) return null;
  const latest = valid[valid.length - 1];
  const first = valid[0];
  const change = ((latest - first) / first * 100).toFixed(2);
  const hi = Math.max(...valid);
  const lo = Math.min(...valid);
  const totalVol = vol ? vol.reduce((a, b) => a + (b || 0), 0) : null;
  return {{ label, latest, change: parseFloat(change), hi, lo, totalVol, color }};
}}

function renderStats(g, s, r) {{
  const stats = [
    calcStats(g.close, g.vol, 'GOLD', 'var(--gold)'),
    calcStats(s.close, s.vol, 'SILVER', 'var(--silver)'),
  ].filter(Boolean);

  if (r.vals && r.vals.length) {{
    const rv = r.vals.filter(v => v !== null);
    if (rv.length) {{
      stats.push({{
        label: 'Au/Ag RATIO',
        latest: rv[rv.length - 1],
        change: parseFloat(((rv[rv.length-1] - rv[0]) / rv[0] * 100).toFixed(2)),
        hi: Math.max(...rv),
        lo: Math.min(...rv),
        totalVol: null,
        color: 'var(--ratio)'
      }});
    }}
  }}

  const row = document.getElementById('statsRow');
  row.innerHTML = '';
  stats.forEach((s, idx) => {{
    // カテゴリ間にセパレーターを挿入（GOLD/SILVER/RATIO の区切り）
    if (idx > 0) {{
      row.innerHTML += `<div class="stat-separator"></div>`;
    }}
    const chgClass = s.change >= 0 ? 'up' : 'down';
    const chgSign = s.change >= 0 ? '+' : '';
    row.innerHTML += `
      <div class="stat-card">
        <div class="label">${{s.label}} Latest</div>
        <div class="value" style="color:${{s.color}}">${{s.latest.toLocaleString()}}</div>
        <div class="change ${{chgClass}}">${{chgSign}}${{s.change}}% over period</div>
      </div>
      <div class="stat-card">
        <div class="label">${{s.label}} Range</div>
        <div class="value" style="color:${{s.color}};font-size:14px">${{s.lo.toLocaleString()}} — ${{s.hi.toLocaleString()}}</div>
        ${{s.totalVol !== null ? `<div class="change" style="color:var(--text-muted)">Vol: ${{(s.totalVol/1000).toFixed(0)}}K</div>` : ''}}
      </div>`;
  }});
}}

// ── チャート共通設定 ──
const commonOptions = (yLabel) => ({{
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  interaction: {{ mode: 'index', intersect: false }},
  plugins: {{
    legend: {{ display: false }},
    tooltip: {{
      backgroundColor: '#1e293b',
      titleColor: '#e2e8f0',
      bodyColor: '#94a3b8',
      borderColor: '#334155',
      borderWidth: 1,
      titleFont: {{ family: 'JetBrains Mono', size: 11 }},
      bodyFont: {{ family: 'JetBrains Mono', size: 11 }},
      padding: 10,
      displayColors: true,
      callbacks: {{
        title: (items) => items[0]?.label || '',
        label: function(ctx) {{
          const ds = ctx.dataset;
          if (ds._candle && ds._candleData) {{
            const d = ds._candleData[ctx.dataIndex];
            if (d) return [`O: ${{d.o}}  H: ${{d.h}}`, `L: ${{d.l}}  C: ${{d.c}}`];
          }}
          if (ds.label === 'Volume') return `Vol: ${{ctx.parsed.y.toLocaleString()}}`;
          return `${{ds.label}}: ${{ctx.parsed.y}}`;
        }}
      }}
    }}
  }},
  scales: {{
    x: {{
      type: 'time',
      offset: false,  // バーのオフセットを無効化（余白を削除）
      bounds: 'data', // データの範囲に基づいて境界を設定
      time: {{
        parser: "yyyy-MM-dd'T'HH:mm:ss",
        tooltipFormat: 'MM-dd HH:mm',
      }},
      ticks: {{
        font: {{ family: 'JetBrains Mono', size: 10 }},
        color: '#64748b',
        maxRotation: 0,
        maxTicksLimit: 20,
        // callback は buildCharts で範囲ごとに設定
      }},
      grid: {{ color: 'rgba(30,41,59,0.5)', drawBorder: false }}
    }},
    y: {{
      position: 'right',
      ticks: {{
        font: {{ family: 'JetBrains Mono', size: 10 }},
        color: '#64748b',
      }},
      grid: {{ color: 'rgba(30,41,59,0.5)', drawBorder: false }},
      title: {{
        display: true,
        text: yLabel,
        font: {{ family: 'JetBrains Mono', size: 10 }},
        color: '#64748b'
      }}
    }},
    yVol: {{
      position: 'left',
      display: true,  // 常に表示（幅を確保するため）
      beginAtZero: true,
      grid: {{ display: false }},
      afterFit: function(scale) {{
        scale.width = 60;  // 軸幅を常に60pxに固定（出来高ON/OFF時のズレ防止）
      }},
      ticks: {{
        display: false,  // デフォルトは非表示（buildCharts で切り替え）
        font: {{ family: 'JetBrains Mono', size: 9 }},
        color: '#64748b',
      }}
    }}
  }}
}});

// ── データセット構築 ── (time axis 用)
function zip(ts, vals) {{
  return ts.map((t, i) => vals[i] !== null ? {{ x: t, y: vals[i] }} : null).filter(Boolean);
}}

// ローソク足データ構築: {{x, o, h, l, c}} 配列
function buildCandleData(ts, opn, high, low, close) {{
  const data = [];
  for (let i = 0; i < ts.length; i++) {{
    if (opn[i] != null && high[i] != null && low[i] != null && close[i] != null) {{
      data.push({{ x: ts[i], o: opn[i], h: high[i], l: low[i], c: close[i] }});
    }}
  }}
  return data;
}}

// ローソク足描画プラグイン
const candlestickPlugin = {{
  id: 'candlestick',
  afterDatasetsDraw(chart) {{
    const meta = chart.getDatasetMeta(0);
    if (!meta || !meta.data.length) return;
    const dataset = chart.data.datasets[0];
    if (!dataset._candle) return;  // マーカー

    const ctx = chart.ctx;
    const xScale = chart.scales.x;
    const yScale = chart.scales.y;
    const candleData = dataset._candleData;
    if (!candleData || !candleData.length) return;

    // ローソク幅: 表示範囲全体の密度から計算
    let barWidth = 3;
    if (candleData.length > 1) {{
      // 表示範囲全体のピクセル幅とデータポイント数から平均間隔を計算
      const rangeStartMs = xScale.min;
      const rangeEndMs = xScale.max;
      const rangeStartPx = xScale.getPixelForValue(rangeStartMs);
      const rangeEndPx = xScale.getPixelForValue(rangeEndMs);
      const totalWidth = Math.abs(rangeEndPx - rangeStartPx);

      // 表示範囲内のデータポイント数をカウント
      let visibleCount = 0;
      for (const d of candleData) {{
        const ms = new Date(d.x).getTime();
        if (ms >= rangeStartMs && ms <= rangeEndMs) {{
          visibleCount++;
        }}
      }}

      if (visibleCount > 1) {{
        const avgInterval = totalWidth / visibleCount;
        barWidth = Math.max(1, Math.min(12, avgInterval * 0.6));
      }}
    }}
    const halfBar = barWidth / 2;

    ctx.save();
    for (const d of candleData) {{
      const xPx = xScale.getPixelForValue(new Date(d.x).getTime());
      const oPx = yScale.getPixelForValue(d.o);
      const cPx = yScale.getPixelForValue(d.c);
      const hPx = yScale.getPixelForValue(d.h);
      const lPx = yScale.getPixelForValue(d.l);

      if (isNaN(xPx) || isNaN(oPx)) continue;

      const isUp = d.c >= d.o;
      const color = isUp ? '#22c55e' : '#ef4444';

      // ヒゲ（上下の細線）
      ctx.beginPath();
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.moveTo(xPx, hPx);
      ctx.lineTo(xPx, lPx);
      ctx.stroke();

      // 実体（矩形）
      const top = Math.min(oPx, cPx);
      const bodyH = Math.max(1, Math.abs(cPx - oPx));
      ctx.fillStyle = color;
      ctx.fillRect(xPx - halfBar, top, barWidth, bodyH);
    }}
    ctx.restore();
  }}
}};

// X軸に日付ラベルを描画するプラグイン（各日の00:00位置にラベルと縦線を表示）
const tickLinePlugin = {{
  id: 'tickLine',
  afterDraw(chart) {{
    const xScale = chart.scales.x;
    const ctx = chart.ctx;
    const chartArea = chart.chartArea;
    if (!xScale || !chartArea) return;

    // 表示範囲の開始・終了時刻を取得
    const minMs = xScale.min;
    const maxMs = xScale.max;
    if (!minMs || !maxMs) return;

    ctx.save();

    // 表示範囲内の各日の00:00を計算して描画
    const startDate = new Date(minMs);
    const endDate = new Date(maxMs);

    // 開始日の00:00を取得（JSTベースで計算）
    let currentDate = new Date(startDate);
    currentDate.setHours(0, 0, 0, 0);

    // 開始日より前なら次の日へ
    if (currentDate.getTime() < minMs) {{
      currentDate.setDate(currentDate.getDate() + 1);
    }}

    while (currentDate.getTime() <= maxMs) {{
      const xPx = xScale.getPixelForValue(currentDate.getTime());

      // チャート領域内のみ描画
      if (xPx >= chartArea.left && xPx <= chartArea.right) {{
        // 日付ラベルをチャート上部に描画
        ctx.fillStyle = '#94a3b8';
        ctx.font = '10px "JetBrains Mono", monospace';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        const mm = String(currentDate.getMonth() + 1).padStart(2, '0');
        const dd = String(currentDate.getDate()).padStart(2, '0');
        const dateLabel = mm + '-' + dd;
        ctx.fillText(dateLabel, xPx + 4, chartArea.top + 4);

        // 00:00の位置に縦線を描画
        ctx.strokeStyle = 'rgba(100, 116, 139, 0.5)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(xPx, chartArea.top);
        ctx.lineTo(xPx, chartArea.bottom);
        ctx.stroke();
      }}

      // 次の日へ
      currentDate.setDate(currentDate.getDate() + 1);
    }}
    ctx.restore();
  }}
}};

// ローソク足データセット: closeのラインを非表示にし、プラグインで描画
function makePriceDataset(ts, opn, close, high, low) {{
  const candleData = buildCandleData(ts, opn, high, low, close);
  return [
    {{
      label: 'OHLC',
      data: zip(ts, close),
      borderColor: 'transparent',
      backgroundColor: 'transparent',
      borderWidth: 0,
      pointRadius: 0,
      pointHitRadius: 4,
      yAxisID: 'y',
      order: 1,
      _candle: true,
      _candleData: candleData,
    }},
  ];
}}

function makeVolDataset(ts, vol, opn, close, hidden = false) {{
  // 価格変動に応じた色分け（上昇: 薄緑、下落: 薄赤）
  const colorUp = 'rgba(0, 255, 255, 0.3)';     // シアン（上昇時）
  const colorDown = 'rgba(255, 0, 255, 0.3)';   // マゼンタ（下落時）
  const colors = vol.map((v, i) => {{
    if (opn[i] == null || close[i] == null) return colorUp;
    return close[i] >= opn[i] ? colorUp : colorDown;
  }});
  const data = vol.map((v, i) => {{
    const y = volLog ? (v > 0 ? Math.log10(v + 1) : 0) : v;
    return {{ x: ts[i], y: y }};
  }});
  return {{
    label: 'Volume',
    data: data,
    type: 'bar',
    backgroundColor: hidden ? 'transparent' : colors,  // 非表示時は透明
    borderWidth: 0,
    yAxisID: 'yVol',
    order: 3,
    barPercentage: 0.8,
    hidden: hidden,  // Chart.js の hidden プロパティ
  }};
}}

// ── チャート生成 ──
let showVolume = true;
let volLog = false;
let currentRange = '1d';
let navEndMs = null;  // 終点ミリ秒
let followLatest = true;  // 最新追従モード（true: 常に最新を表示）

// データ保持
const fullData = {{
  gold: {{ ts: goldTs, open: goldOpen, close: goldClose, high: goldHigh, low: goldLow, vol: goldVol }},
  silver: {{ ts: silverTs, open: silverOpen, close: silverClose, high: silverHigh, low: silverLow, vol: silverVol }},
  ratio: {{ ts: ratioTs, vals: ratioVals }},
}};

// データの最初/最後のタイムスタンプ (ms)
const allTs = goldTs.concat(silverTs);
const dataMinMs = allTs.length ? new Date(allTs[0]).getTime() : 0;
const dataMaxMs = allTs.length ? new Date(allTs[allTs.length - 1]).getTime() : 0;

const rangeHours = {{ '1w': 168, '5d': 120, '3d': 72, '1d': 24, '12h': 12, '6h': 6, '1h': 1 }};

// スクロール/ナビ操作用のデータポイント数定義
const rangePoints = {{
  '1w': 7 * 24 * 60,   // 10080 ポイント
  '5d': 5 * 24 * 60,   // 7200 ポイント
  '3d': 3 * 24 * 60,   // 4320 ポイント
  '1d': 24 * 60,       // 1440 ポイント
  '12h': 12 * 60,      // 720 ポイント
  '6h': 6 * 60,        // 360 ポイント
  '1h': 60,            // 60 ポイント
}};

// 全タイムスタンプ配列（ソート済み・ユニーク・ms）- スクロール/ナビ用
const allTsSortedMs = [...new Set(goldTs.concat(silverTs))]
  .map(t => new Date(t).getTime())
  .sort((a, b) => a - b);

// 指定msに最も近いデータのインデックスを二分探索で取得
function findNearestIndex(targetMs) {{
  if (!allTsSortedMs.length) return 0;
  let lo = 0, hi = allTsSortedMs.length - 1;
  while (lo < hi) {{
    const mid = Math.floor((lo + hi) / 2);
    if (allTsSortedMs[mid] < targetMs) lo = mid + 1;
    else hi = mid;
  }}
  return lo;
}}

// 指定ms以下で最大のデータインデックスを取得
function getEndIndex(endMs) {{
  const idx = findNearestIndex(endMs);
  if (idx > 0 && allTsSortedMs[idx] > endMs) return idx - 1;
  if (idx === 0 && allTsSortedMs[idx] > endMs) return 0;
  return Math.min(idx, allTsSortedMs.length - 1);
}}

function sliceByWindow(data, startMs, endMs) {{
  if (!data.ts.length) return data;
  const result = {{ ts: [], open: [], close: [], high: [], low: [], vol: [] }};
  for (let i = 0; i < data.ts.length; i++) {{
    const ms = new Date(data.ts[i]).getTime();
    if (ms >= startMs && ms <= endMs) {{
      result.ts.push(data.ts[i]);
      if (data.open)  result.open.push(data.open[i]);
      if (data.close) result.close.push(data.close[i]);
      if (data.high)  result.high.push(data.high[i]);
      if (data.low)   result.low.push(data.low[i]);
      if (data.vol)   result.vol.push(data.vol[i]);
    }}
  }}
  return result;
}}

function sliceRatioByWindow(data, startMs, endMs) {{
  if (!data.ts.length) return data;
  const result = {{ ts: [], vals: [] }};
  for (let i = 0; i < data.ts.length; i++) {{
    const ms = new Date(data.ts[i]).getTime();
    if (ms >= startMs && ms <= endMs) {{
      result.ts.push(data.ts[i]);
      result.vals.push(data.vals[i]);
    }}
  }}
  return result;
}}

// 現在の表示ウィンドウ [startMs, endMs] を計算
function getWindow() {{
  if (currentRange === 'all') {{
    return [dataMinMs, dataMaxMs];
  }}
  const hours = rangeHours[currentRange] || 24;
  const endMs = followLatest ? dataMaxMs : navEndMs;
  const startMs = endMs - hours * 3600 * 1000;
  return [startMs, endMs];
}}

// datetime-local の値 ⇔ ms 変換
function msToLocalInput(ms) {{
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${{d.getFullYear()}}-${{pad(d.getMonth()+1)}}-${{pad(d.getDate())}}T${{pad(d.getHours())}}:${{pad(d.getMinutes())}}`;
}}

function localInputToMs(val) {{
  return new Date(val).getTime();
}}

// ナビ UI 更新
function updateNavUI() {{
  const navGroup = document.getElementById('navGroup');
  const navInput = document.getElementById('navEnd');
  const info = document.getElementById('windowInfo');
  const isWindowed = currentRange !== 'all';

  navGroup.style.display = isWindowed ? 'flex' : 'none';

  if (isWindowed) {{
    const [startMs, endMs] = getWindow();
    navInput.value = msToLocalInput(endMs);

    // min/max 制限
    navInput.min = msToLocalInput(dataMinMs);
    navInput.max = msToLocalInput(dataMaxMs);

    // 前後ボタンの有効/無効
    const hours = rangeHours[currentRange] || 24;
    document.getElementById('btn-prev').disabled = (startMs <= dataMinMs);
    document.getElementById('btn-next').disabled = followLatest;
    document.getElementById('btn-latest').disabled = followLatest;

    // ウィンドウ情報表示
    const fmtShort = (ms) => {{
      const d = new Date(ms);
      const pad = (n) => String(n).padStart(2, '0');
      return `${{pad(d.getMonth()+1)}}-${{pad(d.getDate())}} ${{pad(d.getHours())}}:${{pad(d.getMinutes())}}`;
    }};
    info.textContent = `表示区間: ${{fmtShort(startMs)}} — ${{fmtShort(endMs)}} JST`;
  }} else {{
    info.textContent = '';
  }}
}}

// ナビ操作
function onNavEndChange() {{
  const val = document.getElementById('navEnd').value;
  if (!val) return;
  navEndMs = localInputToMs(val);
  followLatest = false;  // 手動指定したので追従OFF
  // データ範囲にクランプ
  if (navEndMs > dataMaxMs) navEndMs = dataMaxMs;
  const hours = rangeHours[currentRange] || 24;
  const minEnd = dataMinMs + hours * 3600 * 1000;
  if (navEndMs < minEnd) navEndMs = minEnd;
  buildCharts();
}}

function navStep(dir) {{
  if (!allTsSortedMs.length) return;

  // データポイント数ベースでスライド（ウィンドウの半分ずつ）
  const windowPoints = rangePoints[currentRange] || 1440;
  const stepPoints = Math.ceil(windowPoints * 0.5);

  const currentEndMs = followLatest ? dataMaxMs : navEndMs;
  const currentEndIdx = getEndIndex(currentEndMs);

  let newEndIdx = currentEndIdx + dir * stepPoints;
  newEndIdx = Math.max(0, Math.min(newEndIdx, allTsSortedMs.length - 1));

  navEndMs = allTsSortedMs[newEndIdx];
  followLatest = (newEndIdx >= allTsSortedMs.length - 1);

  buildCharts();
}}

function navLatest() {{
  followLatest = true;  // 最新追従モードON
  buildCharts();
}}

let chartGold, chartSilver, chartRatio;

// 出来高軸の上限を適応的に決定（高さ1/2のため2倍に設定）
function volAxisMax(vol) {{
  const v = volLog ? vol.map(x => x > 0 ? Math.log10(x + 1) : 0) : vol;
  const valid = v.filter(x => x > 0).sort((a, b) => a - b);
  if (!valid.length) return 100;
  if (volLog) {{
    // log モード: 最大値にマージン 1.15 倍 × 2（高さ半分）
    return valid[valid.length - 1] * 1.15 * 2;
  }}
  const p75 = valid[Math.floor(valid.length * 0.75)];
  return p75 * 3.3 * 2;  // 高さ半分のため2倍
}}

function buildCharts() {{
  if (chartGold) chartGold.destroy();
  if (chartSilver) chartSilver.destroy();
  if (chartRatio) chartRatio.destroy();

  const [startMs, endMs] = getWindow();

  // 範囲に応じた目盛り設定を決定
  // unit: Chart.js の time unit, stepSize: その unit での間隔
  const tickConfig = {{
    '1h':  {{ unit: 'minute', stepSize: 10 }},
    '6h':  {{ unit: 'minute', stepSize: 30 }},
    '12h': {{ unit: 'hour',   stepSize: 1 }},
    '1d':  {{ unit: 'hour',   stepSize: 2 }},
    '3d':  {{ unit: 'hour',   stepSize: 6 }},
    '5d':  {{ unit: 'hour',   stepSize: 12 }},
    '1w':  {{ unit: 'hour',   stepSize: 12 }},
    'all': {{ unit: 'day',    stepSize: 1 }},
  }}[currentRange] || {{ unit: 'hour', stepSize: 2 }};

  // X軸の目盛りラベルをフォーマットする callback（常に時刻のみ表示）
  function makeXTicksCallback() {{
    return function(value, index, ticks) {{
      const d = new Date(value);
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      return hh + ':' + mm;
    }};
  }}

  // X軸の time 設定を適用するヘルパー
  function applyXAxisConfig(opts, rangeStartMs, rangeEndMs) {{
    opts.scales.x.time.unit = tickConfig.unit;
    opts.scales.x.time.stepSize = tickConfig.stepSize;
    opts.scales.x.ticks.callback = makeXTicksCallback();

    // 6h/1h の場合、切りの良い時刻に目盛りを明示的に配置
    if (currentRange === '6h' || currentRange === '1h') {{
      const intervalMin = (currentRange === '1h') ? 10 : 30;
      const intervalMs = intervalMin * 60 * 1000;

      // 表示範囲の開始を切りの良い時刻に切り上げ
      const firstTickMs = Math.ceil(rangeStartMs / intervalMs) * intervalMs;

      // 目盛り配列を生成
      const tickValues = [];
      for (let t = firstTickMs; t <= rangeEndMs; t += intervalMs) {{
        tickValues.push(t);
      }}

      // afterBuildTicks で Chart.js の自動生成目盛りを上書き
      opts.scales.x.afterBuildTicks = function(axis) {{
        axis.ticks = tickValues.map(function(v) {{ return {{ value: v }}; }});
      }};
    }}
  }}

  const g = currentRange === 'all' ? fullData.gold : sliceByWindow(fullData.gold, startMs, endMs);
  const s = currentRange === 'all' ? fullData.silver : sliceByWindow(fullData.silver, startMs, endMs);
  const r = currentRange === 'all' ? fullData.ratio : sliceRatioByWindow(fullData.ratio, startMs, endMs);

  // ナビ UI 更新
  updateNavUI();

  // 統計カード更新 (ウィンドウ適用後のデータで再計算)
  renderStats(g, s, r);

  // Gold
  const goldDatasets = makePriceDataset(g.ts, g.open, g.close, g.high, g.low);
  goldDatasets.push(makeVolDataset(g.ts, g.vol, g.open, g.close, !showVolume));  // 常に追加、hidden で制御

  const goldOpts = commonOptions('USD/oz');
  // X軸の範囲を明示的に設定（データ欠損があっても指定幅を維持）
  goldOpts.scales.x.min = startMs;
  goldOpts.scales.x.max = endMs;
  applyXAxisConfig(goldOpts, startMs, endMs);
  goldOpts.scales.yVol.ticks.display = showVolume;  // 軸は常に表示、目盛りラベルのみ切り替え
  goldOpts.scales.yVol.max = volAxisMax(g.vol);  // 常に設定（レイアウト安定化）
  // Y軸スケール設定を適用
  if (!scaleSettings.gold.auto) {{
    goldOpts.scales.y.min = scaleSettings.gold.min;
    goldOpts.scales.y.max = scaleSettings.gold.max;
  }}

  chartGold = new Chart(document.getElementById('chartGold'), {{
    type: 'line',
    data: {{ datasets: goldDatasets }},
    options: goldOpts,
    plugins: [tickLinePlugin, candlestickPlugin],
  }});

  // Silver
  const silverDatasets = makePriceDataset(s.ts, s.open, s.close, s.high, s.low);
  silverDatasets.push(makeVolDataset(s.ts, s.vol, s.open, s.close, !showVolume));  // 常に追加、hidden で制御

  const silverOpts = commonOptions('USD/oz');
  // X軸の範囲を明示的に設定（データ欠損があっても指定幅を維持）
  silverOpts.scales.x.min = startMs;
  silverOpts.scales.x.max = endMs;
  applyXAxisConfig(silverOpts, startMs, endMs);
  // Y軸スケール設定を適用
  if (!scaleSettings.silver.auto) {{
    silverOpts.scales.y.min = scaleSettings.silver.min;
    silverOpts.scales.y.max = scaleSettings.silver.max;
  }}
  silverOpts.scales.yVol.ticks.display = showVolume;  // 軸は常に表示、目盛りラベルのみ切り替え
  silverOpts.scales.yVol.max = volAxisMax(s.vol);  // 常に設定（レイアウト安定化）

  chartSilver = new Chart(document.getElementById('chartSilver'), {{
    type: 'line',
    data: {{ datasets: silverDatasets }},
    options: silverOpts,
    plugins: [tickLinePlugin, candlestickPlugin],
  }});

  // Ratio
  const ratioOpts = commonOptions('Ratio');
  // X軸の範囲を明示的に設定（データ欠損があっても指定幅を維持）
  ratioOpts.scales.x.min = startMs;
  ratioOpts.scales.x.max = endMs;
  applyXAxisConfig(ratioOpts, startMs, endMs);
  // Y軸スケール設定を適用
  if (!scaleSettings.ratio.auto) {{
    ratioOpts.scales.y.min = scaleSettings.ratio.min;
    ratioOpts.scales.y.max = scaleSettings.ratio.max;
  }}

  chartRatio = new Chart(document.getElementById('chartRatio'), {{
    type: 'line',
    data: {{
      datasets: [{{
        label: 'Au/Ag Ratio',
        data: zip(r.ts, r.vals),
        borderColor: '#06b6d4',
        backgroundColor: 'rgba(6,182,212,0.06)',
        borderWidth: 1.2,
        pointRadius: 0,
        tension: 0.1,
        fill: true,
      }}],
    }},
    options: ratioOpts,
    plugins: [tickLinePlugin],
  }});

  // Shift+ホイールで横スクロールするためのイベントハンドラ登録
  [chartGold, chartSilver, chartRatio].forEach(chart => {{
    if (!chart || !chart.canvas) return;
    chart.canvas.removeEventListener('wheel', wheelHandler);
    chart.canvas.addEventListener('wheel', wheelHandler, {{ passive: false }});
  }});
}}

// Shift+ホイールによる横スクロールハンドラ（データポイント数ベース）
function wheelHandler(e) {{
  // Shift キーが押されていない、または全期間表示の場合はスキップ
  if (!e.shiftKey || currentRange === 'all') return;
  if (!allTsSortedMs.length) return;

  e.preventDefault();

  // 現在の終端インデックスを取得
  const currentEndMs = followLatest ? dataMaxMs : navEndMs;
  const currentEndIdx = getEndIndex(currentEndMs);

  // スクロール量：ウィンドウ内のデータポイント数の8%
  const windowPoints = rangePoints[currentRange] || 1440;
  const scrollPoints = Math.max(1, Math.ceil(windowPoints * 0.08));
  const direction = e.deltaY > 0 ? 1 : -1;  // 下=右, 上=左

  // 新しい終端インデックス
  let newEndIdx = currentEndIdx + direction * scrollPoints;
  newEndIdx = Math.max(0, Math.min(newEndIdx, allTsSortedMs.length - 1));

  // インデックスからmsに変換
  navEndMs = allTsSortedMs[newEndIdx];
  followLatest = (newEndIdx >= allTsSortedMs.length - 1);

  buildCharts();
}}

function setRange(range) {{
  currentRange = range;
  followLatest = true;  // 範囲選択時は常に最新を表示
  document.querySelectorAll('.controls > button').forEach(b => {{
    if (b.id === 'btn-vol' || b.id === 'btn-vollog' || b.id === 'btn-scale') return;
    b.classList.remove('active');
  }});
  document.getElementById('btn-' + range)?.classList.add('active');
  buildCharts();
  saveSettingsToStorage();
}}

function toggleVolume() {{
  showVolume = !showVolume;
  const btn = document.getElementById('btn-vol');
  btn.classList.toggle('active', showVolume);
  buildCharts();
  saveSettingsToStorage();
}}

function toggleVolLog() {{
  volLog = !volLog;
  const btn = document.getElementById('btn-vollog');
  btn.classList.toggle('active', volLog);
  if (!showVolume) {{
    showVolume = true;
    document.getElementById('btn-vol').classList.add('active');
  }}
  buildCharts();
  saveSettingsToStorage();
}}

// ── localStorage による設定永続化 ──
const STORAGE_KEY = 'auag_chart_settings';

function saveSettingsToStorage() {{
  const settings = {{
    range: currentRange,
    followLatest: followLatest,
    showVolume: showVolume,
    volLog: volLog,
    autoReload: autoReload,
    scaleSettings: scaleSettings,
  }};
  try {{
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
  }} catch (e) {{ /* localStorage unavailable */ }}
}}

function loadSettingsFromStorage() {{
  try {{
    const saved = localStorage.getItem(STORAGE_KEY);
    if (!saved) return false;
    const settings = JSON.parse(saved);

    if (settings.showVolume !== undefined) showVolume = settings.showVolume;
    if (settings.volLog !== undefined) volLog = settings.volLog;
    if (settings.autoReload !== undefined) autoReload = settings.autoReload;
    if (settings.range) {{
      currentRange = settings.range;
    }}
    // followLatest の復元（なければ最新追従とみなす）
    if (settings.followLatest !== undefined) {{
      followLatest = settings.followLatest;
    }} else {{
      followLatest = true;
    }}
    navEndMs = dataMaxMs;
    if (settings.scaleSettings) {{
      scaleSettings = settings.scaleSettings;
    }}
    return true;
  }} catch (e) {{ /* ignore */ }}
  return false;
}}

function syncUIState() {{
  // スケール入力を同期
  updateScaleInputs();

  // ボタン表示を同期
  document.querySelectorAll('.controls > button').forEach(b => {{
    if (b.id === 'btn-vol' || b.id === 'btn-vollog' || b.id === 'btn-scale') return;
    b.classList.remove('active');
  }});
  document.getElementById('btn-' + currentRange)?.classList.add('active');
  document.getElementById('btn-vol').classList.toggle('active', showVolume);
  document.getElementById('btn-vollog').classList.toggle('active', volLog);
}}

// ── 自動リロード（UI状態をURLハッシュに保存/復元）──
const RELOAD_INTERVAL_SEC = 180;
let autoReload = true;
let reloadTimer = null;
let countdownTimer = null;
let countdown = RELOAD_INTERVAL_SEC;

// 状態保存 → URLハッシュ（リロード時の一時保存用）
function saveStateToHash() {{
  const state = {{
    r: currentRange,
    e: navEndMs,
    fl: followLatest ? 1 : 0,
    v: showVolume ? 1 : 0,
    l: volLog ? 1 : 0,
    ar: autoReload ? 1 : 0,
    sc: scaleSettings,
  }};
  window.location.hash = encodeURIComponent(JSON.stringify(state));
}}

// 状態復元: URLハッシュ優先 → localStorage → デフォルト
function restoreState() {{
  let restored = false;

  // 1. URLハッシュがあれば優先（リロード時の状態保持）
  if (window.location.hash && window.location.hash.length >= 3) {{
    try {{
      const state = JSON.parse(decodeURIComponent(window.location.hash.slice(1)));
      if (state.v !== undefined) showVolume = state.v !== 0;
      if (state.l !== undefined) volLog = state.l !== 0;
      if (state.ar !== undefined) autoReload = state.ar !== 0;

      if (state.r) {{
        currentRange = state.r;
      }}

      // followLatest の復元（キー: fl）
      if (state.fl !== undefined) {{
        followLatest = state.fl !== 0;
      }} else {{
        // 旧形式互換: fl がなければ最新追従とみなす
        followLatest = true;
      }}
      navEndMs = state.e || dataMaxMs;

      if (state.sc) {{
        scaleSettings = state.sc;
      }}
      restored = true;
    }} catch (e) {{ /* ignore */ }}
  }}

  // 2. URLハッシュがなければ localStorage から
  if (!restored) {{
    restored = loadSettingsFromStorage();
  }}

  // 3. どちらからも復元しなかった場合（初回アクセス）: 最新追従モード
  if (!restored) {{
    followLatest = true;
  }}

  // UI を同期してチャート再構築
  syncUIState();
  buildCharts();
}}

function updateReloadUI() {{
  const statusEl = document.getElementById('reloadStatus');
  const btn = document.getElementById('btn-reload');
  if (autoReload) {{
    statusEl.textContent = `次回更新: ${{countdown}}秒`;
    btn.textContent = '自動更新 ON';
    btn.style.color = 'var(--success)';
    btn.style.borderColor = 'var(--success)';
  }} else {{
    statusEl.textContent = '自動更新 OFF';
    btn.textContent = '自動更新 OFF';
    btn.style.color = 'var(--text-muted)';
    btn.style.borderColor = 'var(--border)';
  }}
}}

function startReloadTimer() {{
  countdown = RELOAD_INTERVAL_SEC;
  updateReloadUI();
  countdownTimer = setInterval(() => {{
    countdown--;
    updateReloadUI();
    if (countdown <= 0) clearInterval(countdownTimer);
  }}, 1000);
  reloadTimer = setTimeout(() => {{
    if (autoReload) {{
      saveStateToHash();
      window.location.reload();
    }}
  }}, RELOAD_INTERVAL_SEC * 1000);
}}

function toggleAutoReload() {{
  autoReload = !autoReload;
  if (autoReload) {{
    startReloadTimer();
  }} else {{
    clearTimeout(reloadTimer);
    clearInterval(countdownTimer);
    updateReloadUI();
  }}
  saveSettingsToStorage();
}}

// 初期化: 状態復元 → リロードタイマー開始
restoreState();
if (autoReload) startReloadTimer();
else updateReloadUI();
</script>
</body>
</html>"""
    return html


def main():
    parser = argparse.ArgumentParser(
        description="蓄積データからインタラクティブな時系列チャート HTML を生成"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--start", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", help="終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--resample", default=None,
        help="リサンプル間隔 (例: 5min, 15min, 30min, 1h)"
    )
    parser.add_argument("-o", "--output", default="chart.html", help="出力 HTML ファイル名")
    # Y軸スケール設定
    parser.add_argument("--gold-ymin", type=float, default=Y_SCALE_GOLD[0], help="Gold Y軸最小値")
    parser.add_argument("--gold-ymax", type=float, default=Y_SCALE_GOLD[1], help="Gold Y軸最大値")
    parser.add_argument("--silver-ymin", type=float, default=Y_SCALE_SILVER[0], help="Silver Y軸最小値")
    parser.add_argument("--silver-ymax", type=float, default=Y_SCALE_SILVER[1], help="Silver Y軸最大値")
    parser.add_argument("--ratio-ymin", type=float, default=Y_SCALE_RATIO[0], help="Ratio Y軸最小値")
    parser.add_argument("--ratio-ymax", type=float, default=Y_SCALE_RATIO[1], help="Ratio Y軸最大値")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: DB ファイルが見つかりません: {args.db}")
        sys.exit(1)

    # リサンプル間隔のパース
    resample_min = 1
    resample_label = "1-min resolution"
    if args.resample:
        r = args.resample.lower().strip()
        if r.endswith("min"):
            resample_min = int(r.replace("min", ""))
        elif r.endswith("h"):
            resample_min = int(r.replace("h", "")) * 60
        else:
            resample_min = int(r)
        resample_label = f"{resample_min}-min resolution" if resample_min < 60 else f"{resample_min//60}h resolution"

    print(f"データ読み込み中... (DB: {args.db})")

    gold_data = load_data(args.db, "GC=F", args.start, args.end)
    silver_data = load_data(args.db, "SI=F", args.start, args.end)

    print(f"  Gold:   {len(gold_data):,} 行")
    print(f"  Silver: {len(silver_data):,} 行")

    if resample_min > 1:
        gold_data = resample_data(gold_data, resample_min)
        silver_data = resample_data(silver_data, resample_min)
        print(f"  リサンプル後: Gold={len(gold_data):,}, Silver={len(silver_data):,}")

    if not gold_data and not silver_data:
        print("ERROR: データが見つかりません")
        sys.exit(1)

    period_str = ""
    if args.start or args.end:
        period_str = f" ({args.start or '...'} ~ {args.end or '...'})"

    title = f"Au/Ag Monitor{period_str}"
    y_scales = {
        'gold': (args.gold_ymin, args.gold_ymax),
        'silver': (args.silver_ymin, args.silver_ymax),
        'ratio': (args.ratio_ymin, args.ratio_ymax),
    }
    html = generate_html(gold_data, silver_data, title, resample_label, y_scales)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")
    print(f"\n✓ チャート生成完了: {output_path}")
    print(f"  ブラウザで開いてください: file://{output_path.resolve()}")


if __name__ == "__main__":
    main()
