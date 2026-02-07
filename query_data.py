#!/usr/bin/env python3
"""
query_data.py - 蓄積データの確認・エクスポート・簡易分析ツール
=============================================================

Usage:
    python3 query_data.py stats                          # 蓄積状況
    python3 query_data.py export --symbol GC=F           # CSV エクスポート
    python3 query_data.py export --symbol GC=F --start 2026-01-20 --end 2026-01-25
    python3 query_data.py gaps --symbol GC=F             # 欠損区間の検出
    python3 query_data.py spectrum --symbol GC=F --date 2026-01-22  # 簡易スペクトル表示
"""

import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

DEFAULT_DB_PATH = Path(__file__).parent / "precious_metals.db"


def get_conn(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        print(f"ERROR: DB ファイルが見つかりません: {db_path}")
        sys.exit(1)
    return sqlite3.connect(str(db_path))


# ──────────────────────────────────────────────
# stats: 蓄積状況の表示
# ──────────────────────────────────────────────
def cmd_stats(args):
    conn = get_conn(args.db)

    print("\n=== 蓄積データ概要 ===\n")

    rows = conn.execute("""
        SELECT symbol,
               COUNT(*) as total,
               MIN(ts) as earliest,
               MAX(ts) as latest
        FROM ohlcv_1m
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()

    if not rows:
        print("データがありません。collector.py を実行してください。")
        return

    for symbol, total, earliest, latest in rows:
        # 日数計算
        d1 = datetime.fromisoformat(earliest.replace("+00:00", "+00:00"))
        d2 = datetime.fromisoformat(latest.replace("+00:00", "+00:00"))
        span_days = (d2 - d1).days + 1

        print(f"  {symbol}")
        print(f"    行数:     {total:>10,}")
        print(f"    期間:     {earliest[:16]} ~ {latest[:16]}")
        print(f"    日数:     {span_days} 日")
        print(f"    平均行/日: {total / max(span_days, 1):,.0f}")
        print()

    # 日別集計（直近14日）
    print("=== 直近14日の日別行数 ===\n")
    for symbol, _, _, _ in rows:
        print(f"  [{symbol}]")
        daily = conn.execute("""
            SELECT DATE(ts) as day, COUNT(*) as cnt
            FROM ohlcv_1m
            WHERE symbol = ?
            GROUP BY day
            ORDER BY day DESC
            LIMIT 14
        """, (symbol,)).fetchall()

        for day, cnt in daily:
            bar = "█" * (cnt // 50)
            print(f"    {day}  {cnt:>5}  {bar}")
        print()

    # 収集ログ（直近10件）
    print("=== 直近の収集ログ ===\n")
    logs = conn.execute("""
        SELECT symbol, started_at, status, rows_fetched, rows_inserted, error_msg
        FROM collection_log
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    for symbol, started, status, fetched, inserted, err in logs:
        mark = "✓" if status == "success" else "✗"
        err_str = f" ({err[:50]})" if err else ""
        print(f"  {mark} {started[:16]} [{symbol}] fetch={fetched} ins={inserted} {status}{err_str}")

    print()
    conn.close()


# ──────────────────────────────────────────────
# export: CSV エクスポート
# ──────────────────────────────────────────────
def cmd_export(args):
    conn = get_conn(args.db)

    query = "SELECT ts, open, high, low, close, volume FROM ohlcv_1m WHERE symbol = ?"
    params = [args.symbol]

    if args.start:
        query += " AND ts >= ?"
        params.append(args.start + "T00:00:00+00:00")
    if args.end:
        query += " AND ts <= ?"
        params.append(args.end + "T23:59:59+00:00")

    query += " ORDER BY ts"

    rows = conn.execute(query, params).fetchall()

    if not rows:
        print(f"データが見つかりません: {args.symbol}")
        return

    out_file = args.output or f"{args.symbol.replace('=', '_')}_{len(rows)}rows.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)

    print(f"エクスポート完了: {out_file} ({len(rows):,} 行)")
    conn.close()


# ──────────────────────────────────────────────
# gaps: 欠損区間の検出
# ──────────────────────────────────────────────
def cmd_gaps(args):
    """取引時間中のデータ欠損を検出する"""
    conn = get_conn(args.db)

    rows = conn.execute("""
        SELECT ts FROM ohlcv_1m
        WHERE symbol = ?
        ORDER BY ts
    """, (args.symbol,)).fetchall()

    if len(rows) < 2:
        print("データ不足です。")
        return

    print(f"\n=== {args.symbol} 欠損区間（{args.gap_min}分以上のギャップ） ===\n")

    threshold = timedelta(minutes=args.gap_min)
    gap_count = 0

    for i in range(1, len(rows)):
        t1 = datetime.fromisoformat(rows[i - 1][0].replace("+00:00", "+00:00"))
        t2 = datetime.fromisoformat(rows[i][0].replace("+00:00", "+00:00"))
        diff = t2 - t1

        if diff > threshold:
            gap_count += 1
            gap_min = diff.total_seconds() / 60
            # 週末（金→日）は正常なギャップ
            weekday1 = t1.weekday()
            tag = " [週末]" if weekday1 == 4 and gap_min > 1000 else ""
            print(f"  {t1:%Y-%m-%d %H:%M} → {t2:%Y-%m-%d %H:%M}  ({gap_min:.0f}分){tag}")

    print(f"\n  合計 {gap_count} 箇所のギャップ")
    conn.close()


# ──────────────────────────────────────────────
# spectrum: 簡易スペクトル分析（プロトタイプ）
# ──────────────────────────────────────────────
def cmd_spectrum(args):
    """
    HPF + FFT による簡易スペクトル分析のプロトタイプ。
    指定日の1分足データから高周波成分のパワースペクトルを計算する。
    """
    conn = get_conn(args.db)

    start_ts = args.date + "T00:00:00+00:00"
    end_ts = args.date + "T23:59:59+00:00"

    rows = conn.execute("""
        SELECT ts, close, volume
        FROM ohlcv_1m
        WHERE symbol = ? AND ts >= ? AND ts <= ?
        ORDER BY ts
    """, (args.symbol, start_ts, end_ts)).fetchall()

    if len(rows) < 60:
        print(f"データ不足: {len(rows)} 行（最低60行必要）")
        return

    prices = np.array([r[1] for r in rows if r[1] is not None], dtype=np.float64)
    volumes = np.array([r[2] for r in rows if r[2] is not None], dtype=np.float64)

    if len(prices) < 60:
        print("有効なデータ不足")
        return

    print(f"\n=== {args.symbol} {args.date} 簡易スペクトル分析 ===")
    print(f"  データ点数: {len(prices)}")
    print(f"  価格範囲:  {prices.min():.2f} ~ {prices.max():.2f}")
    print(f"  出来高合計: {volumes.sum():,.0f}")

    # ──────────────────────────────
    # 1. 移動平均でトレンド除去（HPF相当）
    # ──────────────────────────────
    window = min(args.window, len(prices) // 4)
    trend = np.convolve(prices, np.ones(window) / window, mode="same")

    # 端点処理（畳み込みの端は不正確なので除去）
    margin = window // 2
    detrended = (prices - trend)[margin:-margin]

    # 出来高で規格化（出来高=0のときは1で代替）
    vol_smooth = np.convolve(volumes, np.ones(window) / window, mode="same")[margin:-margin]
    vol_smooth = np.where(vol_smooth > 0, vol_smooth, 1.0)
    normalized = detrended / np.sqrt(vol_smooth)  # √volume で規格化

    print(f"\n  [HPF] 窓幅={window}分, 有効データ点={len(detrended)}")
    print(f"  高周波成分 σ = {np.std(detrended):.4f}")
    print(f"  規格化後   σ = {np.std(normalized):.4f}")

    # ──────────────────────────────
    # 2. FFT でパワースペクトル
    # ──────────────────────────────
    N = len(normalized)
    fft_vals = np.fft.rfft(normalized * np.hanning(N))
    psd = np.abs(fft_vals) ** 2
    freqs = np.fft.rfftfreq(N, d=1.0)  # 単位: cycles/minute

    # 主要ピーク（DC除く）
    psd_no_dc = psd[1:]
    freqs_no_dc = freqs[1:]
    peak_indices = np.argsort(psd_no_dc)[-5:][::-1]

    print(f"\n  [FFT] パワースペクトル 上位5ピーク:")
    print(f"  {'周波数 (cycles/min)':>22}  {'周期 (分)':>10}  {'パワー':>12}")
    print(f"  {'─' * 22}  {'─' * 10}  {'─' * 12}")

    for i in peak_indices:
        freq = freqs_no_dc[i]
        period = 1.0 / freq if freq > 0 else float("inf")
        power = psd_no_dc[i]
        print(f"  {freq:>22.6f}  {period:>10.1f}  {power:>12.2f}")

    # ──────────────────────────────
    # 3. 時間窓ごとのボラティリティ（エンベロープ）
    # ──────────────────────────────
    env_window = 30  # 30分窓
    if len(detrended) >= env_window * 2:
        print(f"\n  [エンベロープ] {env_window}分窓 の高周波振幅推移:")
        n_segments = len(detrended) // env_window
        for i in range(min(n_segments, 20)):  # 最大20セグメント表示
            seg = detrended[i * env_window : (i + 1) * env_window]
            amp = np.std(seg)
            ts_approx = rows[margin + i * env_window][0][11:16]
            bar = "█" * int(amp * 1000)
            print(f"    {ts_approx}  σ={amp:.4f}  {bar}")

    print()
    conn.close()


# ──────────────────────────────────────────────
# CLI エントリポイント
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="蓄積データの確認・分析ツール")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)

    sub = parser.add_subparsers(dest="command", required=True)

    # stats
    sub.add_parser("stats", help="蓄積状況を表示")

    # export
    p_export = sub.add_parser("export", help="CSV エクスポート")
    p_export.add_argument("--symbol", required=True)
    p_export.add_argument("--start", help="開始日 (YYYY-MM-DD)")
    p_export.add_argument("--end", help="終了日 (YYYY-MM-DD)")
    p_export.add_argument("--output", "-o", help="出力ファイル名")

    # gaps
    p_gaps = sub.add_parser("gaps", help="欠損区間を検出")
    p_gaps.add_argument("--symbol", required=True)
    p_gaps.add_argument("--gap-min", type=int, default=5, help="ギャップ閾値（分）")

    # spectrum
    p_spec = sub.add_parser("spectrum", help="簡易スペクトル分析")
    p_spec.add_argument("--symbol", required=True)
    p_spec.add_argument("--date", required=True, help="分析対象日 (YYYY-MM-DD)")
    p_spec.add_argument("--window", type=int, default=60, help="HPF 窓幅（分）")

    args = parser.parse_args()

    commands = {
        "stats": cmd_stats,
        "export": cmd_export,
        "gaps": cmd_gaps,
        "spectrum": cmd_spectrum,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
