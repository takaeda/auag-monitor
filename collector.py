#!/usr/bin/env python3
"""
precious_metals_collector - 金・銀の1分足データを yfinance から取得し SQLite に蓄積する
====================================================================================

cron で定期実行し、ローカルに分足データベースを構築する。
yfinance の1分足は直近7日間のみ取得可能なため、毎日（または数時間おきに）
実行することでデータを連続的に蓄積していく。

Usage:
    python3 collector.py                    # デフォルト設定で実行
    python3 collector.py --db /path/to.db   # DB パスを指定
    python3 collector.py --symbols GC=F     # 金のみ取得
    python3 collector.py --days 3           # 直近3日分のみ

Target environment: Raspberry Pi + cron
"""

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance が見つかりません。 pip install yfinance を実行してください。")
    sys.exit(1)


# ──────────────────────────────────────────────
# デフォルト設定
# ──────────────────────────────────────────────
DEFAULT_DB_PATH = Path(__file__).parent / "precious_metals.db"
DEFAULT_SYMBOLS = {
    "GC=F": "Gold Futures (XAU/USD相当)",
    "SI=F": "Silver Futures (XAG/USD相当)",
}
DEFAULT_PERIOD_DAYS = 7  # yfinance 1分足の最大取得範囲
RETRY_COUNT = 3
RETRY_DELAY_SEC = 30
OVERLAP_MINUTES = 30  # 差分取得時の重複バッファ（欠損補完用）
MAX_REFETCH_HOURS = 24  # volume=0検知による再取得の最大遡り時間


# ──────────────────────────────────────────────
# ロギング設定
# ──────────────────────────────────────────────
def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "collector.log"

    logger = logging.getLogger("collector")
    logger.setLevel(logging.DEBUG)

    # ファイルハンドラ（詳細ログ）
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))

    # コンソールハンドラ（簡易ログ）
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%H:%M:%S"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ──────────────────────────────────────────────
# データベース初期化
# ──────────────────────────────────────────────
def init_db(db_path: Path) -> sqlite3.Connection:
    """SQLite DB を初期化し、テーブルを作成する"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))

    conn.executescript("""
        -- 1分足 OHLCV データ
        CREATE TABLE IF NOT EXISTS ohlcv_1m (
            symbol      TEXT    NOT NULL,
            ts          TEXT    NOT NULL,   -- ISO 8601 UTC タイムスタンプ
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            collected_at TEXT   NOT NULL,    -- 取得日時（デバッグ用）
            PRIMARY KEY (symbol, ts)
        );

        -- 取得ログ
        CREATE TABLE IF NOT EXISTS collection_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT    NOT NULL,
            started_at  TEXT    NOT NULL,
            finished_at TEXT,
            rows_fetched   INTEGER DEFAULT 0,
            rows_inserted  INTEGER DEFAULT 0,
            status      TEXT    DEFAULT 'running',
            error_msg   TEXT
        );

        -- ts でのレンジクエリを高速化
        CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_ts ON ohlcv_1m(symbol, ts);
    """)

    conn.execute("PRAGMA journal_mode=WAL")  # 同時読み取りに強い
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ──────────────────────────────────────────────
# データ取得・格納
# ──────────────────────────────────────────────
def fetch_and_store(
    conn: sqlite3.Connection,
    symbol: str,
    period_days: int,
    logger: logging.Logger,
    force_full: bool = False,
) -> dict:
    """
    yfinance から1分足データを取得し、SQLite に UPSERT する。
    戻り値: {"fetched": int, "inserted": int}
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    # 取得ログ開始
    cursor = conn.execute(
        "INSERT INTO collection_log (symbol, started_at) VALUES (?, ?)",
        (symbol, now_utc),
    )
    log_id = cursor.lastrowid
    conn.commit()

    period = f"{period_days}d"
    result = {"fetched": 0, "inserted": 0}

    try:
        # ── DB 内の最新タイムスタンプを確認し、取得範囲を最適化 ──
        row = conn.execute(
            "SELECT MAX(ts) FROM ohlcv_1m WHERE symbol = ?", (symbol,)
        ).fetchone()
        latest_in_db = row[0] if row and row[0] else None

        now = datetime.now(timezone.utc)
        max_lookback = now - timedelta(days=7)  # yfinance 1分足の絶対上限

        use_start = None  # None → period パラメータを使用
        if force_full:
            logger.info(f"[{symbol}] --force-full → 全期間 ({period}) 取得")
        elif latest_in_db:
            try:
                # DB の ts は "2026-01-27T10:00:00+00:00" 形式
                db_latest_dt = datetime.fromisoformat(latest_in_db)
                if db_latest_dt.tzinfo is None:
                    db_latest_dt = db_latest_dt.replace(tzinfo=timezone.utc)

                # 7日以内のデータがあれば差分取得に切り替え
                if db_latest_dt > max_lookback:
                    # volume=0 の最古時刻を検索（24時間以内かつ7日制限内）
                    refetch_limit = now - timedelta(hours=MAX_REFETCH_HOURS)
                    search_from = max(refetch_limit, max_lookback)
                    oldest_zero_vol = conn.execute(
                        """SELECT MIN(ts) FROM ohlcv_1m
                           WHERE symbol = ? AND volume = 0 AND ts > ?""",
                        (symbol, search_from.isoformat())
                    ).fetchone()

                    if oldest_zero_vol and oldest_zero_vol[0]:
                        # volume=0 が見つかった場合、その時刻から再取得
                        zero_vol_dt = datetime.fromisoformat(oldest_zero_vol[0])
                        if zero_vol_dt.tzinfo is None:
                            zero_vol_dt = zero_vol_dt.replace(tzinfo=timezone.utc)
                        use_start = zero_vol_dt
                        logger.info(
                            f"[{symbol}] volume=0検知: {oldest_zero_vol[0][:19]} UTC から再取得"
                        )
                    else:
                        # volume=0 がなければ従来通りDB最新からOVERLAP分遡る
                        use_start = db_latest_dt - timedelta(minutes=OVERLAP_MINUTES)
                        gap_hours = (now - db_latest_dt).total_seconds() / 3600
                        logger.info(
                            f"[{symbol}] DB最新: {latest_in_db[:19]} UTC "
                            f"({gap_hours:.1f}h前) → {OVERLAP_MINUTES}分バッファ付き差分取得"
                        )
                else:
                    logger.info(
                        f"[{symbol}] DB最新が7日以上前 → 全期間 ({period}) 取得"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(f"[{symbol}] DB タイムスタンプ解析失敗: {e} → 全期間取得")
        else:
            logger.info(f"[{symbol}] DB にデータなし → 全期間 ({period}) 取得")

        # yfinance でダウンロード（リトライ付き）
        dl_kwargs = dict(
            tickers=symbol,
            interval="1m",
            progress=False,
            auto_adjust=True,
            prepost=True,
        )
        if use_start:
            # yfinance に timezone-aware な datetime を渡す
            # end も明示的に指定してタイムゾーン解釈の問題を回避
            dl_kwargs["start"] = use_start
            dl_kwargs["end"] = datetime.now(timezone.utc)
        else:
            dl_kwargs["period"] = period

        logger.info(f"[{symbol}] 1分足データを取得中...")

        df = None
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                df = yf.download(**dl_kwargs)
                if df is not None and len(df) > 0:
                    break
            except Exception as e:
                logger.warning(f"[{symbol}] 取得試行 {attempt}/{RETRY_COUNT} 失敗: {e}")
                if attempt < RETRY_COUNT:
                    time.sleep(RETRY_DELAY_SEC)

        if df is None or len(df) == 0:
            raise RuntimeError(f"データ取得失敗（{RETRY_COUNT}回リトライ後）")

        result["fetched"] = len(df)
        fetch_mode = "差分" if use_start else "全期間"
        logger.info(f"[{symbol}] {len(df)} 行を取得 ({fetch_mode})")

        # MultiIndex カラムの処理（yfinance 0.2.x 対応）
        if isinstance(df.columns, __import__("pandas").MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # UPSERT: 新規行は INSERT、既存行は新データが非NULLなら上書き
        # （yfinance は直近データが不完全なことがあり、後の再取得で補完される）
        collected_at = datetime.now(timezone.utc).isoformat()
        inserted = 0
        updated = 0

        for idx, row in df.iterrows():
            # yfinance のタイムスタンプを確実に UTC へ変換してから格納
            if hasattr(idx, 'tz') and idx.tz is not None:
                idx_utc = idx.tz_convert('UTC')
            else:
                # timezone-naive の場合は UTC とみなす
                idx_utc = idx
            ts = idx_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00")

            v_open  = float(row["Open"])  if row["Open"]  == row["Open"]  else None
            v_high  = float(row["High"])  if row["High"]  == row["High"]  else None
            v_low   = float(row["Low"])   if row["Low"]   == row["Low"]   else None
            v_close = float(row["Close"]) if row["Close"] == row["Close"] else None
            v_vol   = int(row["Volume"])  if row["Volume"] == row["Volume"] else None

            try:
                cur = conn.execute(
                    """INSERT INTO ohlcv_1m (symbol, ts, open, high, low, close, volume, collected_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(symbol, ts) DO UPDATE SET
                           open   = COALESCE(excluded.open,   ohlcv_1m.open),
                           high   = COALESCE(excluded.high,   ohlcv_1m.high),
                           low    = COALESCE(excluded.low,    ohlcv_1m.low),
                           close  = COALESCE(excluded.close,  ohlcv_1m.close),
                           volume = COALESCE(excluded.volume, ohlcv_1m.volume),
                           collected_at = excluded.collected_at
                       """,
                    (symbol, ts, v_open, v_high, v_low, v_close, v_vol, collected_at),
                )
                # rowcount: 1=INSERT, 1=UPDATE (SQLite では区別困難だが changes() で追跡)
                inserted += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()
        result["inserted"] = inserted
        result["updated"] = updated
        logger.info(f"[{symbol}] {inserted} 行を DB に格納（UPSERT: 新規+補完上書き）")

        # 取得ログ更新
        conn.execute(
            """UPDATE collection_log
               SET finished_at=?, rows_fetched=?, rows_inserted=?, status='success'
               WHERE id=?""",
            (datetime.now(timezone.utc).isoformat(), result["fetched"], result["inserted"], log_id),
        )
        conn.commit()

    except Exception as e:
        logger.error(f"[{symbol}] エラー: {e}")
        conn.execute(
            """UPDATE collection_log
               SET finished_at=?, status='error', error_msg=?
               WHERE id=?""",
            (datetime.now(timezone.utc).isoformat(), str(e), log_id),
        )
        conn.commit()
        raise

    return result


# ──────────────────────────────────────────────
# DB 統計表示
# ──────────────────────────────────────────────
def print_db_stats(conn: sqlite3.Connection, logger: logging.Logger):
    """蓄積状況のサマリーを出力する"""
    logger.info("── DB 蓄積状況 ──")

    rows = conn.execute("""
        SELECT symbol,
               COUNT(*) as total_rows,
               MIN(ts) as earliest,
               MAX(ts) as latest
        FROM ohlcv_1m
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()

    if not rows:
        logger.info("  (データなし)")
        return

    for symbol, total, earliest, latest in rows:
        # DB サイズ概算
        logger.info(
            f"  {symbol}: {total:,} 行 | {earliest[:10]} ~ {latest[:10]}"
        )

    # DB ファイルサイズ
    db_size = conn.execute("SELECT page_count * page_size FROM pragma_page_count, pragma_page_size").fetchone()
    if db_size and db_size[0]:
        size_mb = db_size[0] / (1024 * 1024)
        logger.info(f"  DB サイズ: {size_mb:.1f} MB")


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="金・銀 1分足データを yfinance から収集し SQLite に蓄積する"
    )
    parser.add_argument(
        "--db", type=Path, default=DEFAULT_DB_PATH,
        help=f"SQLite DB ファイルパス (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--symbols", nargs="+", default=list(DEFAULT_SYMBOLS.keys()),
        help=f"取得シンボル (default: {' '.join(DEFAULT_SYMBOLS.keys())})",
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_PERIOD_DAYS, choices=range(1, 8),
        help="取得日数 1-7 (default: 7)",
    )
    parser.add_argument(
        "--log-dir", type=Path, default=None,
        help="ログディレクトリ (default: DB と同じディレクトリ)",
    )
    parser.add_argument(
        "--force-full", action="store_true",
        help="DB の既存データを無視し、常に全期間を取得する",
    )
    args = parser.parse_args()

    log_dir = args.log_dir or args.db.parent
    logger = setup_logging(log_dir)

    logger.info("=" * 50)
    logger.info("金・銀1分足データ収集 開始")
    logger.info(f"  DB: {args.db}")
    logger.info(f"  シンボル: {args.symbols}")
    logger.info(f"  取得期間: 直近{args.days}日")
    if args.force_full:
        logger.info(f"  モード: 全期間取得 (--force-full)")
    else:
        logger.info(f"  モード: 差分取得（DB最新から自動判定）")
    logger.info("=" * 50)

    conn = init_db(args.db)

    success_count = 0
    error_count = 0

    for symbol in args.symbols:
        try:
            # シンボル間に少し間隔を空ける（レートリミット対策）
            if success_count + error_count > 0:
                time.sleep(5)

            fetch_and_store(conn, symbol, args.days, logger, force_full=args.force_full)
            success_count += 1

        except Exception as e:
            logger.error(f"[{symbol}] 収集失敗: {e}")
            error_count += 1

    print_db_stats(conn, logger)
    conn.close()

    logger.info(f"完了: 成功={success_count}, 失敗={error_count}")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
