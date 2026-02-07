#!/bin/bash
# setup.sh - Raspberry Pi 用セットアップスクリプト
# =============================================
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh
#
# 実行すること:
#   1. Python 依存パッケージのインストール
#   2. 動作テスト
#   3. cron 登録（対話的に確認）

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COLLECTOR="$SCRIPT_DIR/collector.py"
PYTHON=$(command -v python3)

echo "=========================================="
echo " 金・銀1分足データ収集 セットアップ"
echo "=========================================="
echo ""
echo "  スクリプトディレクトリ: $SCRIPT_DIR"
echo "  Python: $PYTHON"
echo ""

# ──────────────────────────────────────────────
# 1. Python 依存パッケージ
# ──────────────────────────────────────────────
echo "── 1. 依存パッケージの確認 ──"

# Raspberry Pi OS の場合 --break-system-packages が必要な場合がある
PIP_ARGS=""
if $PYTHON -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
    PIP_ARGS="--break-system-packages"
fi

for pkg in yfinance numpy; do
    if $PYTHON -c "import $pkg" 2>/dev/null; then
        echo "  ✓ $pkg は既にインストール済み"
    else
        echo "  → $pkg をインストール中..."
        $PYTHON -m pip install $pkg $PIP_ARGS
    fi
done

echo ""

# ──────────────────────────────────────────────
# 2. 動作テスト
# ──────────────────────────────────────────────
echo "── 2. 動作テスト ──"
echo "  collector.py を 1日分で実行テスト..."

if $PYTHON "$COLLECTOR" --days 1; then
    echo "  ✓ テスト成功"
else
    echo "  ✗ テスト失敗（ネットワーク接続を確認してください）"
    echo "    ※ 市場が閉まっている時間帯はデータが少ない場合があります"
fi

echo ""

# ──────────────────────────────────────────────
# 3. cron 登録
# ──────────────────────────────────────────────
echo "── 3. cron 設定 ──"
echo ""
echo "  推奨スケジュール:"
echo "    A) 30分ごと（推奨）: リアルタイム性とサーバー負荷のバランス"
echo "    B) 1時間ごと       : 控えめな更新頻度"
echo "    C) 6時間ごと       : 最小限の更新（長期データ蓄積用）"
echo ""

# cron エントリの定義（データ取得＋チャート更新）
PLOT_SCRIPT="$SCRIPT_DIR/plot_timeseries.py"
CRON_30MIN="*/30 * * * * cd $SCRIPT_DIR && $PYTHON $COLLECTOR && $PYTHON $PLOT_SCRIPT >> /dev/null 2>&1"
CRON_1H="0 * * * * cd $SCRIPT_DIR && $PYTHON $COLLECTOR && $PYTHON $PLOT_SCRIPT >> /dev/null 2>&1"
CRON_6H="0 */6 * * * cd $SCRIPT_DIR && $PYTHON $COLLECTOR && $PYTHON $PLOT_SCRIPT >> /dev/null 2>&1"

read -p "  cron に登録しますか？ [A/B/C/n] " choice

case "$choice" in
    A|a)
        CRON_ENTRY="$CRON_30MIN"
        SCHEDULE_DESC="30分ごと"
        ;;
    B|b)
        CRON_ENTRY="$CRON_1H"
        SCHEDULE_DESC="1時間ごと"
        ;;
    C|c)
        CRON_ENTRY="$CRON_6H"
        SCHEDULE_DESC="6時間ごと"
        ;;
    *)
        echo "  → cron 登録をスキップしました"
        echo ""
        echo "  手動で登録する場合:"
        echo "    crontab -e"
        echo "    # 以下を追加:"
        echo "    $CRON_30MIN"
        echo ""
        exit 0
        ;;
esac

# 既存の cron エントリと重複チェック
if crontab -l 2>/dev/null | grep -q "collector.py"; then
    echo "  ⚠ 既に collector.py の cron エントリが存在します"
    echo "  既存のエントリ:"
    crontab -l | grep "collector.py"
    echo ""
    read -p "  上書きしますか？ [y/N] " overwrite
    if [[ "$overwrite" != "y" && "$overwrite" != "Y" ]]; then
        echo "  → スキップしました"
        exit 0
    fi
    # 既存のエントリを除去
    crontab -l | grep -v "collector.py" | crontab -
fi

# cron に追加
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo "  ✓ cron に登録しました（$SCHEDULE_DESC）"
echo ""

# ──────────────────────────────────────────────
# 完了
# ──────────────────────────────────────────────
echo "=========================================="
echo " セットアップ完了"
echo "=========================================="
echo ""
echo "  DB ファイル: $SCRIPT_DIR/precious_metals.db"
echo "  ログファイル: $SCRIPT_DIR/collector.log"
echo ""
echo "  確認コマンド:"
echo "    $PYTHON $SCRIPT_DIR/query_data.py stats"
echo ""
echo "  CSV エクスポート:"
echo "    $PYTHON $SCRIPT_DIR/query_data.py export --symbol GC=F"
echo ""
echo "  簡易スペクトル分析:"
echo "    $PYTHON $SCRIPT_DIR/query_data.py spectrum --symbol GC=F --date 2026-02-04"
echo ""
echo "  cron 確認:"
echo "    crontab -l"
echo ""
