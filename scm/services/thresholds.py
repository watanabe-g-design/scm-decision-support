"""
閾値・判定基準の設定
===================
将来的に顧客ごと・商材ごとにカスタマイズできる前提で定数化。
画面ごとに使う閾値を明示的に分離する。
"""

# ══════════════════════════════════════════════════════
# A. フォーキャスト・発注リスク画面
# ══════════════════════════════════════════════════════
# 「発注必要日」= フォーキャスト使用予定日 - 標準リードタイム
# 「残り日数」= 発注必要日 - 今日
FORECAST_RISK = {
    "critical_days": 3,     # 発注必要日まで 3 日以内 or 過ぎている
    "high_days":     7,     # 発注必要日まで 7 日以内
    "medium_days":   14,    # 発注必要日まで 14 日以内
    # Normal: それ以外
}

# ══════════════════════════════════════════════════════
# B. 受注・納品リスク画面
# ══════════════════════════════════════════════════════
# 指定納期 (requested_delivery_date) と回答納期/デッドラインの差分で判定
ORDER_DELIVERY_RISK = {
    # Critical: 指定納期まで 7 日以内 かつ 回答納期-デッドライン差 3 日以内
    "critical_delivery_days": 7,
    "critical_response_deadline_diff": 3,
    # High: 指定納期まで 30 日以内 かつ 回答納期-デッドライン差 7 日以内
    "high_delivery_days": 30,
    "high_response_deadline_diff": 7,
    # Medium: 指定納期まで 60 日以内 or 回答納期 > 指定納期
    "medium_delivery_days": 60,
    # Normal: 上記以外
}

# ══════════════════════════════════════════════════════
# C. 在庫ステータス
# ══════════════════════════════════════════════════════
INVENTORY_STATUS = {
    # Shortage: 利用可能在庫 < 0
    # Low: 利用可能在庫 < safety_threshold (品目の min_stock を使う)
    # Available: それ以外
    "use_min_stock_as_safety": True,  # min_stock を安全在庫しきい値として使う
    "default_safety_qty": 100,        # min_stock が無い場合のデフォルト
}

# ══════════════════════════════════════════════════════
# D. 月次需給バランス
# ══════════════════════════════════════════════════════
MONTHLY_BALANCE = {
    "shortage_highlight_color": "#ff4646",    # 不足月の強調色
    "surplus_highlight_color":  "#2ea043",    # 余剰月の色
    "neutral_color":            "#58a6ff",    # 均衡の色
}

# ══════════════════════════════════════════════════════
# 共通: デモ基準日 (config.py の get_as_of_date と整合させる)
# ══════════════════════════════════════════════════════
from services.config import get_as_of_date as _get_as_of_date
TODAY = _get_as_of_date().isoformat()
