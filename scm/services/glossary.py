"""
業務用語辞書
============
本Appで使用するすべての業務用語・状態名・列名を一元管理する。

設計方針:
  - 画面表示・CSV項目・Genie応答で同じ語彙を使う
  - 内部キー（英語/技術用語）→ 業務日本語表記 を統一マッピング
  - 用語の定義はツールチップで参照可能にする
"""
from __future__ import annotations


# ════════════════════════════════════════════════════════
# 1. 業務用語の定義（ツールチップ・凡例で参照）
# ════════════════════════════════════════════════════════
TERM_DEFINITIONS: dict[str, dict[str, str]] = {
    "受注残": {
        "definition": "顧客から受注したが、まだ出荷していない数量。マクニカ視点では『顧客に対する未出荷義務』。",
        "source":     "silver_sales_orders.remaining_qty",
    },
    "発注残BL": {
        "definition": "マクニカからメーカーへ発注したが、まだ入荷していない数量。BL = Back Log。"
                       "入荷予定日(expected_delivery_date)が将来で、未入荷分の合計。",
        "source":     "silver_purchase_orders.outstanding_qty",
    },
    "フリー在庫": {
        "definition": "マクニカが特定顧客向けに事前に引当済の在庫。当該顧客は通常LTを待たず引当可能。"
                       "顧客側の自社倉庫在庫とは区別する。",
        "source":     "silver_macnica_free_inventory.qty_available",
    },
    "顧客在庫": {
        "definition": "顧客自身が自社倉庫で保有する在庫。即時利用可能だが、他需要での消費見込みを差し引いた実効在庫で判断する。",
        "source":     "silver_inventory_current.stock_qty",
    },
    "安全在庫": {
        "definition": "部材ごとに定められた、これを下回ると欠品リスクがある最低在庫量。",
        "source":     "silver_components.min_stock",
    },
    "希望納期": {
        "definition": "需要側が指定する、製品/部材が必要な日付。",
        "source":     "silver_demand_plan_components.requested_date",
    },
    "リードタイム": {
        "definition": "部材発注から入荷までの期間。LT = Lead Time。週単位で管理。",
        "source":     "silver_components.base_lead_time_weeks",
    },
    "残日数": {
        "definition": "希望納期までの残り日数。負値は納期超過を意味する。",
        "source":     "計算項目 (requested_date - today)",
    },
    "BOM": {
        "definition": "Bill of Materials。1製品を作るために必要な部材と数量の一覧。",
        "source":     "silver_bom",
    },
    "充足率": {
        "definition": "BOM上の全部材に対し、4ルート組合せで需要を充足できる部材の割合。",
        "source":     "gold_bom_fulfillment_status.fulfillment_rate",
    },
    "前倒し": {
        "definition": "本来の納期より早く調達・発注すること。市場品購入や他月需要の前倒し充当を含む。",
        "source":     "—",
    },
}


# ════════════════════════════════════════════════════════
# 2. ルート種別の表示名
# ════════════════════════════════════════════════════════
ROUTE_LABEL = {
    "CUSTOMER_STOCK": "顧客在庫",
    "MACNICA_FREE":   "フリー在庫",
    "EXISTING_PO":    "発注残BL",
    "NEW_ORDER":      "新規発注",
}


# ════════════════════════════════════════════════════════
# 3. 対応レベルの業務用語表現
# ════════════════════════════════════════════════════════
ACTION_LEVEL_LABEL = {
    "不要": "OK",
    "軽":   "Medium",
    "中":   "High",
    "重":   "Critical",
}

ACTION_LEVEL_ICON = {
    "不要": "🟢",
    "軽":   "🟡",
    "中":   "🟠",
    "重":   "🔴",
}

# アクションレベルの業務的説明 (ダッシュボードKPIのhelp文に使用)
ACTION_LEVEL_HELP = {
    "重":   "今すぐ新規発注が必要。LTを考慮すると納期に間に合わない可能性が高く、最優先で対応してください。",
    "中":   "今週中にマクニカへ相談が必要。既存在庫ルートの組み合わせで対応可能ですが、確認が必要です。",
    "軽":   "来週までに確認。既存のPO催促またはマクニカフリー在庫で単独対応可能。",
    "不要": "顧客在庫で充足。現状このまま進めてOKです。",
}


# ════════════════════════════════════════════════════════
# 4. 需要発生源の業務用語
# ════════════════════════════════════════════════════════
SOURCE_LABEL = {
    "FCST_AUTO":        "営業FCSTから自動展開",
    "EMERGENCY_MANUAL": "緊急手動入力",
}


# ════════════════════════════════════════════════════════
# 5. 列名（内部↔画面表示↔CSV）の統一マッピング
# ════════════════════════════════════════════════════════
COLUMN_LABEL: dict[str, str] = {
    # 共通
    "demand_id":           "需要ID",
    "component_id":        "部材ID",
    "part_number":         "品番",
    "component_name":      "部材名",
    "component_category":  "部材カテゴリ",
    "product_id":          "製品ID",
    "product_name":        "製品名",
    "product_category":    "製品カテゴリ",
    "supplier_name":       "メーカー名",
    "supplier_id":         "メーカーID",
    "warehouse_id":        "倉庫ID",
    "warehouse_name":      "倉庫名",
    "prefecture":          "都道府県",
    "customer_id":         "顧客ID",
    "customer_name":       "顧客名",
    # 日付/期間
    "requested_date":      "希望納期",
    "order_date":          "発注日",
    "expected_delivery_date": "入荷予定日",
    "actual_arrival_date":    "実到着日",
    "earliest_eta":        "最早入荷日",
    "as_of_date":          "基準日",
    "created_at":          "作成日",
    "requested_month":     "対象月",
    "forecast_month":      "予測月",
    # 数量
    "requested_qty":       "必要数",
    "available_qty":       "確保可能数",
    "shortage_qty":        "不足数",
    "stock_qty":           "在庫数",
    "qty_available":       "引当可能数",
    "outstanding_qty":     "発注残数",
    "order_qty":           "受注数",
    "shipped_qty":         "出荷済数",
    "remaining_qty":       "受注残数",
    "quantity":            "数量",
    "received_qty":        "入荷済数",
    "min_stock":           "安全在庫",
    "max_stock":           "上限在庫",
    "min_order_qty":       "最低発注ロット",
    "partial_available_qty": "分納可能数",
    # 日数
    "days_to_due":         "残日数",
    "days_late":           "遅延日数",
    "days_to_required":    "希望納期まで",
    "days_to_expected":    "入荷予定まで",
    "delay_days":          "遅延日数",
    "base_lead_time_weeks": "リードタイム(週)",
    # フラグ/ステータス
    "source_type":         "需要発生源",
    "route_type":          "調達ルート",
    "confidence":          "確実度",
    "action_level":        "対応レベル",
    "needs_action":        "要対応",
    "is_in_time":          "納期内可否",
    "is_emergency":        "緊急フラグ",
    "is_delayed":          "遅延フラグ",
    "is_overdue":          "納期超過",
    "status":              "ステータス",
    "production_status":   "生産可否",
    "fulfillment_rate":    "充足率",
    # その他
    "note":                "備考",
    "unit_price_usd":      "単価(USD)",
    "unit_price_jpy":      "単価(円)",
    "total_components":    "BOM部材数",
    "shortage_components": "不足部材数",
    "fulfillable_components": "充足可能部材数",
    "delay_cause":         "遅延原因",
}


# ════════════════════════════════════════════════════════
# 6. ヘルパー関数
# ════════════════════════════════════════════════════════
def rename_columns(df, extra: dict[str, str] | None = None):
    """DataFrameの列名を業務用語表示名に置換して返す"""
    mapping = dict(COLUMN_LABEL)
    if extra:
        mapping.update(extra)
    cols_to_rename = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=cols_to_rename)


def route_label_jp(route_type: str) -> str:
    """ルート種別の業務用語表示名"""
    return ROUTE_LABEL.get(route_type, route_type)


def action_level_label_jp(level: str) -> str:
    """対応レベルの表示 (アイコン + Critical/High/Medium/OK)"""
    icon = ACTION_LEVEL_ICON.get(level, "")
    label = ACTION_LEVEL_LABEL.get(level, level)
    return f"{icon} {label}"


def action_level_help(level: str) -> str:
    """対応レベルの業務的説明文"""
    return ACTION_LEVEL_HELP.get(level, "")


def source_label_jp(source: str) -> str:
    """需要発生源の業務用語表示"""
    return SOURCE_LABEL.get(source, source)


def render_glossary(st_module) -> None:
    """Streamlitに用語集を展開可能なExpanderで描画"""
    with st_module.expander("📖 業務用語集（クリックで展開）", expanded=False):
        for term, info in TERM_DEFINITIONS.items():
            st_module.markdown(
                f"**{term}**: {info['definition']}<br>"
                f"<span style='font-size:10px;opacity:0.7;'>データソース: <code>{info['source']}</code></span>",
                unsafe_allow_html=True,
            )
            st_module.markdown("")
