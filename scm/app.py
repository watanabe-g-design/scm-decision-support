"""
📊 総合ダッシュボード
======================
本Appのトップページ。今日のサマリーと優先対応事項を一画面で把握する。

業務上の役割:
  - 顧客（購買担当）が朝一に開いて状況を把握
  - 「今日対応すべき部材は何件か？」を即座に確認
  - 期間フィルターで「いつまで」の話かを切替可能
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.inventory_badge import render_inventory_legend
from services.config import get_as_of_date
from services.database import (
    get_silver_demand_plan_components,
    get_silver_macnica_free_inventory,
    get_silver_inventory_current,
    get_procurement_options,
    get_silver_components,
)

st.set_page_config(page_title="総合ダッシュボード | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="顧客 CUS001（ネクサス精機）向けデモ")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    free_inv   = get_silver_macnica_free_inventory()
    cust_inv   = get_silver_inventory_current()
    options    = get_procurement_options()
    components = get_silver_components()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 📊 総合ダッシュボード")
st.caption("今日の調達状況サマリーと優先対応事項")

# ────────────────────────────────────────────────────────
# 期間フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 期間フィルター")
period_options = {
    "今月（〜30日）":      30,
    "3ヶ月以内":          90,
    "6ヶ月以内":         180,
    "全期間":           9999,
}
sel_period = st.radio(
    "ダッシュボードに集計する需要の期間",
    options=list(period_options.keys()),
    index=1,
    horizontal=True,
)
period_days = period_options[sel_period]
end_date = today + pd.Timedelta(days=period_days).to_pytimedelta()

# 期間内の需要に絞る
if not demand.empty:
    demand_filtered = demand.copy()
    demand_filtered["requested_date"] = pd.to_datetime(demand_filtered["requested_date"], errors="coerce").dt.date
    demand_filtered = demand_filtered[
        (demand_filtered["requested_date"] >= today - pd.Timedelta(days=30).to_pytimedelta())
        & (demand_filtered["requested_date"] <= end_date)
    ]
else:
    demand_filtered = demand

st.markdown("---")

# ────────────────────────────────────────────────────────
# KPI Row 1: 需要関連
# ────────────────────────────────────────────────────────
demand_total = len(demand_filtered)
demand_emergency = int((demand_filtered["source_type"] == "EMERGENCY_MANUAL").sum()) if not demand_filtered.empty else 0

if not demand_filtered.empty:
    req_dates = pd.to_datetime(demand_filtered["requested_date"], errors="coerce").dt.date
    upcoming_30d = int(((req_dates >= today) & (req_dates <= today + pd.Timedelta(days=30).to_pytimedelta())).sum())
    overdue = int((req_dates < today).sum())
else:
    upcoming_30d = 0
    overdue = 0

st.markdown(f"### 📋 需要サマリー（{sel_period}）")
k1, k2, k3, k4 = st.columns(4)
k1.metric("対象期間の需要件数", f"{demand_total:,} 件",
          help=f"希望納期が {today.isoformat()} 〜 {end_date.isoformat()} の範囲にある需要")
k2.metric("🚨 緊急手動入力", f"{demand_emergency:,} 件",
          help="営業FCSTから漏れた突発的な需要 (EMERGENCY_MANUAL)")
k3.metric("⏰ 30日以内に納期到来", f"{upcoming_30d:,} 件")
k4.metric("🔴 既に納期超過", f"{overdue:,} 件")

st.markdown("---")

# ────────────────────────────────────────────────────────
# KPI Row 2: 在庫サマリー
# ────────────────────────────────────────────────────────
st.markdown("### 📦 在庫サマリー")
render_inventory_legend()
st.markdown("")

cust_total_qty = int(pd.to_numeric(cust_inv["stock_qty"], errors="coerce").fillna(0).sum()) if not cust_inv.empty else 0
cust_components = int(cust_inv["component_id"].nunique()) if not cust_inv.empty else 0
free_total_qty = int(pd.to_numeric(free_inv["qty_available"], errors="coerce").fillna(0).sum()) if not free_inv.empty else 0
free_components = int(free_inv["component_id"].nunique()) if not free_inv.empty else 0

i1, i2, i3, i4 = st.columns(4)
i1.metric("🏭 顧客在庫 数量合計", f"{cust_total_qty:,} 個")
i2.metric("🏭 顧客在庫 部材数", f"{cust_components} 品目")
i3.metric("📦 マクニカフリー在庫 数量", f"{free_total_qty:,} 個")
i4.metric("📦 マクニカフリー在庫 部材数", f"{free_components} 品目")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 要対応リスト（needs_action フラグを使用）
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 要対応リスト（顧客在庫だけで賄えない需要 Top 10）")
st.caption(
    "顧客在庫のみで充足できる需要は除外し、マクニカへの相談・既存PO催促・新規発注のいずれかが必要なものを抽出。"
    "action_level: 軽=単一ルートで対応可 / 中=複数ルート組合せ必要 / 重=新規発注LT必須"
)

if not options.empty and not demand_filtered.empty:
    opt = options.copy()
    opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date
    opt = opt[(opt["requested_date"] >= today - pd.Timedelta(days=30).to_pytimedelta()) & (opt["requested_date"] <= end_date)]

    # 需要ごとに最良ルートを選定
    opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0)
    opt["days_late"] = pd.to_numeric(opt["days_late"], errors="coerce").fillna(0)
    opt["_score"] = opt["shortage_qty"].clip(lower=0) * 1000 + opt["days_late"]
    best_route = opt.sort_values("_score").groupby("demand_id").first().reset_index()

    # 「needs_action == True」のみを要対応とする (顧客在庫単独で済むものは除外)
    if "needs_action" in best_route.columns:
        needs = best_route[best_route["needs_action"] == True].copy()  # noqa: E712
    else:
        needs = best_route[(best_route["shortage_qty"] > 0) | (best_route["days_late"] > 0)].copy()

    # action_level 順で並び替え (重 → 中 → 軽)
    if "action_level" in needs.columns:
        level_order = {"重": 0, "中": 1, "軽": 2, "不要": 3}
        needs["_lv"] = needs["action_level"].map(level_order).fillna(9)
        needs = needs.sort_values(["_lv", "shortage_qty", "days_late"], ascending=[True, False, False])

    if needs.empty:
        st.success("✅ 顧客在庫だけで全需要が賄えます。マクニカへの追加調達相談は不要です。")
    else:
        if not components.empty:
            comp_lite = components[["component_id", "part_number", "component_name"]]
            needs = needs.merge(comp_lite, on="component_id", how="left")

        cols_map = [
            ("demand_id",      "需要ID"),
            ("part_number",    "品番"),
            ("component_name", "部材名"),
            ("requested_date", "希望納期"),
            ("requested_qty",  "必要数"),
            ("action_level",   "対応レベル"),
            ("route_type",     "推奨最良ルート"),
            ("available_qty",  "確保可能数"),
            ("shortage_qty",   "不足数"),
            ("days_late",      "遅延日数"),
        ]
        cols_present = [(k, lbl) for k, lbl in cols_map if k in needs.columns]
        df_show = needs[[k for k, _ in cols_present]].head(10).rename(columns=dict(cols_present))
        st.dataframe(df_show, hide_index=True, use_container_width=True)
        st.caption(f"💡 詳細評価は「🎯 調達アクションセンター」または「🚨 緊急調達シミュレーター」へ。要対応 {len(needs)} 件中、上位10件を表示。")
else:
    st.info("Goldテーブル `gold_procurement_options` がまだ生成されていません。Lakeflow パイプラインを実行してください。")
