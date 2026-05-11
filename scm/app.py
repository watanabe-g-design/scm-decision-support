"""
📊 総合ダッシュボード
======================
本Appのトップページ。今日のサマリーと優先対応事項を一画面で把握する。

業務上の役割:
  - 顧客（購買担当）が朝一に開いて状況を把握
  - 「今日対応すべき部材は何件か？」を即座に確認
  - 期間フィルターで「いつまで」の話かを切替可能
  - 各KPIに「詳細を見る」ボタンで該当ページへ即遷移（ドリルダウン）
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
from components.drill_down import render_drill_down_button
from services.config import get_as_of_date
from services.database import (
    get_silver_demand_plan_components,
    get_silver_macnica_free_inventory,
    get_silver_inventory_current,
    get_procurement_options,
    get_silver_components,
)
from services.kpi_logic import (
    filter_by_period,
    kpi_demand_total, kpi_demand_emergency, kpi_demand_upcoming_30d, kpi_demand_overdue,
    kpi_customer_stock, kpi_macnica_free_stock,
    aggregate_best_route_per_demand, filter_needs_action, kpi_action_count_by_level,
)
from services.glossary import (
    rename_columns, route_label_jp, action_level_label_jp, source_label_jp,
    render_glossary,
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
# タイトル + 用語集
# ────────────────────────────────────────────────────────
st.markdown("## 📊 総合ダッシュボード")
st.caption("今日の調達状況サマリーと優先対応事項。各カード下の「詳細を見る」で該当ページへ遷移します。")
render_glossary(st)

# ────────────────────────────────────────────────────────
# 期間フィルター（タイトル明確化）
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 期間フィルター（ダッシュボードに集計する需要の対象期間）")
period_options = {
    "今月（〜30日先）":     30,
    "3ヶ月以内":           90,
    "6ヶ月以内":          180,
    "全期間":            9999,
}
sel_period = st.radio(
    "📅 表示する未来期間（今日からどこまで先の需要を集計するか）",
    options=list(period_options.keys()),
    index=1,
    horizontal=True,
    help="期間内に希望納期があるFCST/緊急需要のみ集計対象になります。",
)
period_days = period_options[sel_period]

# 期間内の需要に絞る（KPI計算と詳細でロジック共用）
demand_filtered = filter_by_period(demand, "requested_date", today, period_days)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 📋 需要サマリー（各KPIにドリルダウンボタン）
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 需要サマリー（対象: {sel_period}）")

demand_total      = kpi_demand_total(demand_filtered)
demand_emergency  = kpi_demand_emergency(demand_filtered)
upcoming_30d      = kpi_demand_upcoming_30d(demand_filtered, today)
overdue           = kpi_demand_overdue(demand_filtered, today)

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("📦 対象期間の需要件数", f"{demand_total:,} 件",
              help=f"希望納期が期間内にある需要数（FCST自動展開 + 緊急手動入力の合計）")
    render_drill_down_button("📅 製品FCST×部材必要量へ", "pages/1_demand_timeline.py",
                              filter_payload={"period_days": period_days},
                              key="drill_total")

with k2:
    st.metric("🚨 緊急手動入力", f"{demand_emergency:,} 件",
              help="営業FCSTから漏れた突発的な需要 (EMERGENCY_MANUAL)")
    render_drill_down_button("🚨 緊急シミュレーターへ", "pages/3_emergency_simulator.py",
                              filter_payload={"source_filter": "EMERGENCY_MANUAL"},
                              key="drill_emergency")

with k3:
    st.metric("⏰ 30日以内に納期到来", f"{upcoming_30d:,} 件",
              help="希望納期が今日から30日以内の需要件数")
    render_drill_down_button("🎯 調達アクションへ", "pages/2_action_center.py",
                              filter_payload={"days_filter": "30日以内"},
                              key="drill_upcoming")

with k4:
    st.metric("🔴 既に納期超過", f"{overdue:,} 件",
              help="希望納期が既に過ぎている需要件数（最優先対応）")
    render_drill_down_button("🎯 調達アクションへ", "pages/2_action_center.py",
                              filter_payload={"days_filter": "今日まで（超過のみ）"},
                              key="drill_overdue")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 📦 在庫サマリー
# ────────────────────────────────────────────────────────
st.markdown("### 📦 在庫サマリー（種別を明確に区別）")
render_inventory_legend()
st.markdown("")

cust_kpi = kpi_customer_stock(cust_inv)
free_kpi = kpi_macnica_free_stock(free_inv)

i1, i2, i3, i4 = st.columns(4)
i1.metric("🏭 顧客在庫 数量合計", f"{cust_kpi['total_qty']:,} 個",
          help="顧客自身が保有する自社倉庫の在庫合計")
i2.metric("🏭 顧客在庫 部材数", f"{cust_kpi['n_components']} 品目")
i3.metric("📦 マクニカフリー在庫 数量", f"{free_kpi['total_qty']:,} 個",
          help="マクニカが本顧客向けに引当済の在庫合計")
i4.metric("📦 マクニカフリー在庫 部材数", f"{free_kpi['n_components']} 品目")

cols = st.columns(2)
with cols[0]:
    render_drill_down_button("🏭 顧客在庫の詳細を見る (近日実装)", "pages/2_action_center.py",
                              filter_payload={"focus": "customer_stock"}, key="drill_cust_inv")
with cols[1]:
    render_drill_down_button("📦 マクニカフリー在庫の詳細を見る", "pages/4_macnica_free_inventory.py",
                              filter_payload={}, key="drill_free_inv")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 🚨 要対応リスト（KPI内訳とドリルダウン）
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 要対応リスト（顧客在庫だけで賄えない需要）")
st.caption(
    "顧客在庫で済む需要を除外し、マクニカへの相談・既存PO催促・新規発注のいずれかが必要なものを集計。"
    "各カウントをクリックすると同じフィルターで詳細ページに遷移します。"
)

if not options.empty and not demand_filtered.empty:
    opt = options.copy()
    opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date
    opt = filter_by_period(opt, "requested_date", today, period_days)

    best = aggregate_best_route_per_demand(opt)
    needs = filter_needs_action(best)
    level_counts = kpi_action_count_by_level(best)

    a1, a2, a3, a4 = st.columns(4)
    with a1:
        st.metric(f"{action_level_label_jp('重')}", f"{level_counts['重']} 件")
        render_drill_down_button("🎯 該当需要を見る", "pages/2_action_center.py",
                                  filter_payload={"action_level": "重"}, key="drill_lv_heavy")
    with a2:
        st.metric(f"{action_level_label_jp('中')}", f"{level_counts['中']} 件")
        render_drill_down_button("🎯 該当需要を見る", "pages/2_action_center.py",
                                  filter_payload={"action_level": "中"}, key="drill_lv_med")
    with a3:
        st.metric(f"{action_level_label_jp('軽')}", f"{level_counts['軽']} 件")
        render_drill_down_button("🎯 該当需要を見る", "pages/2_action_center.py",
                                  filter_payload={"action_level": "軽"}, key="drill_lv_light")
    with a4:
        st.metric(f"{action_level_label_jp('不要')}", f"{level_counts['不要']} 件")

    st.markdown("")

    if needs.empty:
        st.success("✅ 顧客在庫だけで全需要が賄えます。マクニカへの追加調達相談は不要です。")
    else:
        # 重→中→軽 順、不足数・遅延が大きい順
        level_order = {"重": 0, "中": 1, "軽": 2, "不要": 3}
        needs = needs.copy()
        needs["_lv"] = needs["action_level"].map(level_order).fillna(9)
        needs = needs.sort_values(["_lv", "shortage_qty", "days_late"], ascending=[True, False, False])

        if not components.empty:
            needs = needs.merge(
                components[["component_id", "part_number", "component_name"]],
                on="component_id", how="left",
            )

        # 業務用語に置換 + 列順を整理
        needs["対応レベル"] = needs["action_level"].apply(action_level_label_jp)
        needs["推奨最良ルート"] = needs["route_type"].apply(route_label_jp)

        display_cols = [
            "demand_id", "part_number", "component_name",
            "requested_date", "requested_qty",
            "対応レベル", "推奨最良ルート",
            "available_qty", "shortage_qty", "days_late",
        ]
        cols_present = [c for c in display_cols if c in needs.columns]
        df_show = rename_columns(needs[cols_present].head(10))

        st.dataframe(df_show, hide_index=True, use_container_width=True)
        st.caption(f"💡 要対応 {len(needs)} 件中、上位10件を表示。全件確認は「🎯 調達アクションセンター」へ。")
else:
    st.info("Goldテーブル `gold_procurement_options` がまだ生成されていません。Lakeflow パイプラインを実行してください。")
