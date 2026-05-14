"""
📊 総合ダッシュボード
======================
本Appのトップページ。今日のサマリーと優先対応事項を一画面で把握する。

Phase 8 改修:
  - KPIをCritical/High/Medium/OK に整理 (~5/~10/~20/rest)
  - 「在庫切れ予測 部材」KPIを廃止 (コンテキスト不明瞭)
  - 代わりに「今月中に安全在庫を割る」件数を直感的に表示
  - 各KPIに業務上の意味を明確なhelpテキストで付与
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
    get_lt_escalation,
    get_bom_fulfillment_status,
    get_pipeline_health,
    get_order_commit_risk,
)
from services.kpi_logic import (
    filter_by_period,
    kpi_demand_total, kpi_demand_emergency, kpi_demand_upcoming_30d, kpi_demand_overdue,
    kpi_customer_stock, kpi_macnica_free_stock,
    aggregate_best_route_per_demand, filter_needs_action, kpi_action_count_by_level,
    kpi_bom_fulfillment,
)
from services.glossary import (
    rename_columns, route_label_jp, action_level_label_jp, action_level_help,
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
    lt_escal   = get_lt_escalation()
    bom_fulfill = get_bom_fulfillment_status()
    pipe_health = get_pipeline_health()
    commit_risk = get_order_commit_risk()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル + 用語集
# ────────────────────────────────────────────────────────
st.markdown("## 📊 総合ダッシュボード")
st.caption("今日の調達状況サマリーと優先対応事項。各カード下の「詳細を見る」で該当ページへ遷移します。")
render_glossary(st)

# ────────────────────────────────────────────────────────
# 期間フィルター (9ヶ月対応)
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 期間フィルター")
period_options = {
    "今月（〜30日先）":      30,
    "3ヶ月以内":            90,
    "6ヶ月以内":           180,
    "9ヶ月以内（全期間）":  280,
}
sel_period = st.radio(
    "今日からどこまで先の需要を集計するか",
    options=list(period_options.keys()),
    index=3,
    horizontal=True,
    label_visibility="collapsed",
    help="本データは最大9ヶ月先まで網羅しています。",
)
period_days = period_options[sel_period]

demand_filtered = filter_by_period(demand, "requested_date", today, period_days, past_buffer_days=0)

st.markdown("---")

# ────────────────────────────────────────────────────────
# Section 0: 製品の顧客納期コミット (受注ベース)
# ────────────────────────────────────────────────────────
st.markdown("### 📌 **製品** の顧客納期コミット（受注ベース）")
st.caption(
    "顧客から受注した**製品**の納期コミット状況。"
    "**Critical = 残3日以内 / High = 残7日以内** が今日〜今週中に動くべき最優先案件です。"
    "（このセクションは製品レベルの受注を見ています。半導体部材の調達優先度は下のセクションを参照。）"
)

if not commit_risk.empty:
    cr = commit_risk.copy()
    if "days_to_due" not in cr.columns:
        cr["days_to_due"] = 0
    cr["days_to_due"] = pd.to_numeric(cr["days_to_due"], errors="coerce").fillna(0)
    n_cr_crit = int((cr["priority_rank"] == "Critical").sum())
    n_cr_high = int((cr["priority_rank"] == "High").sum())
    n_cr_mid  = int((cr["priority_rank"] == "Mid").sum())
    n_cr_low  = int((cr["priority_rank"] == "Low").sum())

    cc1, cc2, cc3, cc4 = st.columns(4)
    with cc1:
        st.metric(
            "🔴 Critical (残3日以内)",
            f"{n_cr_crit} 件",
            help="納期まで残3日以内。今すぐ動かないと納期遅延の可能性が高い受注。",
        )
        render_drill_down_button("今すぐ確認する", "pages/0_commit_dashboard.py",
                                  filter_payload={"priority": "Critical"}, key="drill_cr_crit")
    with cc2:
        st.metric(
            "🟠 High (残7日以内)",
            f"{n_cr_high} 件",
            help="納期まで残7日以内。今週中にマクニカへ相談・前倒し調整。",
        )
        render_drill_down_button("今週中に対応", "pages/0_commit_dashboard.py",
                                  filter_payload={"priority": "High"}, key="drill_cr_high")
    with cc3:
        st.metric("🟡 Mid (残14日以内)", f"{n_cr_mid} 件",
                  help="納期まで残14日以内。来週までに方針確定。")
    with cc4:
        st.metric("🟢 Low (残15日以上)", f"{n_cr_low} 件",
                  help="納期まで残15日以上。状況モニタリング段階。")
    st.markdown("")
    if n_cr_crit + n_cr_high == 0:
        st.success("✅ 残7日以内のCritical/High案件はありません。本日は通常運用でOK。")
else:
    st.info("gold_order_commit_risk が未生成です。受注データを確認してください。")

st.markdown("---")

# ────────────────────────────────────────────────────────
# Section 1: 半導体部材の需要サマリー (FCSTから展開)
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 **半導体部材** 需要サマリー（FCSTから展開、対象期間: {sel_period}）")

demand_total     = kpi_demand_total(demand_filtered)
demand_emergency = kpi_demand_emergency(demand_filtered)
upcoming_30d     = kpi_demand_upcoming_30d(demand_filtered, today)
overdue          = kpi_demand_overdue(demand_filtered, today)

k1, k2, k3 = st.columns(3)
with k1:
    st.metric(
        "📦 対象期間の需要件数",
        f"{demand_total:,} 件",
        help=f"希望納期が {sel_period} 内にある部材需要の総数。FCST自動展開 + 緊急手動入力の合計。",
    )
    render_drill_down_button("📅 部材タイムラインへ", "pages/1_demand_timeline.py",
                              filter_payload={"period_days": period_days}, key="drill_total")
with k2:
    st.metric(
        "🚨 緊急手動入力",
        f"{demand_emergency:,} 件",
        help="営業FCSTから漏れた突発的な需要（EMERGENCY_MANUAL）の件数。最優先でシミュレーションを。",
    )
    render_drill_down_button("🚨 緊急シミュレーターへ", "pages/3_emergency_simulator.py",
                              filter_payload={"source_filter": "EMERGENCY_MANUAL"},
                              key="drill_emergency")
with k3:
    st.metric(
        "⏰ 30日以内に納期到来",
        f"{upcoming_30d:,} 件",
        help="今日から30日以内に希望納期が来る需要。今週中に調達ルートを確定すべき案件。",
    )
    render_drill_down_button("🎯 調達アクションへ", "pages/2_action_center.py",
                              filter_payload={"days_filter": "30日以内"}, key="drill_upcoming")

st.markdown("---")

# ────────────────────────────────────────────────────────
# Section 2: 半導体部材の調達アクション優先度 (期間フィルター連動)
# ────────────────────────────────────────────────────────
st.markdown("### 🎯 **半導体部材** 調達アクション優先度")
st.caption(
    "FCST×BOM展開された**半導体部材**ごとの調達ルート評価 (4ルート) による優先度判定。"
    "**Critical (🔴) = 今すぐ新規発注**, **High (🟠) = 今週中にマクニカ相談** が最重要。"
    f"（表示期間: {sel_period}）"
)

if not options.empty and not demand_filtered.empty:
    opt = options.copy()
    opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date
    # 「今月 (30日以内)」で絞り込み → Critical件数を現実的に抑制
    opt = filter_by_period(opt, "requested_date", today, min(period_days, 90), past_buffer_days=0)

    best = aggregate_best_route_per_demand(opt)
    level_counts = kpi_action_count_by_level(best)

    a1, a2, a3, a4 = st.columns(4)
    with a1:
        st.metric(
            "🔴 Critical",
            f"{level_counts['重']} 件",
            help=action_level_help("重"),
        )
        render_drill_down_button("今すぐ対応する", "pages/2_action_center.py",
                                  filter_payload={"action_level": "重"}, key="drill_critical")
    with a2:
        st.metric(
            "🟠 High",
            f"{level_counts['中']} 件",
            help=action_level_help("中"),
        )
        render_drill_down_button("今週中に対応する", "pages/2_action_center.py",
                                  filter_payload={"action_level": "中"}, key="drill_high")
    with a3:
        st.metric(
            "🟡 Medium",
            f"{level_counts['軽']} 件",
            help=action_level_help("軽"),
        )
        render_drill_down_button("来週までに確認", "pages/2_action_center.py",
                                  filter_payload={"action_level": "軽"}, key="drill_medium")
    with a4:
        st.metric(
            "🟢 OK",
            f"{level_counts['不要']} 件",
            help=action_level_help("不要"),
        )

    # 要対応 Top 10 テーブル (Critical→High→Medium 順)
    needs = filter_needs_action(best)
    if not needs.empty:
        st.markdown("")
        level_order = {"重": 0, "中": 1, "軽": 2, "不要": 3}
        needs = needs.copy()
        needs["_lv"] = needs["action_level"].map(level_order).fillna(9)
        needs = needs.sort_values(["_lv", "shortage_qty", "days_late"], ascending=[True, False, False])

        if not components.empty:
            needs = needs.merge(
                components[["component_id", "part_number", "component_name"]],
                on="component_id", how="left",
            )

        needs["Priority"] = needs["action_level"].apply(action_level_label_jp)
        needs["推奨ルート"] = needs["route_type"].apply(route_label_jp)

        # P2-1: shortage_qty 列を削除（常に0のため不要）
        display_cols = [
            "part_number", "component_name",
            "requested_date", "requested_qty",
            "Priority", "推奨ルート",
        ]
        cols_present = [c for c in display_cols if c in needs.columns]
        df_show = rename_columns(needs[cols_present].head(10))
        st.dataframe(df_show, hide_index=True, use_container_width=True)
        st.caption(f"💡 要対応 {len(needs)} 件中、上位10件を表示。全件確認は「🎯 調達アクションセンター」へ。")
    else:
        st.success("✅ すべての需要が顧客在庫で充足可能です。追加調達不要。")
else:
    st.info("gold_procurement_options が未生成です。Lakeflow パイプラインを実行してください。")

st.markdown("---")

st.markdown("---")

# ────────────────────────────────────────────────────────
# Section 4: 供給能力モニター (コンパクト)
# ────────────────────────────────────────────────────────
st.markdown("### 🧩 供給能力モニター")
st.caption("BOM充足 / LT延長 の確認。詳細は各ページで。")

n_lt_escal = len(lt_escal) if not lt_escal.empty else 0
bom_kpi = kpi_bom_fulfillment(bom_fulfill)

s1, s2 = st.columns(2)
with s1:
    st.metric(
        "🔴 生産困難 製品×月",
        f"{bom_kpi['critical']} 件",
        help=f"BOM全部材が揃わず充足率 < 80% の製品×月の組合せ（評価対象 {bom_kpi['total']} 件中）。",
    )
    render_drill_down_button("🧩 BOM充足ビューへ", "pages/6_bom_fulfillment.py",
                              filter_payload={}, key="drill_bom")
with s2:
    st.metric(
        "⏳ LT延長中の部材",
        f"{n_lt_escal} 品目",
        help="3ヶ月前 or 6ヶ月前 比較でリードタイムが延びている部材。新規発注タイミングの前倒しが必要。",
    )
    render_drill_down_button("⏳ LT推移を確認", "pages/7_lead_time_trend.py",
                              filter_payload={}, key="drill_lt")

# Pipeline status (1行)
st.markdown("")
if not pipe_health.empty:
    n_ok = int(pipe_health["success_flag"].astype(str).str.lower().isin(["true","1","t"]).sum())
    n_total_ph = len(pipe_health)
    status = "✅ すべて正常" if n_ok == n_total_ph else f"⚠️ {n_total_ph - n_ok}本に異常"
    st.caption(f"🔧 データパイプライン: {status} ({n_ok}/{n_total_ph}本更新済)")
