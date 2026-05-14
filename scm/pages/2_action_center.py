"""
🎯 調達アクションセンター【本Appのコア画面】
=============================================
業務フロー: バランスチェック → 4ルート判断 → 納期回答

このページの役割:
  - 全部材の充足状況を一覧表示（部材×希望納期×対応レベル）
  - 行クリックで4ルート比較 + 複数の具体アクション案を提示
  - 顧客（購買担当）が「どのルートで調達するか」を判断する中核UI
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import timedelta

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.route_comparison import render_route_comparison, render_route_legend
from components.inventory_badge import render_inventory_legend
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from components.drill_down import pop_drill_filter
from services.config import get_as_of_date
from services.database import (
    get_silver_demand_plan_components,
    get_silver_components,
    get_procurement_options,
)
from services.glossary import (
    rename_columns, route_label_jp, action_level_label_jp, source_label_jp,
)
from services.recommendation import (
    generate_action_options, estimate_pull_in_qty_from_next_month,
)

st.set_page_config(page_title="調達アクションセンター | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="全部材の充足状況と4ルート評価")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    components = get_silver_components()
    options    = get_procurement_options()

today = get_as_of_date()

# ダッシュボードからのドリルダウン状態を取得
drill = pop_drill_filter()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🎯 調達アクションセンター")
st.caption("全需要×全部材の充足状況を一覧。行を選択すると4ルート比較 + 複数の具体アクション案が表示されます。")
render_route_legend()
render_inventory_legend()
st.markdown("")

if demand.empty or options.empty:
    st.warning("需要計画 または 調達評価データが空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# ────────────────────────────────────────────────────────
# 検索 + 部材セレクタ (Phase 7: 部材カテゴリは撤廃)
# ────────────────────────────────────────────────────────
sb1, sb2 = st.columns(2)
with sb1:
    search_query = render_search_bar(components, key="action_search")
with sb2:
    selected_comp_ids = render_component_selector(components, key="action_comp_sel")

# ────────────────────────────────────────────────────────
# 需要×部材の集約 (各需要に対する4ルート評価から「最良ルート」を選定)
# ────────────────────────────────────────────────────────
opt = options.copy()
opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0).astype(int)
opt["available_qty"] = pd.to_numeric(opt["available_qty"], errors="coerce").fillna(0).astype(int)
opt["days_late"] = pd.to_numeric(opt["days_late"], errors="coerce").fillna(0).astype(int)
opt["requested_qty"] = pd.to_numeric(opt["requested_qty"], errors="coerce").fillna(0).astype(int)
opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date

# 需要ごと: 3ルート (NEW_ORDER除外) の確保可能数 / 最良ルート / 充足判定
# NEW_ORDER は「発注すれば原理上いくらでも確保できる」ため、不足数計算から除外
opt_3routes = opt[opt["route_type"] != "NEW_ORDER"] if "route_type" in opt.columns else opt

agg_dict = {
    "requested_qty":        ("requested_qty",  "first"),
    "requested_date":       ("requested_date", "first"),
    "component_id":         ("component_id",   "first"),
    "total_avail_3routes":  ("available_qty",  "sum"),    # 3ルート合計
}
if "action_level" in opt.columns:
    agg_dict["action_level"] = ("action_level", "first")
if "needs_action" in opt.columns:
    agg_dict["needs_action"] = ("needs_action", "first")

agg = opt_3routes.groupby("demand_id", as_index=False).agg(**agg_dict)
# 不足数 = 3ルートで賄えない分 (NEW_ORDER を使わずに調達できない数量)
agg["shortage_3routes"] = (agg["requested_qty"] - agg["total_avail_3routes"]).clip(lower=0)

# 部材名・カテゴリを結合
if not components.empty:
    agg = agg.merge(
        components[["component_id", "part_number", "component_name", "component_category", "base_lead_time_weeks"]],
        on="component_id", how="left",
    )

# 需要メタ
demand_meta = demand[["demand_id", "source_type", "note"]] if not demand.empty else pd.DataFrame()
if not demand_meta.empty:
    agg = agg.merge(demand_meta, on="demand_id", how="left")

# 業務用語による表示用列
agg["対応レベル"] = agg["action_level"].apply(action_level_label_jp) if "action_level" in agg.columns else "—"
agg["発生源"] = agg["source_type"].apply(source_label_jp) if "source_type" in agg.columns else "—"
agg["希望納期まで(日)"] = (pd.to_datetime(agg["requested_date"]) - pd.Timestamp(today)).dt.days

# 検索 + 部材セレクタフィルター適用
if search_query:
    agg = apply_component_search(agg, search_query)
if selected_comp_ids:
    agg = apply_component_id_filter(agg, selected_comp_ids)

# ────────────────────────────────────────────────────────
# フィルター（タイトル明確化）
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター")
fc1, fc2, fc3 = st.columns(3)

# ドリルダウンからの初期値
init_action_level = drill.get("action_level") if drill else None
init_days_filter = drill.get("days_filter") if drill else None

with fc1:
    level_options = ["（すべて）", "🔴 新規発注必要（LT考慮）", "🟠 複数ルート組合せ要相談", "🟡 単一ルート相談で対応可", "🟢 自社在庫で対応可"]
    level_default = 0
    if init_action_level:
        # ドリルダウンの action_level (重/中/軽) → 表示名
        level_map = {"重": 1, "中": 2, "軽": 3, "不要": 4}
        level_default = level_map.get(init_action_level, 0)
    sel_level = st.selectbox(
        "🚦 対応レベル（自社で対応可か、マクニカ相談が必要かの判別）",
        level_options, index=level_default,
    )

with fc2:
    src_options = ["（すべて）", "営業FCSTから自動展開", "緊急手動入力"]
    sel_src = st.selectbox(
        "📋 需要発生源（FCSTか、突発緊急かの判別）",
        src_options,
    )

with fc3:
    days_options = ["（すべて）", "今日まで（超過のみ）", "30日以内", "60日以内", "90日以内"]
    days_default = 0
    if init_days_filter:
        try:
            days_default = days_options.index(init_days_filter)
        except ValueError:
            days_default = 0
    sel_days = st.selectbox(
        "⏰ 希望納期の時期（緊急度で絞り込み）",
        days_options, index=days_default,
    )

# 絞り込み適用
df = agg.copy()
level_label_to_internal = {
    "🔴 新規発注必要（LT考慮）":         "重",
    "🟠 複数ルート組合せ要相談":         "中",
    "🟡 単一ルート相談で対応可":         "軽",
    "🟢 自社在庫で対応可":              "不要",
}
if sel_level != "（すべて）":
    target_internal = level_label_to_internal.get(sel_level)
    if target_internal:
        df = df[df["action_level"] == target_internal]

src_label_to_internal = {
    "営業FCSTから自動展開": "FCST_AUTO",
    "緊急手動入力":         "EMERGENCY_MANUAL",
}
if sel_src != "（すべて）" and "source_type" in df.columns:
    df = df[df["source_type"] == src_label_to_internal[sel_src]]

if sel_days == "今日まで（超過のみ）":
    df = df[df["希望納期まで(日)"] < 0]
elif sel_days == "30日以内":
    df = df[(df["希望納期まで(日)"] >= 0) & (df["希望納期まで(日)"] <= 30)]
elif sel_days == "60日以内":
    df = df[(df["希望納期まで(日)"] >= 0) & (df["希望納期まで(日)"] <= 60)]
elif sel_days == "90日以内":
    df = df[(df["希望納期まで(日)"] >= 0) & (df["希望納期まで(日)"] <= 90)]

# ────────────────────────────────────────────────────────
# 一覧表（業務観点で列順を最適化）
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 需要一覧 ({len(df):,} 件)")

if df.empty:
    st.info("条件に該当する需要がありません。フィルターを調整してください。")
else:
    # 表示順: 対応レベル降順、希望納期昇順
    level_order = {"重": 0, "中": 1, "軽": 2, "不要": 3}
    df["_lv"] = df["action_level"].map(level_order).fillna(9) if "action_level" in df.columns else 9
    df = df.sort_values(["_lv", "希望納期まで(日)"]).drop(columns="_lv")

    # 業務観点の列順
    show_cols = [
        "demand_id",
        "part_number",
        "component_name",
        "発生源",
        "requested_date",
        "希望納期まで(日)",
        "requested_qty",
        "total_avail_3routes",
        "shortage_3routes",
        "対応レベル",
    ]
    cols_present = [c for c in show_cols if c in df.columns]
    df_show = rename_columns(
        df[cols_present],
        extra={"total_avail_3routes": "3ルート確保可能数", "shortage_3routes": "不足数(3ルート)"},
    )
    st.dataframe(df_show, hide_index=True, use_container_width=True, height=380)

    st.markdown("---")

    # ────────────────────────────────────────────────────
    # 4ルート比較 + 具体アクション提案
    # ────────────────────────────────────────────────────
    st.markdown("### 🔬 4ルート比較 + 具体アクション提案")

    df_select = df.head(200).copy()
    df_select["_label"] = (
        df_select["demand_id"].astype(str)
        + "  ｜  " + df_select.get("part_number", pd.Series("")).fillna("").astype(str)
        + "  ｜  " + df_select.get("component_name", pd.Series("")).fillna("").astype(str)
        + "  ｜  必要 " + df_select["requested_qty"].astype(str) + " 個"
        + "  ｜  納期 " + df_select["requested_date"].astype(str)
    )
    sel_label = st.selectbox("詳細を見る需要を選択", df_select["_label"].tolist())

    if sel_label:
        sel_demand_id = sel_label.split("  ｜  ", 1)[0].strip()
        sel_options = opt[opt["demand_id"] == sel_demand_id].copy()
        sel_demand = df_select[df_select["demand_id"] == sel_demand_id].iloc[0]
        sel_comp_id = sel_demand["component_id"]

        # 4ルートカード
        render_route_comparison(
            sel_options,
            requested_qty=int(sel_demand["requested_qty"]),
            requested_date=sel_demand["requested_date"],
        )

        # ────────────────────────────────────────────
        # 顧客在庫の推移ビューへの遷移ボタン (Phase 7追加)
        # ────────────────────────────────────────────
        bc1, bc2 = st.columns([1, 3])
        with bc1:
            if st.button("📈 この部材の在庫推移を見る", key=f"goto_inv_{sel_demand_id}", use_container_width=True):
                st.session_state["drill_filter"] = {"component_id": sel_comp_id}
                st.switch_page("pages/8_inventory_health.py")
        with bc2:
            st.caption(
                "💡 顧客在庫の月別推移（入荷・消費・予測在庫）を時系列で確認します。"
                "前倒し消費の可否判断に活用してください。"
            )

        # ────────────────────────────────────────────
        # 💡 具体アクション提案（複数案を並列表示）
        # ────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 💡 具体アクション案（実行可能な選択肢を併記）")
        st.caption("各案には具体的な手順・数量・完了日を明示。比較してご判断ください。")

        # P4: 用語定義パネル (⑤⑥は初出のため説明を提供)
        with st.expander("📖 案⑤⑥の用語説明（クリックで展開）", expanded=False):
            st.markdown(
                """
**⑤ 新規追加発注とは？**
マクニカ経由でメーカーへ新規に追加発注する方法。必要数量に上限はありませんが、
部材ごとのリードタイム（LT）週数後に入荷となります。
「今すぐ発注しても間に合わない」場合でも、できる限り早く動くことで遅延を最小化できます。
→ **依頼先**: マクニカ調達部

**⑥ 翌月以降のFCSTから前倒し充当とは？**
来月・再来月に予定している製品FCSTの需要分を、今月の緊急需要に前倒しで使う方法。
前倒しした分は翌月の調達計画に影響しますが、在庫余裕がある月なら活用可能です。
→ **注意**: 翌月の調達手配が別途必要になります。生産FCSTの承認が前提。
"""
            )

        lt_weeks = int(pd.to_numeric(sel_demand.get("base_lead_time_weeks", 20), errors="coerce") or 20)
        pull_in = estimate_pull_in_qty_from_next_month(
            component_id=sel_comp_id,
            requested_date=sel_demand["requested_date"],
            demand_df=demand,
            today=today,
        )

        action_opts = generate_action_options(
            routes_df=sel_options,
            requested_qty=int(sel_demand["requested_qty"]),
            requested_date=sel_demand["requested_date"],
            today=today,
            other_month_pull_in_qty=pull_in,
            component_lt_weeks=lt_weeks,
        )

        if action_opts:
            for i, opt_a in enumerate(action_opts[:5], start=1):
                feas_color = {"確実": "#059669", "見込み": "#d97706", "要相談": "#dc2626"}.get(opt_a.feasibility, "#475569")
                feas_bg    = {"確実": "#f0fdf4", "見込み": "#fffbeb", "要相談": "#fef2f2"}.get(opt_a.feasibility, "#f8fafc")
                feas_border= {"確実": "#bbf7d0", "見込み": "#fde68a", "要相談": "#fecaca"}.get(opt_a.feasibility, "#e2e8f0")

                steps_html = "".join(
                    f'<div style="display:flex;gap:8px;align-items:flex-start;margin-bottom:6px;">'
                    f'<span style="min-width:20px;height:20px;background:#f1f5f9;border-radius:50%;'
                    f'display:inline-flex;align-items:center;justify-content:center;font-size:10px;'
                    f'font-weight:700;color:#475569;flex-shrink:0;margin-top:1px;">{j}</span>'
                    f'<span style="font-size:13px;color:#475569;line-height:1.5;">{s}</span></div>'
                    for j, s in enumerate(opt_a.steps, 1)
                )
                gap_block = (
                    f'<div style="display:flex;flex-direction:column;gap:2px;">'
                    f'<div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.6px;">未充足</div>'
                    f'<div style="font-size:20px;font-weight:700;color:#dc2626;letter-spacing:-0.02em;">{opt_a.gap_qty:,}個</div>'
                    f'</div>'
                ) if opt_a.gap_qty > 0 else ""

                card_html = (
                    f'<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;'
                    f'padding:18px 20px;margin-bottom:12px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'

                    # ヘッダー: タイトル(①②...含む) / 確実度バッジ ※「案N」の二重表記を削除
                    f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">'
                    f'<span style="font-size:15px;font-weight:600;color:#0f172a;flex:1;">{opt_a.title}</span>'
                    f'<span style="display:inline-flex;align-items:center;gap:4px;background:{feas_bg};'
                    f'color:{feas_color};border:1px solid {feas_border};border-radius:6px;'
                    f'padding:3px 10px;font-size:12px;font-weight:600;">{opt_a.feasibility}</span>'
                    f'</div>'

                    # メタ情報: 充足数量 / 未充足 / 完了日
                    f'<div style="display:flex;gap:28px;margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid #f1f5f9;">'
                    f'<div style="display:flex;flex-direction:column;gap:2px;">'
                    f'<div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.6px;">充足数量</div>'
                    f'<div style="font-size:20px;font-weight:700;color:#059669;letter-spacing:-0.02em;">{opt_a.coverage_qty:,}個</div>'
                    f'</div>'
                    f'{gap_block}'
                    f'<div style="display:flex;flex-direction:column;gap:2px;">'
                    f'<div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.6px;">完了予定日</div>'
                    f'<div style="font-size:20px;font-weight:700;color:#0f172a;letter-spacing:-0.02em;">{opt_a.eta_date.isoformat()}</div>'
                    f'</div>'
                    f'</div>'

                    # 手順
                    f'<div style="display:flex;flex-direction:column;">{steps_html}</div>'
                    f'</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)

        # 補助情報
        st.caption(
            f"部材: {sel_demand.get('component_name', '—')} ({sel_demand.get('part_number', '—')})"
            f"  ｜  発生源: {sel_demand.get('発生源', '—')}"
            f"  ｜  メモ: {sel_demand.get('note', '') or '—'}"
        )
