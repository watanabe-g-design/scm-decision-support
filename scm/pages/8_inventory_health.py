"""
🏭 顧客在庫×安全在庫モニター
================================
業務上の役割:
  - 部材ごとの顧客自社倉庫の在庫が安全在庫レンジに納まっているかを月別に可視化
  - 月末在庫予測 (gold_balance_projection_monthly) で未来の在庫健全性を把握
  - 各部材を Expander で展開: 月次表 + 折れ線グラフを一体表示
  - 前倒し消費の可否判断、新規発注タイミング判断の中核データ

Phase 7 改修:
  ✅ 部材ごとに Expander → 中で「表 + グラフ」を併置 (添付スクショ風UI)
  ✅ グラフ: 予測在庫(実線) + 安全在庫(min, 破線) + 上限(max, 破線) + 状態マーカー
  ✅ 部材カテゴリフィルター撤廃、検索 + 部材セレクタへ
  ✅ アクションセンターからの遷移 (drill_filter.component_id) に対応
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from components.drill_down import pop_drill_filter
from components.timeline_helper import add_today_vline
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens
from services.database import (
    get_balance_projection,
    get_inventory_breach,
    get_silver_inventory_current,
    get_silver_components,
)

st.set_page_config(page_title="顧客在庫健全性 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="顧客自社倉庫の在庫が安全在庫の中に納まっているかを部材別にモニター")

t = get_theme_tokens()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    proj   = get_balance_projection()
    breach = get_inventory_breach()
    cur    = get_silver_inventory_current()
    comps  = get_silver_components()

today = get_as_of_date()

# 遷移パラメータ
drill = pop_drill_filter()
drill_comp_id = drill.get("component_id") if drill else None

st.markdown("## 🏭 顧客在庫×安全在庫モニター")
st.caption(
    "顧客自社倉庫が保有する在庫を月別に可視化。"
    "**ZERO**=在庫切れ / **UNDER**=安全在庫割れ / **OK**=健全 / **OVER**=過剰。"
    "各部材を展開すると、月別の入出庫テーブル + 安全在庫レンジ付きグラフが表示されます。"
)

if proj.empty:
    st.warning("gold_balance_projection_monthly が空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# 型整え
proj = proj.copy()
for c in ["customer_stock_proj", "min_qty", "max_qty", "confirmed_order_qty",
          "forecast_qty", "inbound_qty_order_linked", "production_use_qty"]:
    if c in proj.columns:
        proj[c] = pd.to_numeric(proj[c], errors="coerce").fillna(0).astype(int)

# components マージ
if not comps.empty:
    proj = proj.merge(
        comps[["component_id", "part_number", "component_name", "component_category", "supplier_name"]].rename(
            columns={"component_id": "item_id"}
        ),
        on="item_id", how="left",
    )

# 過去月は除外 (今日が含まれる月以降を表示)
today_month = today.strftime("%Y-%m")
proj_display = proj[proj["month_end_date"] >= today_month].copy()

# ────────────────────────────────────────────────────────
# KPI: 需給バランス全体像 (健全性スコア中心)
# ────────────────────────────────────────────────────────
breach_df = breach.copy() if not breach.empty else pd.DataFrame()
n_zero = int((breach_df["breach_type"] == "ZERO").sum())  if not breach_df.empty else 0
n_under = int((breach_df["breach_type"] == "UNDER").sum()) if not breach_df.empty else 0
n_over = int((breach_df["breach_type"] == "OVER").sum())  if not breach_df.empty else 0
n_managed = int(proj_display["item_id"].nunique())

# 健全性スコア: OK 状態の component×month の比率
total_evaluated = len(proj_display)
n_ok = int((proj_display["policy_status"] == "OK").sum()) if total_evaluated else 0
health_pct = round(n_ok / total_evaluated * 100, 1) if total_evaluated else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "🩺 在庫健全性スコア",
    f"{health_pct} %",
    help="月末予測在庫が安全在庫レンジ内（min ≤ 在庫 ≤ max）の比率。65-75%が現実的に健全な水準。",
)
k2.metric("📦 管理部材数", f"{n_managed} 品目")
k3.metric("🔴 在庫ZERO予測", f"{n_zero} 件",
          help="6ヶ月以内に予測在庫が0以下となる部材×月の件数。新規発注が必要。")
k4.metric("🟠 安全在庫割れ予測", f"{n_under} 件",
          help="6ヶ月以内に予測在庫が安全在庫を割る部材×月の件数。発注タイミング前倒し検討。")

# ────────────────────────────────────────────────────────
# 需給バランス インサイト (上位リスク部材を可視化)
# ────────────────────────────────────────────────────────
st.markdown("")
st.markdown("#### 📌 今すぐ確認すべき部材 (上位リスク3件)")
st.caption("健全性に最も影響している部材を抽出。クリックで個別ビューに展開。")

if not breach_df.empty:
    risk_top = breach_df.copy()
    risk_top["projected_stock"] = pd.to_numeric(risk_top["projected_stock"], errors="coerce").fillna(0)
    # ZERO -> UNDER -> OVER の順、かつ first_breach が早い順
    type_order = {"ZERO": 0, "UNDER": 1, "OVER": 2}
    risk_top["_o"] = risk_top["breach_type"].map(type_order).fillna(9)
    risk_top = risk_top.sort_values(["_o", "first_breach"]).drop_duplicates(subset=["item_id"]).head(3)

    cols = st.columns(3)
    for col, (_, row) in zip(cols, risk_top.iterrows()):
        breach_type = row.get("breach_type", "ZERO")
        breach_color = {"ZERO": "#dc2626", "UNDER": "#d97706", "OVER": "#7c3aed"}.get(breach_type, "#475569")
        breach_label = {"ZERO": "在庫ゼロ予測", "UNDER": "安全在庫割れ", "OVER": "過剰在庫"}.get(breach_type, "—")
        with col:
            st.markdown(
                f"""
                <div class="biz-card" style="border-left:4px solid {breach_color};">
                    <div style="font-size:11px;font-weight:600;color:{breach_color};text-transform:uppercase;letter-spacing:0.6px;">{breach_label}</div>
                    <div style="font-size:15px;font-weight:600;color:#0f172a;margin:4px 0 8px;">
                        {row.get('item_code', '—')}<br>
                        <span style="font-size:13px;font-weight:400;color:#475569;">{row.get('product_name', '—')}</span>
                    </div>
                    <div style="font-size:12px;color:#475569;">
                        初回発生月: <strong style="color:#0f172a;">{row.get('first_breach', '—')}</strong><br>
                        予測在庫: <strong style="color:{breach_color};">{int(row.get('projected_stock', 0)):,}個</strong> (安全在庫 {int(row.get('min_qty', 0)):,})
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
else:
    st.success("✅ リスク部材はありません。全部材が安全在庫レンジ内で推移する見込みです。")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 検索 + 部材セレクタ
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（部材を選択して在庫推移を確認）")
fc1, fc2 = st.columns(2)
with fc1:
    search_query = render_search_bar(comps, key="inv_search")
with fc2:
    sel_ids = render_component_selector(comps, key="inv_comp_sel")

fc3, fc4 = st.columns([1, 2])
with fc3:
    status_options = ["（すべて）", "🔴 ZERO（在庫切れ）", "🟠 UNDER（安全在庫割れ）", "🟢 OK（健全）", "🟡 OVER（過剰）"]
    sel_status = st.selectbox("🚦 在庫ステータスで絞り込み", status_options)

# ドリルダウンからのパラメータ
filtered = proj_display.copy()
if drill_comp_id and not sel_ids:
    filtered = filtered[filtered["item_id"] == drill_comp_id]
    st.info(f"📍 アクションセンターから遷移: 部材 **{drill_comp_id}** に絞り込み中")

if search_query:
    q = search_query.lower().strip()
    mask = (
        filtered["item_id"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["item_code"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["product_name"].astype(str).str.lower().str.contains(q, na=False)
    )
    filtered = filtered[mask]
if sel_ids:
    filtered = filtered[filtered["item_id"].isin(sel_ids)]

status_map = {
    "🔴 ZERO（在庫切れ）": "ZERO",
    "🟠 UNDER（安全在庫割れ）": "UNDER",
    "🟢 OK（健全）":       "OK",
    "🟡 OVER（過剰）":     "OVER",
}
if sel_status != "（すべて）":
    target = status_map.get(sel_status)
    affected_items = filtered[filtered["policy_status"] == target]["item_id"].unique()
    filtered = filtered[filtered["item_id"].isin(affected_items)]

if filtered.empty:
    st.info("条件に該当する部材がありません。検索/セレクタを調整してください。")
    st.stop()

# ────────────────────────────────────────────────────────
# 部材ごとに Expander で表+グラフを一体表示 (添付スクショ風)
# ────────────────────────────────────────────────────────
st.markdown(f"### 📊 部材別 在庫健全性（{filtered['item_id'].nunique()} 品目）")
st.caption(
    "各部材を展開すると、月別の入出庫表 + 安全在庫レンジ付きグラフが表示されます。"
    "🔴=在庫ZERO予測月, 🟠=安全在庫割れ予測月, 🟡=過剰在庫月 をマーカーで強調。"
)

# 状態が悪い部材を上位に並べる
item_priority = (
    filtered.groupby("item_id")
    .agg(
        worst=("policy_status", lambda s: 0 if "ZERO" in s.values
               else (1 if "UNDER" in s.values
               else (2 if "OVER" in s.values else 3))),
    )
    .reset_index()
    .sort_values("worst")
)
ordered_ids = item_priority["item_id"].tolist()

# 表示数上限
max_show = st.number_input(
    "🔢 表示する部材数（上から優先度の高い順）",
    min_value=1, max_value=200, value=min(20, len(ordered_ids)), step=5,
)
ordered_ids = ordered_ids[:int(max_show)]

# 状態 → 表示用アイコン
status_icon_map = {"ZERO": "🔴", "UNDER": "🟠", "OK": "🟢", "OVER": "🟡"}

for iid in ordered_ids:
    sub = filtered[filtered["item_id"] == iid].sort_values("month_end_date")
    if sub.empty:
        continue

    part_number = sub["item_code"].iloc[0] if "item_code" in sub.columns else ""
    name = sub["product_name"].iloc[0] if "product_name" in sub.columns else ""
    supplier = sub["supplier_name"].iloc[0] if "supplier_name" in sub.columns else ""
    worst_status = sub["policy_status"].mode().iloc[0] if not sub["policy_status"].empty else "OK"

    # サマリラベル (ZERO/UNDER/OVER のあるなしを反映)
    statuses_set = set(sub["policy_status"].unique())
    badges = ""
    for s in ["ZERO", "UNDER", "OVER"]:
        if s in statuses_set:
            badges += f" {status_icon_map[s]}{s}"
    if not badges:
        badges = " 🟢OK"
    expander_label = f"{part_number} ｜ {name} ｜ {supplier} —{badges}"

    # ドリルダウンで来た部材は最初から展開
    is_expanded = (drill_comp_id == iid)
    with st.expander(expander_label, expanded=is_expanded):
        # ── 月次表 ──
        sub_show = sub.copy()
        sub_show["状態"] = sub_show["policy_status"].map(status_icon_map).fillna(sub_show["policy_status"]) + " " + sub_show["policy_status"]
        table_cols = [
            ("month_end_date",            "月"),
            ("customer_stock_proj",       "予測在庫"),
            ("confirmed_order_qty",       "確定受注"),
            ("forecast_qty",              "フォーキャスト"),
            ("inbound_qty_order_linked",  "入荷予定"),
            ("production_use_qty",        "消費量"),
            ("min_qty",                   "min（安全在庫）"),
            ("max_qty",                   "max（上限）"),
            ("状態",                       "ステータス"),
        ]
        cols_present = [(k, v) for k, v in table_cols if k in sub_show.columns]
        table = sub_show[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
        st.dataframe(table, hide_index=True, use_container_width=True, height=min(320, 60 + 36*len(table)))

        # ── グラフ (予測在庫 + min/max + 状態マーカー) ──
        sub["month_dt"] = pd.to_datetime(sub["month_end_date"] + "-01", errors="coerce")
        # min/max は最後の値で代表
        min_v = int(sub["min_qty"].iloc[-1]) if "min_qty" in sub.columns else 0
        max_v = int(sub["max_qty"].iloc[-1]) if "max_qty" in sub.columns else 0

        fig = go.Figure()
        # 予測在庫ライン
        fig.add_trace(go.Scatter(
            x=sub["month_dt"], y=sub["customer_stock_proj"],
            mode="lines+markers",
            name="予測在庫",
            line=dict(color=t["text"], width=2.5),
            marker=dict(size=8, color=t["text"]),
            hovertemplate="%{x|%Y-%m}<br>予測在庫: %{y:,}個<extra></extra>",
        ))
        # 安全在庫 (min) - dashed
        fig.add_trace(go.Scatter(
            x=sub["month_dt"], y=[min_v] * len(sub),
            mode="lines",
            name=f"安全在庫 (min={min_v:,})",
            line=dict(color=t["orange"], width=1.5, dash="dash"),
            hovertemplate=f"安全在庫: {min_v:,}個<extra></extra>",
        ))
        # 上限 (max) - dashed
        fig.add_trace(go.Scatter(
            x=sub["month_dt"], y=[max_v] * len(sub),
            mode="lines",
            name=f"最大基準 (max={max_v:,})",
            line=dict(color=t["blue"], width=1.5, dash="dash"),
            hovertemplate=f"最大基準: {max_v:,}個<extra></extra>",
        ))

        # 状態の悪い月にマーカー追加
        for status, color, symbol in [("ZERO", t["red"], "x"),
                                       ("UNDER", t["orange"], "x"),
                                       ("OVER", "#d4a000", "diamond")]:
            bad = sub[sub["policy_status"] == status]
            if not bad.empty:
                fig.add_trace(go.Scatter(
                    x=bad["month_dt"], y=bad["customer_stock_proj"],
                    mode="markers",
                    name=status,
                    marker=dict(color=color, size=14, symbol=symbol, line=dict(width=2, color=color)),
                    hovertemplate="%{x|%Y-%m}<br>" + status + "<extra></extra>",
                ))

        fig.update_layout(**base_layout(height=340, x_title="月", y_title="数量（個）"))
        fig.update_xaxes(tickformat="%Y-%m")
        add_today_vline(fig)
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{iid}")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 安全在庫割れアラート明細
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 安全在庫割れ/在庫ZERO 部材一覧（初回発生月でソート）")
if breach_df.empty:
    st.success("✅ 直近6ヶ月以内に安全在庫割れ/在庫切れ予測の部材はありません。")
else:
    show_cols = [
        ("item_id",          "部材ID"),
        ("item_code",        "品番"),
        ("product_name",     "部材名"),
        ("breach_type",      "状態"),
        ("breach_date",      "発生月"),
        ("projected_stock",  "予測在庫"),
        ("min_qty",          "安全在庫"),
        ("max_qty",          "上限在庫"),
        ("first_breach",     "初回発生月"),
    ]
    cols_present = [(k, v) for k, v in show_cols if k in breach_df.columns]
    table = breach_df[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    st.dataframe(table, hide_index=True, use_container_width=True, height=320)
