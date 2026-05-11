"""
🏭 顧客在庫×安全在庫モニター
================================
業務上の役割:
  - 製品/部材単位で「顧客自社倉庫の在庫が安全在庫の中に納まっているか」を可視化
  - 月末在庫予測 (gold_balance_projection_monthly) を使って未来の在庫健全性を把握
  - ZERO / UNDER / OK / OVER の4状態を月別マトリクスで一覧
  - 全製品でいつ在庫が不足するかをタイミング込みで把握

LT推移ページと並んで、新規発注の判断根拠を提供する重要モニター。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css, plot_colors
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.search_bar import render_search_bar, apply_component_search
from components.timeline_helper import add_today_vline
from services.config import get_as_of_date
from services.database import (
    get_balance_projection,
    get_inventory_breach,
    get_silver_inventory_current,
    get_silver_components,
)

st.set_page_config(page_title="顧客在庫健全性 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="顧客自社倉庫の在庫が安全在庫の中に納まっているかをモニター")

colors = plot_colors()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    proj   = get_balance_projection()
    breach = get_inventory_breach()
    cur    = get_silver_inventory_current()
    comps  = get_silver_components()

today = get_as_of_date()

st.markdown("## 🏭 顧客在庫×安全在庫モニター")
st.caption(
    "顧客自社倉庫が保有する在庫を月末予測で可視化。"
    "ZERO=在庫切れ予測 / UNDER=安全在庫割れ / OK=健全 / OVER=過剰。"
    "未来月に在庫が不足するタイミングを早期察知できます。"
)

if proj.empty:
    st.warning("gold_balance_projection_monthly が空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# 型整え
proj = proj.copy()
proj["customer_stock_proj"] = pd.to_numeric(proj["customer_stock_proj"], errors="coerce").fillna(0).astype(int)
proj["min_qty"] = pd.to_numeric(proj["min_qty"], errors="coerce").fillna(0).astype(int)
proj["max_qty"] = pd.to_numeric(proj["max_qty"], errors="coerce").fillna(0).astype(int)
proj["confirmed_order_qty"] = pd.to_numeric(proj["confirmed_order_qty"], errors="coerce").fillna(0).astype(int)
proj["forecast_qty"] = pd.to_numeric(proj["forecast_qty"], errors="coerce").fillna(0).astype(int)
proj["inbound_qty_order_linked"] = pd.to_numeric(proj["inbound_qty_order_linked"], errors="coerce").fillna(0).astype(int)
proj["production_use_qty"] = pd.to_numeric(proj["production_use_qty"], errors="coerce").fillna(0).astype(int)

# 部材カテゴリを結合
if not comps.empty:
    proj = proj.merge(
        comps[["component_id", "component_category"]].rename(columns={"component_id": "item_id"}),
        on="item_id", how="left",
    )

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
breach_df = breach.copy() if not breach.empty else pd.DataFrame()
n_zero = int((breach_df["breach_type"] == "ZERO").sum())  if not breach_df.empty else 0
n_under = int((breach_df["breach_type"] == "UNDER").sum()) if not breach_df.empty else 0
n_over = int((breach_df["breach_type"] == "OVER").sum())  if not breach_df.empty else 0
n_managed = int(proj["item_id"].nunique())

k1, k2, k3, k4 = st.columns(4)
k1.metric("📦 管理部材数", f"{n_managed} 品目")
k2.metric("🔴 在庫ZERO予測", f"{n_zero} 件")
k3.metric("🟠 安全在庫割れ予測", f"{n_under} 件")
k4.metric("🟡 過剰在庫予測", f"{n_over} 件")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 検索 + フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（在庫健全性の表示範囲を絞る）")
search_query = render_search_bar(comps, key="inv_health_search")

fc1, fc2, fc3 = st.columns(3)
with fc1:
    cat_options = ["（すべて）"] + sorted([c for c in proj["component_category"].dropna().unique()])
    sel_cat = st.selectbox("📂 部材カテゴリで絞り込み", cat_options)
with fc2:
    status_options = ["（すべて）", "🔴 ZERO（在庫切れ）", "🟠 UNDER（安全在庫割れ）", "🟢 OK（健全）", "🟡 OVER（過剰）"]
    sel_status = st.selectbox("🚦 在庫ステータスで絞り込み", status_options)
with fc3:
    months_avail = sorted([m for m in proj["month_end_date"].dropna().unique()])
    today_month_str = today.strftime("%Y-%m")
    default_idx = 0
    if today_month_str in months_avail:
        default_idx = months_avail.index(today_month_str)
    sel_month = st.selectbox(
        "📅 対象月で絞り込み",
        ["（すべて）"] + months_avail,
        index=(default_idx + 1) if default_idx > 0 else 0,
    )

filtered = proj.copy()
# 検索: item_id/item_code/product_name で
if search_query:
    q = search_query.lower().strip()
    mask = (
        filtered["item_id"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["item_code"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["product_name"].astype(str).str.lower().str.contains(q, na=False)
    )
    filtered = filtered[mask]

if sel_cat != "（すべて）":
    filtered = filtered[filtered["component_category"] == sel_cat]

status_map = {
    "🔴 ZERO（在庫切れ）": "ZERO",
    "🟠 UNDER（安全在庫割れ）": "UNDER",
    "🟢 OK（健全）":       "OK",
    "🟡 OVER（過剰）":     "OVER",
}
if sel_status != "（すべて）":
    filtered = filtered[filtered["policy_status"] == status_map[sel_status]]
if sel_month != "（すべて）":
    filtered = filtered[filtered["month_end_date"] == sel_month]

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材×月マトリクス (在庫状態を一覧)
# ────────────────────────────────────────────────────────
st.markdown(f"### 📊 部材×月 在庫状態マトリクス ({len(filtered):,} 行)")
st.caption("月末予測在庫が安全在庫の枠内にあるかを月別に確認。🔴/🟠 のセルが新規発注の候補。")

if filtered.empty:
    st.info("条件に該当する部材がありません。")
else:
    # 状態をアイコン化
    status_icon = {"ZERO": "🔴", "UNDER": "🟠", "OK": "🟢", "OVER": "🟡"}
    filtered = filtered.copy()
    filtered["状態"] = filtered["policy_status"].map(status_icon).fillna(filtered["policy_status"])

    show_cols = [
        ("item_id",            "部材ID"),
        ("item_code",          "品番"),
        ("product_name",       "部材名"),
        ("component_category", "カテゴリ"),
        ("month_end_date",     "対象月"),
        ("customer_stock_proj","月末予測在庫"),
        ("min_qty",            "安全在庫"),
        ("max_qty",            "上限在庫"),
        ("confirmed_order_qty","確定受注消費"),
        ("forecast_qty",       "FCST消費"),
        ("inbound_qty_order_linked","入荷予定"),
        ("状態",                "状態"),
    ]
    cols_present = [(k, v) for k, v in show_cols if k in filtered.columns]
    table = filtered[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    st.dataframe(table.sort_values(["対象月", "状態"]), hide_index=True, use_container_width=True, height=380)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材選択で時系列在庫推移を描画
# ────────────────────────────────────────────────────────
st.markdown("### 📈 部材別 在庫推移グラフ（安全在庫レンジとの対比）")
st.caption("選択部材の月末予測在庫を時系列で表示。min/max ラインを越えるタイミングを可視化。")

if not filtered.empty:
    pick = filtered[["item_id", "item_code", "product_name"]].drop_duplicates()
    pick["_label"] = (
        pick["item_id"].astype(str)
        + "  ｜  " + pick["item_code"].fillna("").astype(str)
        + "  ｜  " + pick["product_name"].fillna("").astype(str)
    )
    # デフォルト: 状態が悪い部材を3個
    bad = filtered[filtered["policy_status"].isin(["ZERO", "UNDER"])]
    if bad.empty:
        bad = filtered.head(3)
    default_ids = bad["item_id"].drop_duplicates().head(3).tolist()
    default_labels = pick[pick["item_id"].isin(default_ids)]["_label"].tolist()

    sel = st.multiselect(
        "📊 推移を見たい部材を選択（複数選択可）",
        pick["_label"].tolist(),
        default=default_labels[:3],
    )
    if sel:
        sel_ids = [s.split("  ｜  ", 1)[0].strip() for s in sel]
        sub = proj[proj["item_id"].isin(sel_ids)].copy()
        if not sub.empty:
            sub["month_dt"] = pd.to_datetime(sub["month_end_date"] + "-01", errors="coerce")
            sub = sub.sort_values(["item_id", "month_dt"])

            fig = go.Figure()
            palette = [colors["blue"], colors["orange"], colors["green"], colors["red"], colors["purple"]]
            for i, iid in enumerate(sel_ids):
                s = sub[sub["item_id"] == iid]
                if s.empty:
                    continue
                label = f"{s['item_code'].iloc[0]} / {s['product_name'].iloc[0]}"
                color = palette[i % len(palette)]
                fig.add_trace(go.Scatter(
                    x=s["month_dt"], y=s["customer_stock_proj"],
                    mode="lines+markers",
                    name=f"{label} 予測在庫",
                    line=dict(color=color, width=2),
                ))
                # min/max ライン (最後の値で代表させて表示)
                min_val = int(s["min_qty"].iloc[-1])
                max_val = int(s["max_qty"].iloc[-1])
                fig.add_trace(go.Scatter(
                    x=s["month_dt"], y=[min_val] * len(s),
                    mode="lines",
                    name=f"{label} 安全在庫",
                    line=dict(color=color, width=1, dash="dot"),
                    opacity=0.6,
                    showlegend=False,
                ))

            fig.update_layout(
                height=420,
                plot_bgcolor=colors["bg"],
                paper_bgcolor=colors["paper"],
                font=dict(color=colors["text"], size=12),
                xaxis=dict(title="年月", gridcolor=colors["grid"]),
                yaxis=dict(title="月末予測在庫（個）", gridcolor=colors["grid"]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=40, r=20, t=40, b=40),
            )
            add_today_vline(fig)
            st.plotly_chart(fig, use_container_width=True)

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
