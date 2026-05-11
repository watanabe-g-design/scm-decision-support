"""
📦 マクニカフリー在庫モニター
==============================
業務上の役割:
  - マクニカが顧客向けに引当済みの在庫を「部材別」に可視化
  - マクニカのフリー在庫は新子安ロジスティクスセンター1拠点に集約
    (顧客倉庫マスタとは別物。顧客倉庫=顧客自社倉庫)
  - 引当済みなので、マクニカ営業に相談すれば即時(今日+輸送日数)で受け取れる

Phase 6 改修:
  ✅ 倉庫別の棒グラフを撤廃、部材別 表示中心へ
  ✅ 「フリー在庫」の保管拠点=新子安1拠点 を明示
  ✅ 「顧客在庫 vs マクニカフリー」の対比ビューを残す
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
from components.inventory_badge import render_inventory_legend
from components.search_bar import render_search_bar, apply_component_search
from services.config import get_as_of_date
from services.database import (
    get_silver_macnica_free_inventory,
    get_silver_inventory_current,
    get_silver_components,
)

st.set_page_config(page_title="マクニカフリー在庫 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="マクニカが顧客向けに引当済の在庫を部材別に可視化")

colors = plot_colors()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    free_inv   = get_silver_macnica_free_inventory()
    cust_inv   = get_silver_inventory_current()
    components = get_silver_components()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 📦 マクニカフリー在庫モニター")
st.caption(
    "マクニカが貴社向けに引当済の在庫一覧。"
    "**全在庫は新子安ロジスティクスセンターを経由します** (マクニカ拠点は新子安1箇所)。"
    "通常LTを待たずに、相談ベースで引当 → 出荷可能です（マクニカ→貴社の輸送日数のみ）。"
)

render_inventory_legend()
st.info("📍 **マクニカのフリー在庫保管拠点**: 新子安ロジスティクスセンター（神奈川県横浜市鶴見区）のみ")

if free_inv.empty:
    st.warning("マクニカフリー在庫データが空です。")
    st.stop()

# 部材名を結合
free = free_inv.copy()
free["qty_available"] = pd.to_numeric(free["qty_available"], errors="coerce").fillna(0).astype(int)

if not components.empty:
    free = free.merge(
        components[["component_id", "part_number", "component_name", "component_category", "unit_price_usd", "base_lead_time_weeks"]],
        on="component_id", how="left",
    )

# ────────────────────────────────────────────────────────
# 検索 + フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（マクニカフリー在庫の表示範囲を絞る）")
search_query = render_search_bar(components, key="free_search")

fc1, fc2 = st.columns(2)
with fc1:
    cat_options = ["（すべて）"] + sorted([c for c in free["component_category"].dropna().unique()])
    sel_cat = st.selectbox(
        "🔍 部材カテゴリで絞り込み（例: MCU、SiC等）",
        cat_options,
    )
with fc2:
    qty_threshold = st.number_input(
        "📦 最低引当数量フィルター（この値以上の引当のみ表示）",
        min_value=0, value=0, step=10,
        help="少量引当を除外して、ある程度まとまった引当のみを見たいときに使用",
    )

if search_query:
    free = apply_component_search(free, search_query)
if sel_cat != "（すべて）":
    free = free[free["component_category"] == sel_cat]
if qty_threshold > 0:
    free = free[free["qty_available"] >= qty_threshold]

if free.empty:
    st.info("条件に該当する引当がありません。フィルターを調整してください。")
    st.stop()

st.markdown("---")

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
total_qty = int(free["qty_available"].sum())
n_components = int(free["component_id"].nunique())
total_value_usd = float((free["qty_available"] * pd.to_numeric(free.get("unit_price_usd", 0), errors="coerce").fillna(0)).sum())
high_value_items = int((free["qty_available"] * pd.to_numeric(free.get("unit_price_usd", 0), errors="coerce").fillna(0) > 5000).sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("📦 引当総数量", f"{total_qty:,} 個")
k2.metric("🔧 引当部材数", f"{n_components} 品目")
k3.metric("💰 在庫評価額（参考）", f"${total_value_usd:,.0f}")
k4.metric("🏷️ 高額引当（>$5000）", f"{high_value_items} 品目")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 引当数量（メインビュー: バーチャート + テーブル）
# ────────────────────────────────────────────────────────
st.markdown("### 📊 部材別 引当数量（マクニカ→貴社 即時出荷可能）")
st.caption("引当数量が大きい部材ほど、突発需要への即応性が高い候補です。")

by_comp = (
    free.groupby(["component_id", "part_number", "component_name", "component_category"], dropna=False, as_index=False)
    .agg(引当数量=("qty_available", "sum"))
    .sort_values("引当数量", ascending=False)
)

# Top20をバーチャート
top_n = min(20, len(by_comp))
fig = go.Figure()
top = by_comp.head(top_n).sort_values("引当数量", ascending=True)
fig.add_trace(go.Bar(
    x=top["引当数量"], y=top["part_number"].fillna("") + " / " + top["component_name"].fillna(""),
    orientation="h",
    marker_color=colors["green"],
    text=top["引当数量"].apply(lambda v: f"{v:,}"),
    textposition="outside",
    hovertemplate="%{y}<br>引当数量: %{x:,}<extra></extra>",
))
fig.update_layout(
    height=560,
    plot_bgcolor=colors["bg"],
    paper_bgcolor=colors["paper"],
    font=dict(color=colors["text"], size=11),
    xaxis=dict(title="引当数量（個）", gridcolor=colors["grid"]),
    yaxis=dict(title="", gridcolor=colors["grid"]),
    margin=dict(l=200, r=20, t=30, b=40),
    title=dict(text=f"📦 引当数量 Top {top_n}", font=dict(size=14, color=colors["text"])),
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 引当残量と顧客在庫の比較
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 部材別 引当残量（顧客在庫との比較）")
st.caption("「顧客在庫が少ない × マクニカフリー在庫が多い」部材は、活用余地が大きい候補です。")

free_by_comp = free.groupby("component_id", as_index=False)["qty_available"].sum()
free_by_comp.columns = ["component_id", "macnica_free_qty"]

if not cust_inv.empty:
    cust_by_comp = (
        cust_inv.groupby("component_id", as_index=False)["stock_qty"]
        .apply(lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum()))
    )
    cust_by_comp.columns = ["component_id", "customer_qty"]
else:
    cust_by_comp = pd.DataFrame(columns=["component_id", "customer_qty"])

cmp = free_by_comp.merge(cust_by_comp, on="component_id", how="left")
cmp["customer_qty"] = pd.to_numeric(cmp["customer_qty"], errors="coerce").fillna(0).astype(int)
if not components.empty:
    cmp = cmp.merge(
        components[["component_id", "part_number", "component_name", "component_category", "base_lead_time_weeks"]],
        on="component_id", how="left",
    )
cmp["合計"] = cmp["macnica_free_qty"] + cmp["customer_qty"]
cmp["活用余地スコア"] = cmp.apply(
    lambda r: r["macnica_free_qty"] / max(r["customer_qty"], 1) if r["macnica_free_qty"] > 0 else 0,
    axis=1,
).round(2)
cmp = cmp.sort_values("活用余地スコア", ascending=False)

show_cols_map = [
    ("component_id",      "部材ID"),
    ("part_number",       "品番"),
    ("component_name",    "部材名"),
    ("component_category","カテゴリ"),
    ("base_lead_time_weeks", "通常LT(週)"),
    ("macnica_free_qty",  "📦 マクニカフリー在庫"),
    ("customer_qty",      "🏭 顧客在庫"),
    ("合計",               "合計"),
    ("活用余地スコア",       "活用余地(F/C比)"),
]
cols_present = [(k, v) for k, v in show_cols_map if k in cmp.columns]
st.dataframe(
    cmp[[k for k, _ in cols_present]].rename(columns=dict(cols_present)),
    hide_index=True, use_container_width=True, height=400,
)
st.caption(
    "💡 **活用余地スコア**: マクニカフリー数量 ÷ 顧客在庫数量。値が大きいほど"
    "「顧客在庫が薄く、マクニカフリーで補填する価値が高い」部材です。"
)

# ────────────────────────────────────────────────────────
# 利用ガイド
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### ℹ️ マクニカフリー在庫の使い方")
st.markdown(
    """
- **特徴**: 既に貴社向けに引当済のため、マクニカ営業に相談すれば**通常の発注LTを待たずに**出荷手配が可能です。
- **保管場所**: マクニカ新子安ロジスティクスセンター（神奈川県横浜市鶴見区）一極集約。輸送日数は通常2〜3日。
- **典型的な使いどころ**:
    - 営業FCSTから漏れた突発需要が発生したとき
    - 既存発注残BLが間に合わないとき
    - 増産指示で必要数量が増えたとき
- **手順**:
    1. 「🚨 緊急調達シミュレーター」で対象部材・数量・希望納期を入力
    2. ② マクニカフリー在庫ルートで充足できるか確認
    3. 「マクニカ営業への相談用テキスト」を生成して送付
"""
)
