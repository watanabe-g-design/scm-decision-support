"""
📦 マクニカフリー在庫モニター
==============================
業務上の役割:
  - 「マクニカが顧客向けに引当済の在庫」を顧客が直接確認できる画面
  - 顧客側在庫 (silver_inventory_current) との混同を強く防ぐ
  - 倉庫別、部材別の引当残量を可視化
  - 引当済みなので、相談すれば即時 (今日+輸送日数) で受け取れる安心感を提供
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.inventory_badge import render_inventory_badge, render_inventory_legend
from services.config import get_as_of_date
from services.database import (
    get_silver_macnica_free_inventory,
    get_silver_inventory_current,
    get_silver_components,
    get_silver_warehouses,
)

st.set_page_config(page_title="マクニカフリー在庫 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="マクニカが顧客向けに引当済の在庫を可視化")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    free_inv   = get_silver_macnica_free_inventory()
    cust_inv   = get_silver_inventory_current()
    components = get_silver_components()
    warehouses = get_silver_warehouses()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 📦 マクニカフリー在庫モニター")
st.caption(
    "マクニカが貴社向けに引当済の在庫一覧。"
    "通常の調達LTを待たずに、相談ベースで引当 → 出荷可能です（マクニカ→貴社の輸送日数のみ）。"
)

# 在庫種別の凡例（混同防止のため毎ページ表示）
render_inventory_legend()
st.markdown("")

if free_inv.empty:
    st.warning("マクニカフリー在庫データが空です。")
    st.stop()

# 部材名・倉庫名を結合
free = free_inv.copy()
free["qty_available"] = pd.to_numeric(free["qty_available"], errors="coerce").fillna(0).astype(int)

if not components.empty:
    free = free.merge(
        components[["component_id", "part_number", "component_name", "component_category", "unit_price_usd"]],
        on="component_id", how="left",
    )
if not warehouses.empty:
    free = free.merge(
        warehouses[["warehouse_id", "warehouse_name", "prefecture"]],
        on="warehouse_id", how="left",
    )

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
total_qty = int(free["qty_available"].sum())
n_components = int(free["component_id"].nunique())
n_warehouses = int(free["warehouse_id"].nunique())
total_value_usd = float((free["qty_available"] * pd.to_numeric(free.get("unit_price_usd", 0), errors="coerce").fillna(0)).sum())

k1.metric("📦 引当総数量", f"{total_qty:,} 個")
k2.metric("🔧 引当部材数", f"{n_components} 品目")
k3.metric("🏭 保管倉庫数", f"{n_warehouses} 拠点")
k4.metric("💰 在庫評価額（参考）", f"${total_value_usd:,.0f}")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 引当倉庫別の数量
# ────────────────────────────────────────────────────────
st.markdown("### 🏭 倉庫別 引当数量")
by_wh = (
    free.groupby(["warehouse_id", "warehouse_name", "prefecture"], dropna=False, as_index=False)
    .agg(引当数量=("qty_available", "sum"), 引当部材数=("component_id", "nunique"))
    .sort_values("引当数量", ascending=False)
)

c1, c2 = st.columns([3, 2])

with c1:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=by_wh["warehouse_name"], y=by_wh["引当数量"],
        marker_color="#2ea043",
        text=by_wh["引当数量"].apply(lambda v: f"{v:,}"),
        textposition="auto",
    ))
    fig.update_layout(
        height=320,
        plot_bgcolor="#0d1117",
        paper_bgcolor="#0d1117",
        font=dict(color="#e6edf3", size=12),
        xaxis=dict(title="倉庫", gridcolor="#30363d"),
        yaxis=dict(title="引当数量（個）", gridcolor="#30363d"),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.dataframe(
        by_wh.rename(columns={
            "warehouse_id": "倉庫ID",
            "warehouse_name": "倉庫名",
            "prefecture": "都道府県",
        }),
        hide_index=True, use_container_width=True, height=320,
    )

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 引当残量と顧客在庫の比較
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 部材別 引当残量（顧客在庫との比較）")
st.caption("「顧客在庫が少ない × マクニカフリー在庫が多い」部材は、活用余地が大きい候補です。")

# 部材別: マクニカフリー在庫合計
free_by_comp = free.groupby("component_id", as_index=False)["qty_available"].sum()
free_by_comp.columns = ["component_id", "macnica_free_qty"]

# 部材別: 顧客在庫合計
if not cust_inv.empty:
    cust_by_comp = (
        cust_inv.groupby("component_id", as_index=False)["stock_qty"]
        .apply(lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum()))
    )
    cust_by_comp.columns = ["component_id", "customer_qty"]
else:
    cust_by_comp = pd.DataFrame(columns=["component_id", "customer_qty"])

# 結合 + 部材名
cmp = free_by_comp.merge(cust_by_comp, on="component_id", how="left")
cmp["customer_qty"] = pd.to_numeric(cmp["customer_qty"], errors="coerce").fillna(0).astype(int)
if not components.empty:
    cmp = cmp.merge(
        components[["component_id", "part_number", "component_name", "component_category"]],
        on="component_id", how="left",
    )
cmp["合計"] = cmp["macnica_free_qty"] + cmp["customer_qty"]
cmp = cmp.sort_values("macnica_free_qty", ascending=False)

# 表示
show_cols_map = [
    ("component_id",      "部材ID"),
    ("part_number",       "品番"),
    ("component_name",    "部材名"),
    ("component_category", "カテゴリ"),
    ("macnica_free_qty",  "📦 マクニカフリー在庫"),
    ("customer_qty",      "🏭 顧客在庫"),
    ("合計",               "合計"),
]
cols_present = [(k, v) for k, v in show_cols_map if k in cmp.columns]
st.dataframe(
    cmp[[k for k, _ in cols_present]].rename(columns=dict(cols_present)),
    hide_index=True, use_container_width=True, height=400,
)

# ────────────────────────────────────────────────────────
# 利用ガイド
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### ℹ️ マクニカフリー在庫の使い方")
st.markdown(
    """
- **特徴**: 既に貴社向けに引当済のため、マクニカ営業に相談すれば**通常の発注LTを待たずに**出荷手配が可能です。
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
