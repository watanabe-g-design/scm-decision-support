import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import (
    get_silver_inventory_current, get_silver_components, get_silver_sales_orders,
)
from services.risk_logic import build_inventory_df

st.set_page_config(page_title="在庫確認 | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    inventory_current = get_silver_inventory_current()
    components = get_silver_components()
    sales_orders = get_silver_sales_orders()

inv = build_inventory_df(inventory_current, components, sales_orders)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 📦 在庫確認")
st.caption("マクニカ側の実在庫と利用可能在庫を確認")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
k1, k2, k3 = st.columns(3)
status_counts = inv["inventory_status"].value_counts() if "inventory_status" in inv.columns else pd.Series(dtype=int)
k1.metric("🔴 Shortage", int(status_counts.get("Shortage", 0)))
k2.metric("🟡 Low", int(status_counts.get("Low", 0)))
k3.metric("🟢 Available", int(status_counts.get("Available", 0)))

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
st.markdown("#### フィルター")
f1, f2, f3 = st.columns(3)

with f1:
    if "supplier_name" in inv.columns:
        sup_opts = sorted(inv["supplier_name"].dropna().unique().tolist())
        sel_sup = st.multiselect("メーカー", sup_opts, default=[], key="inv_sup")
    else:
        sel_sup = []
with f2:
    if "component_category" in inv.columns:
        cat_opts = sorted(inv["component_category"].dropna().unique().tolist())
        sel_cat = st.multiselect("カテゴリ", cat_opts, default=[], key="inv_cat")
    else:
        sel_cat = []
with f3:
    if "inventory_status" in inv.columns:
        ist_opts = sorted(inv["inventory_status"].dropna().unique().tolist())
        sel_ist = st.multiselect("ステータス", ist_opts, default=[], key="inv_ist")
    else:
        sel_ist = []

filtered = inv.copy()
if sel_sup and "supplier_name" in filtered.columns:
    filtered = filtered[filtered["supplier_name"].isin(sel_sup)]
if sel_cat and "component_category" in filtered.columns:
    filtered = filtered[filtered["component_category"].isin(sel_cat)]
if sel_ist and "inventory_status" in filtered.columns:
    filtered = filtered[filtered["inventory_status"].isin(sel_ist)]

# ---------------------------------------------------------------------------
# Column rename
# ---------------------------------------------------------------------------
rename_map = {
    "part_number": "品番",
    "component_name": "部品名",
    "component_category": "カテゴリ",
    "supplier_name": "メーカー",
    "month_end_inventory": "月末在庫",
    "allocated_qty": "引当済",
    "available_inventory": "利用可能在庫",
    "spot_order_capacity": "突発受注対応可能数",
    "min_stock": "安全在庫基準",
    "inventory_status": "ステータス",
}

display_cols = [c for c in rename_map if c in filtered.columns]
display_df = filtered[display_cols].rename(columns=rename_map)

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
st.markdown("#### 一覧")
st.dataframe(display_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# CSV download
# ---------------------------------------------------------------------------
csv = display_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("📥 CSVダウンロード", csv, "inventory.csv", "text/csv")

# ---------------------------------------------------------------------------
# Bar chart - bottom 20 by available_inventory
# ---------------------------------------------------------------------------
if "available_inventory" in filtered.columns and not filtered.empty:
    st.markdown("#### 利用可能在庫 ワースト20")

    chart_df = filtered.copy()
    chart_df["available_inventory"] = pd.to_numeric(chart_df["available_inventory"], errors="coerce")
    chart_df = chart_df.dropna(subset=["available_inventory"])
    chart_df = chart_df.nsmallest(20, "available_inventory")

    label_col = "part_number" if "part_number" in chart_df.columns else chart_df.index.astype(str)
    if isinstance(label_col, str):
        labels = chart_df[label_col].astype(str)
    else:
        labels = label_col

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=chart_df["available_inventory"],
        y=labels,
        orientation="h",
        marker_color=["#ff4646" if v < 0 else "#ffa000" if v < 50 else "#2ea043"
                       for v in chart_df["available_inventory"]],
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9"),
        xaxis=dict(title="利用可能在庫", gridcolor="#30363d"),
        yaxis=dict(gridcolor="#30363d", autorange="reversed"),
        margin=dict(l=120, r=20, t=20, b=40),
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)
