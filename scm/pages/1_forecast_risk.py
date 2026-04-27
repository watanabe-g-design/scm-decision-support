import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import (
    get_silver_forecasts, get_silver_bom, get_silver_components,
    get_silver_products, get_silver_customers, get_silver_purchase_orders,
)
from services.risk_logic import build_forecast_risk_df

st.set_page_config(page_title="発注リスク | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    forecasts = get_silver_forecasts()
    bom = get_silver_bom()
    components = get_silver_components()
    products = get_silver_products()
    customers = get_silver_customers()
    purchase_orders = get_silver_purchase_orders()

fc_risk = build_forecast_risk_df(forecasts, bom, components, products, customers, purchase_orders)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 📋 フォーキャスト・発注リスク確認")
st.caption("フォーキャストに対して、今発注すべきもの・LT割れしそうなものを把握")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
k1, k2, k3, k4 = st.columns(4)
status_counts = fc_risk["status"].value_counts()
k1.metric("🔴 Critical", int(status_counts.get("Critical", 0)))
k2.metric("🟠 High", int(status_counts.get("High", 0)))
k3.metric("🟡 Medium", int(status_counts.get("Medium", 0)))
k4.metric("🟢 Normal", int(status_counts.get("Normal", 0)))

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
st.markdown("#### フィルター")
f1, f2, f3 = st.columns(3)

with f1:
    cust_opts = sorted(fc_risk["customer_name"].dropna().unique().tolist())
    sel_cust = st.multiselect("顧客", cust_opts, default=[], key="fc_cust")
with f2:
    stat_opts = sorted(fc_risk["status"].dropna().unique().tolist())
    sel_stat = st.multiselect("ステータス", stat_opts, default=[], key="fc_stat")
with f3:
    cat_col = "component_category"
    if cat_col in fc_risk.columns:
        cat_opts = sorted(fc_risk[cat_col].dropna().unique().tolist())
        sel_cat = st.multiselect("カテゴリ", cat_opts, default=[], key="fc_cat")
    else:
        sel_cat = []

filtered = fc_risk.copy()
if sel_cust:
    filtered = filtered[filtered["customer_name"].isin(sel_cust)]
if sel_stat:
    filtered = filtered[filtered["status"].isin(sel_stat)]
if sel_cat and cat_col in filtered.columns:
    filtered = filtered[filtered[cat_col].isin(sel_cat)]

# ---------------------------------------------------------------------------
# Format dates
# ---------------------------------------------------------------------------
if "forecast_month" in filtered.columns:
    filtered["forecast_month"] = pd.to_datetime(
        filtered["forecast_month"], format="mixed", errors="coerce"
    ).dt.strftime("%Y-%m")

if "order_required_date" in filtered.columns:
    filtered["order_required_date"] = pd.to_datetime(
        filtered["order_required_date"], format="mixed", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Column rename mapping
# ---------------------------------------------------------------------------
rename_map = {
    "customer_name": "顧客",
    "product_name": "製品",
    "part_number": "品番",
    "component_name": "部品名",
    "component_category": "カテゴリ",
    "supplier_name": "メーカー",
    "forecast_month": "使用予定月",
    "component_demand_qty": "FCST数量",
    "base_lead_time_weeks": "標準LT(週)",
    "order_required_date": "発注必要日",
    "days_remaining": "残り日数",
    "ordered_qty": "発注済",
    "unordered_qty": "未発注",
    "status": "ステータス",
}

display_cols = [c for c in rename_map if c in filtered.columns]
display_df = filtered[display_cols].rename(columns=rename_map)

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
st.markdown("#### 一覧")
st.dataframe(display_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# CSV Download
# ---------------------------------------------------------------------------
csv = display_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("📥 CSVダウンロード", csv, "forecast_risk.csv", "text/csv")
