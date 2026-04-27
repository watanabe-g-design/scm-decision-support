import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import (
    get_silver_forecasts, get_silver_bom, get_silver_components,
    get_silver_products, get_silver_customers, get_silver_purchase_orders,
    get_silver_sales_orders, get_silver_inventory_current,
    get_balance_projection, get_requirement_timeline,
)
from services.risk_logic import (
    build_forecast_risk_df, build_order_delivery_risk_df,
    build_inventory_df, build_monthly_balance_df, build_overview_kpis,
)

st.set_page_config(page_title="Overview | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    forecasts = get_silver_forecasts()
    bom = get_silver_bom()
    components = get_silver_components()
    products = get_silver_products()
    customers = get_silver_customers()
    purchase_orders = get_silver_purchase_orders()
    sales_orders = get_silver_sales_orders()
    inventory_current = get_silver_inventory_current()
    balance = get_balance_projection()
    timeline = get_requirement_timeline()

# ---------------------------------------------------------------------------
# Build risk DataFrames
# ---------------------------------------------------------------------------
fc_risk = build_forecast_risk_df(forecasts, bom, components, products, customers, purchase_orders)
od_risk = build_order_delivery_risk_df(sales_orders)
inv = build_inventory_df(inventory_current, components, sales_orders)
mb = build_monthly_balance_df(balance)
kpis = build_overview_kpis(fc_risk, od_risk, inv, mb)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 📊 Overview")
st.caption("リスクサマリーと優先対応事項")

# ---------------------------------------------------------------------------
# KPI Cards - Row 1
# ---------------------------------------------------------------------------
r1c1, r1c2, r1c3, r1c4 = st.columns(4)
r1c1.metric("🔴 発注リスク Critical", kpis["forecast_critical"])
r1c2.metric("🟠 発注リスク High", kpis["forecast_high"])
r1c3.metric("🔴 納品リスク Critical", kpis["delivery_critical"])
r1c4.metric("🟠 納品リスク High", kpis["delivery_high"])

# ---------------------------------------------------------------------------
# KPI Cards - Row 2
# ---------------------------------------------------------------------------
r2c1, r2c2, r2c3, r2c4 = st.columns(4)
r2c1.metric("📦 在庫不足品目", kpis["inventory_shortage"])
r2c2.metric("⚠️ 在庫 Low 品目", kpis["inventory_low"])
r2c3.metric("📉 不足月数", kpis["months_with_shortage"])
r2c4.metric("⏰ 納期超過", kpis["delivery_overdue"])

st.markdown("---")

# ---------------------------------------------------------------------------
# Top 10 tables
# ---------------------------------------------------------------------------
left, right = st.columns(2)

with left:
    st.subheader("発注リスク Top 10")
    fc_top = fc_risk[fc_risk["status"].isin(["Critical", "High"])].head(10)
    display_cols_fc = [
        c for c in ["customer_name", "part_number", "component_name", "days_remaining", "status"]
        if c in fc_top.columns
    ]
    if not fc_top.empty:
        st.dataframe(fc_top[display_cols_fc], hide_index=True, use_container_width=True)
    else:
        st.info("Critical / High の発注リスクはありません。")

with right:
    st.subheader("納品リスク Top 10")
    od_top = od_risk[od_risk["delivery_status"].isin(["Critical", "High"])].head(10)
    display_cols_od = [
        c for c in ["sales_order_id", "customer_name", "days_to_delivery", "delivery_status"]
        if c in od_top.columns
    ]
    if not od_top.empty:
        st.dataframe(od_top[display_cols_od], hide_index=True, use_container_width=True)
    else:
        st.info("Critical / High の納品リスクはありません。")

# ---------------------------------------------------------------------------
# Page links
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("##### 詳細ページ")
lnk1, lnk2, lnk3, lnk4, lnk5 = st.columns(5)
with lnk1:
    st.page_link("pages/1_forecast_risk.py", label="📋 発注リスク", icon="📋")
with lnk2:
    st.page_link("pages/2_order_delivery_risk.py", label="🚚 納品リスク", icon="🚚")
with lnk3:
    st.page_link("pages/3_monthly_balance.py", label="📈 月次需給", icon="📈")
with lnk4:
    st.page_link("pages/4_inventory.py", label="📦 在庫確認", icon="📦")
with lnk5:
    st.page_link("pages/5_inbound_outbound.py", label="📝 入出庫", icon="📝")
