import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import get_balance_projection
from services.risk_logic import build_monthly_balance_df

st.set_page_config(page_title="月次需給バランス | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    balance = get_balance_projection()

mb = build_monthly_balance_df(balance)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 📈 月次需給バランス")
st.caption("月ごとの不足・余剰を確認")

# ---------------------------------------------------------------------------
# Component selector
# ---------------------------------------------------------------------------
if "item_code" in mb.columns and "product_name" in mb.columns:
    mb["_selector"] = mb["item_code"].astype(str) + " / " + mb["product_name"].astype(str)
elif "item_code" in mb.columns:
    mb["_selector"] = mb["item_code"].astype(str)
else:
    mb["_selector"] = mb.index.astype(str)

selector_opts = sorted(mb["_selector"].dropna().unique().tolist())
selected = st.selectbox("品目を選択", selector_opts, index=0 if selector_opts else None)

if selected:
    filt = mb[mb["_selector"] == selected].copy()
else:
    filt = mb.head(0)

# ---------------------------------------------------------------------------
# Format month
# ---------------------------------------------------------------------------
if "month_end_date" in filt.columns:
    filt["month_end_date"] = pd.to_datetime(
        filt["month_end_date"], format="mixed", errors="coerce"
    )
    filt = filt.sort_values("month_end_date")

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
rename_map = {
    "month_end_date": "月",
    "forecast_qty": "FCST",
    "confirmed_order_qty": "受注",
    "customer_stock_proj": "在庫予測",
    "inbound_qty_order_linked": "入荷予定",
    "supply_available": "供給可能",
    "shortage_surplus": "不足/余剰",
    "balance_status": "ステータス",
}

display_cols = [c for c in rename_map if c in filt.columns]
display_df = filt[display_cols].copy()
if "month_end_date" in display_df.columns:
    display_df["month_end_date"] = display_df["month_end_date"].dt.strftime("%Y-%m")
display_df = display_df.rename(columns=rename_map)

st.markdown("#### 月次データ")
st.dataframe(display_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------
if not filt.empty and "month_end_date" in filt.columns:
    st.markdown("#### 需給チャート")

    x_vals = filt["month_end_date"].dt.strftime("%Y-%m")

    fig = go.Figure()

    # Bars
    if "forecast_qty" in filt.columns:
        fig.add_trace(go.Bar(
            x=x_vals,
            y=pd.to_numeric(filt["forecast_qty"], errors="coerce"),
            name="FCST",
            marker_color="#ff4646",
            opacity=0.8,
        ))

    if "confirmed_order_qty" in filt.columns:
        fig.add_trace(go.Bar(
            x=x_vals,
            y=pd.to_numeric(filt["confirmed_order_qty"], errors="coerce"),
            name="受注",
            marker_color="#ffa000",
            opacity=0.8,
        ))

    # Lines
    if "customer_stock_proj" in filt.columns:
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=pd.to_numeric(filt["customer_stock_proj"], errors="coerce"),
            name="在庫予測",
            mode="lines+markers",
            line=dict(color="#58a6ff", width=2),
        ))

    if "min_qty" in filt.columns:
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=pd.to_numeric(filt["min_qty"], errors="coerce"),
            name="安全在庫",
            mode="lines",
            line=dict(color="#ffa000", width=2, dash="dash"),
        ))

    # Highlight shortage months
    if "shortage_surplus" in filt.columns:
        shortage_mask = pd.to_numeric(filt["shortage_surplus"], errors="coerce") < 0
        shortage_months = x_vals[shortage_mask]
        for sm in shortage_months:
            fig.add_vline(x=sm, line_dash="dot", line_color="#ff4646", opacity=0.4)

    fig.update_layout(
        barmode="group",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9"),
        xaxis=dict(gridcolor="#30363d"),
        yaxis=dict(gridcolor="#30363d"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=40, b=40),
    )

    st.plotly_chart(fig, use_container_width=True)
