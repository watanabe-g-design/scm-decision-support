import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import get_requirement_timeline
from services.risk_logic import build_inbound_outbound_df

st.set_page_config(page_title="入出庫リスト | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    timeline = get_requirement_timeline()

io_df = build_inbound_outbound_df(timeline)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 📝 入出庫リスト")
st.caption("入庫・出庫予定の詳細一覧")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
k1, k2, k3 = st.columns(3)

if "direction" in io_df.columns:
    inbound_count = int((io_df["direction"] == "入庫").sum())
    outbound_count = int((io_df["direction"] == "出庫").sum())
else:
    inbound_count = 0
    outbound_count = 0

if "quantity" in io_df.columns:
    qty = pd.to_numeric(io_df["quantity"], errors="coerce")
    direction_sign = io_df.get("direction", pd.Series(dtype=str))
    net_change = int(
        qty[direction_sign == "入庫"].sum() - qty[direction_sign == "出庫"].sum()
    ) if not qty.empty else 0
else:
    net_change = 0

k1.metric("📥 入庫件数", inbound_count)
k2.metric("📤 出庫件数", outbound_count)
k3.metric("📊 純変動", net_change)

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
st.markdown("#### フィルター")
f1, f2, f3 = st.columns(3)

with f1:
    if "direction" in io_df.columns:
        dir_opts = sorted(io_df["direction"].dropna().unique().tolist())
        sel_dir = st.multiselect("入出庫区分", dir_opts, default=[], key="io_dir")
    else:
        sel_dir = []
with f2:
    if "event_type" in io_df.columns:
        evt_opts = sorted(io_df["event_type"].dropna().unique().tolist())
        sel_evt = st.multiselect("イベント種別", evt_opts, default=[], key="io_evt")
    else:
        sel_evt = []
with f3:
    if "item_id" in io_df.columns:
        item_opts = sorted(io_df["item_id"].dropna().astype(str).unique().tolist())
        sel_item = st.multiselect("品目ID", item_opts, default=[], key="io_item")
    else:
        sel_item = []

filtered = io_df.copy()
if sel_dir and "direction" in filtered.columns:
    filtered = filtered[filtered["direction"].isin(sel_dir)]
if sel_evt and "event_type" in filtered.columns:
    filtered = filtered[filtered["event_type"].isin(sel_evt)]
if sel_item and "item_id" in filtered.columns:
    filtered = filtered[filtered["item_id"].astype(str).isin(sel_item)]

# ---------------------------------------------------------------------------
# Format date
# ---------------------------------------------------------------------------
if "event_date" in filtered.columns:
    filtered["event_date"] = pd.to_datetime(
        filtered["event_date"], format="mixed", errors="coerce"
    )
    filtered = filtered.sort_values("event_date")

# ---------------------------------------------------------------------------
# Column rename
# ---------------------------------------------------------------------------
rename_map = {
    "event_date": "日付",
    "direction": "入出庫区分",
    "event_type": "イベント種別",
    "item_id": "品目ID",
    "order_no": "関連注文番号",
    "quantity": "数量",
    "cumulative_balance": "累積残高",
}

display_cols = [c for c in rename_map if c in filtered.columns]
display_df = filtered[display_cols].copy()
if "event_date" in display_df.columns:
    display_df["event_date"] = display_df["event_date"].dt.strftime("%Y-%m-%d")
display_df = display_df.rename(columns=rename_map)

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
st.markdown("#### 一覧")
st.dataframe(display_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Timeline chart
# ---------------------------------------------------------------------------
if "cumulative_balance" in filtered.columns and "event_date" in filtered.columns and not filtered.empty:
    st.markdown("#### 累積残高タイムライン")

    chart_df = filtered.copy()
    chart_df["cumulative_balance"] = pd.to_numeric(chart_df["cumulative_balance"], errors="coerce")
    chart_df = chart_df.dropna(subset=["cumulative_balance", "event_date"])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=chart_df["event_date"],
        y=chart_df["cumulative_balance"],
        mode="markers+lines",
        name="累積残高",
        marker=dict(
            color=["#ff4646" if v < 0 else "#58a6ff" for v in chart_df["cumulative_balance"]],
            size=6,
        ),
        line=dict(color="#58a6ff", width=1.5),
    ))

    # Zero line
    fig.add_hline(y=0, line_dash="dash", line_color="#ffa000", opacity=0.7)

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9"),
        xaxis=dict(title="日付", gridcolor="#30363d"),
        yaxis=dict(title="累積残高", gridcolor="#30363d"),
        margin=dict(l=40, r=20, t=20, b=40),
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)
