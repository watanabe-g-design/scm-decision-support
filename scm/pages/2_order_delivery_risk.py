import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from components.sidebar import render_sidebar
from services.database import get_silver_sales_orders
from services.risk_logic import build_order_delivery_risk_df

st.set_page_config(page_title="納品リスク | SCM需給バランス", layout="wide")
inject_css()
render_sidebar()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
with st.spinner("データを読み込んでいます..."):
    sales_orders = get_silver_sales_orders()

od_risk = build_order_delivery_risk_df(sales_orders)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.markdown("## 🚚 受注・納品リスク確認")
st.caption("受注済みオーダーの顧客希望納期に間に合うかを確認")
st.info(
    "📌 この画面は「新規発注を促す画面」ではありません。"
    "すでに受注済みのオーダーの納品リスクを判断する画面です。"
)

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
k1, k2, k3, k4 = st.columns(4)
status_counts = od_risk["delivery_status"].value_counts()
k1.metric("🔴 Critical", int(status_counts.get("Critical", 0)))
k2.metric("🟠 High", int(status_counts.get("High", 0)))
k3.metric("🟡 Medium", int(status_counts.get("Medium", 0)))
k4.metric("🟢 Normal", int(status_counts.get("Normal", 0)))

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
st.markdown("#### フィルター")
f1, f2 = st.columns(2)

with f1:
    cust_opts = sorted(od_risk["customer_name"].dropna().unique().tolist())
    sel_cust = st.multiselect("顧客", cust_opts, default=[], key="od_cust")
with f2:
    stat_opts = sorted(od_risk["delivery_status"].dropna().unique().tolist())
    sel_stat = st.multiselect("ステータス", stat_opts, default=[], key="od_stat")

filtered = od_risk.copy()
if sel_cust:
    filtered = filtered[filtered["customer_name"].isin(sel_cust)]
if sel_stat:
    filtered = filtered[filtered["delivery_status"].isin(sel_stat)]

# ---------------------------------------------------------------------------
# Format dates
# ---------------------------------------------------------------------------
for dcol in ["requested_delivery_date", "response_date", "deadline_date"]:
    if dcol in filtered.columns:
        filtered[dcol] = pd.to_datetime(
            filtered[dcol], format="mixed", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Column rename
# ---------------------------------------------------------------------------
rename_map = {
    "sales_order_id": "注文番号",
    "customer_name": "顧客",
    "product_name": "製品",
    "order_qty": "受注数量",
    "requested_delivery_date": "指定納期",
    "response_date": "回答納期",
    "deadline_date": "デッドライン",
    "days_to_delivery": "指定納期まで(日)",
    "response_deadline_diff": "回答-DL差(日)",
    "delivery_status": "ステータス",
}

display_cols = [c for c in rename_map if c in filtered.columns]
display_df = filtered[display_cols].rename(columns=rename_map)

# ---------------------------------------------------------------------------
# Main table
# ---------------------------------------------------------------------------
st.markdown("#### 一覧")
st.dataframe(display_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# CSV download
# ---------------------------------------------------------------------------
csv = display_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("📥 CSVダウンロード", csv, "order_delivery_risk.csv", "text/csv")

# ---------------------------------------------------------------------------
# Detail expanders for top Critical/High
# ---------------------------------------------------------------------------
st.markdown("#### Critical / High 詳細")
top_risk = od_risk[od_risk["delivery_status"].isin(["Critical", "High"])].head(15)

for _, row in top_risk.iterrows():
    label = f"{row.get('sales_order_id', '?')} — {row.get('customer_name', '')} [{row.get('delivery_status', '')}]"
    with st.expander(label):
        detail_cols = [c for c in od_risk.columns if c in row.index]
        detail_data = {c: [row[c]] for c in detail_cols}
        st.dataframe(pd.DataFrame(detail_data), hide_index=True, use_container_width=True)

        # Mini timeline if date columns exist
        dates = {}
        for dcol, dlabel in [
            ("requested_delivery_date", "指定納期"),
            ("response_date", "回答納期"),
            ("deadline_date", "デッドライン"),
        ]:
            if dcol in row.index and pd.notna(row[dcol]):
                dates[dlabel] = row[dcol]

        if dates:
            st.markdown("**タイムライン**")
            for dlabel, dval in dates.items():
                st.markdown(f"- {dlabel}: `{dval}`")
