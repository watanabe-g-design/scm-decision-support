"""
Commit & Supply — 納期コミットリスク (オーダー軸)
================================================
**MECE スコープ**: このページは **個別オーダー (sales_order_id)** を主役とした
リスク評価専用ページ。「このオーダーは納期通り出せるか?」が問い。

品目単位の月次在庫予測・ZERO/UNDER/OVER 判定は **D. 在庫基準逸脱レーダー** の責務。
品目軸で見たい場合は D へ誘導する。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.database import get_order_commit_risk, get_requirement_timeline
from components.global_filter import render_global_filter, apply_filters
from components.explain_panel import render_explain

st.set_page_config(page_title="納期コミットリスク | SCM判断支援", page_icon="⚖️", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

# ── タイトル ──────────────────────────────────
st.markdown("""
<div class="title-bar">
    <span class="logo">⚖️</span>
    <div>
        <div class="title">納期コミットリスク</div>
        <div class="subtitle">オーダー軸: このオーダーは納期通り出せるか?</div>
    </div>
</div>""", unsafe_allow_html=True)

st.info(
    "📌 **このページの軸 = オーダー (sales_order_id)**。\n\n"
    "個別オーダーの納期リスクと根拠を見るためのページです。"
    "**品目単位の月次在庫予測・ZERO/UNDER/OVER 判定**は "
    "サイドバーから **D. 在庫基準逸脱レーダー** (品目軸) を参照してください。",
    icon="ℹ️",
)

# ── データ読み込み ────────────────────────────
with st.spinner("データ読み込み中..."):
    orders_raw = get_order_commit_risk()
    req_raw = get_requirement_timeline()

# ══════════════════════════════════════════
# Priority KPI cards
# ══════════════════════════════════════════
crit_count = int((orders_raw["priority_rank"] == "Critical").sum())
high_count = int((orders_raw["priority_rank"] == "High").sum())
mid_count = int((orders_raw["priority_rank"] == "Mid").sum())
low_count = int((orders_raw["priority_rank"] == "Low").sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("🔴 Critical", f"{crit_count}件", help="3日以内に対応必要")
k2.metric("🟠 High", f"{high_count}件", help="7日以内に対応必要")
k3.metric("🔵 Mid", f"{mid_count}件", help="14日以内")
k4.metric("🟢 Low", f"{low_count}件", help="余裕あり")

# ══════════════════════════════════════════
# Global filter bar
# ══════════════════════════════════════════
mfr_list = sorted(orders_raw["supplier_name"].dropna().unique().tolist()) if "supplier_name" in orders_raw.columns else []
cat_list = sorted(orders_raw["component_category"].dropna().unique().tolist()) if "component_category" in orders_raw.columns else []
filters = render_global_filter(
    manufacturers=mfr_list,
    categories=cat_list,
    show_priority=True,
    show_scope=False,
)
orders = apply_filters(
    orders_raw, filters,
    manufacturer_col="supplier_name",
    category_col="component_category",
    priority_col="priority_rank",
)

# 日付列変換
date_cols = ["requested_delivery_date", "response_date", "earliest_ship_date", "deadline_date"]
for col in date_cols:
    if col in orders.columns:
        orders[col] = pd.to_datetime(orders[col], format="mixed", errors="coerce").dt.strftime("%Y-%m-%d").fillna("-")

# 表示用列の選定・リネーム
display_cols_map = {
    "sales_order_id": "SD#",
    "customer_name": "顧客",
    "part_number": "品番",
    "component_name": "部品名",
    "supplier_name": "メーカー",
    "remaining_qty": "数量",
    "requested_delivery_date": "指定納期",
    "response_date": "回答納期",
    "earliest_ship_date": "最短出荷可能日",
    "deadline_date": "デッドライン",
    "partial_available_qty": "分納可能数",
    "priority_rank": "調整Priority",
    "adjustment_action": "調整Action",
    "current_customer_stock": "顧客在庫",
}
avail_cols = {k: v for k, v in display_cols_map.items() if k in orders.columns}
orders_display = orders[list(avail_cols.keys())].rename(columns=avail_cols)

st.markdown(f"### オーダー一覧 ({len(orders_display)}件)")
st.caption("リスク順 (Critical → Low) でソート済み")
st.dataframe(orders_display, use_container_width=True, height=420, hide_index=True)

# CSV download
csv_data = orders_display.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="📥 CSV ダウンロード",
    data=csv_data,
    file_name="order_commit_risk.csv",
    mime="text/csv",
)

st.markdown("---")

# ══════════════════════════════════════════
# Per-order drill-down: 各オーダーの「根拠」表示
# (このページの主役: オーダー軸の判断材料)
# ══════════════════════════════════════════
st.markdown("### オーダー別の根拠 (上位20件)")
st.caption("各オーダーをクリックすると、該当部品の所要量タイムライン (在庫・受注・入荷) を表示します。これは**そのオーダーを納期通り出せるかどうかの根拠**です。")

for order_idx, (_, row) in enumerate(orders.head(20).iterrows()):
    sid = row.get("sales_order_id", "")
    pn = row.get("part_number", "")
    cn = row.get("component_name", "")
    prio = row.get("priority_rank", "Low")
    action = row.get("adjustment_action", "")
    icon = {"Critical": "🔴", "High": "🟠", "Mid": "🔵", "Low": "🟢"}.get(prio, "⚪")
    cid = row.get("component_id", "")

    with st.expander(f"{icon} {sid} — {pn} {cn} [{prio}] → {action}"):
        if cid and "item_id" in req_raw.columns:
            tl = req_raw[req_raw["item_id"] == cid].copy()
            if len(tl) > 0:
                tl["event_date"] = pd.to_datetime(tl["event_date"], format="mixed", errors="coerce")
                tl = tl.sort_values("event_date")

                tl_disp = tl[["event_date", "event_type", "order_no", "quantity", "cumulative_balance"]].copy()
                tl_disp["event_date"] = tl_disp["event_date"].dt.strftime("%Y-%m-%d").fillna("-")
                tl_disp = tl_disp.rename(columns={
                    "event_date": "発生日", "event_type": "イベント種別",
                    "order_no": "オーダー番号", "quantity": "数量", "cumulative_balance": "累積残",
                })
                st.dataframe(tl_disp, use_container_width=True, height=200, hide_index=True)

                # Cumulative chart
                fig = go.Figure()
                cum_vals = pd.to_numeric(tl["cumulative_balance"], errors="coerce")
                fig.add_trace(go.Scatter(
                    x=tl["event_date"], y=cum_vals,
                    mode="lines+markers",
                    line=dict(color="#58a6ff", width=2),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(88,166,255,0.08)",
                    name="累積残",
                ))
                fig.add_hline(y=0, line_dash="dash", line_color="#ff4646", line_width=1)
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#c9d1d9"), margin=dict(l=0, r=0, t=8, b=0), height=200,
                    xaxis=dict(gridcolor="#30363d"), yaxis=dict(gridcolor="#30363d", title="累積残"),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, key=f"order_chart_{order_idx}")

                # クロスリンク: 品目軸の詳細は D へ
                st.caption(
                    f"📦 この部品の月次在庫予測 (3〜6ヶ月) を見るには、サイドバーから "
                    f"**D. 在庫基準逸脱レーダー** を開き、フィルタで `{pn}` を選択してください。"
                )
            else:
                st.info("所要量データなし")
        else:
            st.info("所要量データなし")

st.markdown("---")
st.page_link("pages/3_inventory_policy.py", label="📦 品目軸で在庫予測を見る → 在庫基準逸脱レーダー")
