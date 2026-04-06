"""
Commit & Supply Balance — 納期コミット・需給バランス管理
仕様書§9準拠: オーダーリスク / 所要量一覧 / 月次需給バランス
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.database import get_order_commit_risk, get_requirement_timeline, get_balance_projection
from components.global_filter import render_global_filter, apply_filters
from components.explain_panel import render_explain

st.set_page_config(page_title="納期コミット・需給バランス | SCM判断支援", page_icon="⚖️", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

# ── タイトル ──────────────────────────────────
st.markdown("""
<div class="title-bar">
    <span class="logo">⚖️</span>
    <div>
        <div class="title">納期コミット・需給バランス</div>
        <div class="subtitle">オーダーリスク → 所要量一覧 → 月次バランス予測</div>
    </div>
</div>""", unsafe_allow_html=True)

# ── データ読み込み ────────────────────────────
with st.spinner("データ読み込み中..."):
    orders_raw = get_order_commit_risk()
    req_raw = get_requirement_timeline()
    balance_raw = get_balance_projection()

# ── 部品マスタ (Tab2/3のセレクト用) ───────────
comp_df = pd.read_csv(Path(__file__).parent.parent / "sample_data" / "components.csv")
part_options = comp_df.apply(lambda r: f"{r['part_number']} — {r['component_name']}", axis=1).tolist()
part_id_map = dict(zip(part_options, comp_df["component_id"].tolist()))

# ══════════════════════════════════════════
# タブ構成
# ══════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["📋 オーダーリスク", "📈 所要量一覧", "📊 月次需給バランス"])

# ══════════════════════════════════════════
# Tab 1: Order Risk
# ══════════════════════════════════════════
with tab1:
    # Priority KPI cards
    crit_count = int((orders_raw["priority_rank"] == "Critical").sum())
    high_count = int((orders_raw["priority_rank"] == "High").sum())
    mid_count = int((orders_raw["priority_rank"] == "Mid").sum())
    low_count = int((orders_raw["priority_rank"] == "Low").sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🔴 Critical", f"{crit_count}件", help="3日以内に対応必要")
    k2.metric("🟠 High", f"{high_count}件", help="7日以内に対応必要")
    k3.metric("🔵 Mid", f"{mid_count}件", help="14日以内")
    k4.metric("🟢 Low", f"{low_count}件", help="余裕あり")

    # Global filter bar
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
        "customer_name": "customer_name",
        "part_number": "part_number",
        "component_name": "component_name",
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
    st.dataframe(orders_display, use_container_width=True, height=420)

    # CSV download
    csv_data = orders_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="📥 CSV ダウンロード",
        data=csv_data,
        file_name="order_commit_risk.csv",
        mime="text/csv",
    )

    # Per-order expander: requirement timeline
    st.markdown("### 所要量一覧 (オーダー別)")
    for order_idx, (_, row) in enumerate(orders.head(20).iterrows()):
        sid = row.get("sales_order_id", "")
        pn = row.get("part_number", "")
        prio = row.get("priority_rank", "Low")
        action = row.get("adjustment_action", "")
        icon = {"Critical": "🔴", "High": "🟠", "Mid": "🔵", "Low": "🟢"}.get(prio, "⚪")
        cid = row.get("component_id", "")

        with st.expander(f"{icon} {sid} — {pn} [{prio}] {action}"):
            if cid and "item_id" in req_raw.columns:
                tl = req_raw[req_raw["item_id"] == cid].copy()
                if len(tl) > 0:
                    tl["event_date"] = pd.to_datetime(tl["event_date"], format="mixed", errors="coerce")
                    tl = tl.sort_values("event_date")

                    tl_disp = tl[["event_date", "event_type", "order_no", "quantity", "cumulative_balance"]].copy()
                    tl_disp["event_date"] = tl_disp["event_date"].dt.strftime("%Y-%m-%d").fillna("-")
                    st.dataframe(tl_disp, use_container_width=True, height=200)

                    # Cumulative chart
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=tl["event_date"], y=tl["cumulative_balance"],
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
                else:
                    st.info("所要量データなし")
            else:
                st.info("所要量データなし")

# ══════════════════════════════════════════
# Tab 2: Requirement Timeline
# ══════════════════════════════════════════
with tab2:
    st.markdown("### 所要量タイムライン")

    sel_part = st.selectbox("部品選択", part_options, key="req_part_select")
    sel_cid = part_id_map.get(sel_part)

    if sel_cid and "item_id" in req_raw.columns:
        tl2 = req_raw[req_raw["item_id"] == sel_cid].copy()
        if len(tl2) > 0:
            tl2["event_date"] = pd.to_datetime(tl2["event_date"], format="mixed", errors="coerce")
            tl2 = tl2.sort_values("event_date")

            # Transaction list
            tl2_disp = tl2[["event_date", "event_type", "order_no", "quantity"]].copy()
            if "allocated_qty" in tl2.columns:
                tl2_disp["allocated_qty"] = tl2["allocated_qty"]
            tl2_disp["cumulative_balance"] = tl2["cumulative_balance"]
            tl2_disp["event_date"] = pd.to_datetime(tl2_disp["event_date"], format="mixed", errors="coerce").dt.strftime("%Y-%m-%d").fillna("-")

            # Quantity display with +/- sign
            tl2_disp["quantity"] = tl2_disp["quantity"].apply(
                lambda x: f"+{int(x)}" if x > 0 else str(int(x))
            )

            st.dataframe(tl2_disp, use_container_width=True, height=350)

            # Cumulative chart with safety stock and zero line
            comp_row = comp_df[comp_df["component_id"] == sel_cid]
            safety_stock = int(comp_row["min_stock"].iloc[0]) if len(comp_row) > 0 else 0

            fig2 = go.Figure()
            cum_vals = tl2["cumulative_balance"].astype(float)
            colors = ["#ff4646" if v < 0 else "#58a6ff" for v in cum_vals]

            fig2.add_trace(go.Scatter(
                x=tl2["event_date"], y=cum_vals,
                mode="lines+markers",
                line=dict(color="#58a6ff", width=2.5),
                marker=dict(size=6, color=colors),
                name="累積残",
            ))
            # Safety stock line
            fig2.add_hline(
                y=safety_stock, line_dash="dot", line_color="#ffa000", line_width=1,
                annotation_text=f"安全在庫: {safety_stock:,}",
                annotation_font_color="#ffa000",
            )
            # Zero line
            fig2.add_hline(y=0, line_dash="dash", line_color="#ff4646", line_width=1,
                           annotation_text="ゼロライン", annotation_font_color="#ff4646")

            # Red fill for negative
            fig2.add_trace(go.Scatter(
                x=tl2["event_date"], y=cum_vals.clip(upper=0),
                fill="tozeroy", fillcolor="rgba(255,70,70,0.15)",
                line=dict(color="rgba(0,0,0,0)"), showlegend=False,
            ))

            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#c9d1d9"), margin=dict(l=0, r=0, t=20, b=0), height=320,
                xaxis=dict(gridcolor="#30363d", title="日付"),
                yaxis=dict(gridcolor="#30363d", title="累積残高"),
                showlegend=True,
                legend=dict(font=dict(color="#c9d1d9")),
            )
            st.plotly_chart(fig2, use_container_width=True, key="timeline_chart")

            # Explain panel for negative balances
            neg_events = tl2[tl2["cumulative_balance"].astype(float) < 0]
            if len(neg_events) > 0:
                first_neg = neg_events.iloc[0]
                render_explain(
                    title="在庫不足検知",
                    rationale=f"累積残が {first_neg['event_date'].strftime('%Y-%m-%d')} にマイナス転落 (残: {int(first_neg['cumulative_balance']):,})",
                    action="緊急補充 または 納期調整を検討",
                    severity="Critical",
                )
        else:
            st.info("選択部品の所要量データがありません")
    else:
        st.info("部品を選択してください")

# ══════════════════════════════════════════
# Tab 3: Monthly Balance
# ══════════════════════════════════════════
with tab3:
    st.markdown("### 月次需給バランス予測")

    col_sel, col_mode = st.columns([3, 2])
    with col_sel:
        sel_part3 = st.selectbox("部品選択", part_options, key="bal_part_select")
    with col_mode:
        display_mode = st.radio(
            "表示パターン",
            ["採用ロジック", "確定需要", "予測需要"],
            horizontal=True,
            key="bal_display_mode",
        )

    sel_cid3 = part_id_map.get(sel_part3)

    if sel_cid3 and "item_id" in balance_raw.columns:
        bal = balance_raw[balance_raw["item_id"] == sel_cid3].copy()

        # Filter until 2026-11
        bal["month_end_date"] = bal["month_end_date"].astype(str)
        bal = bal[bal["month_end_date"] <= "2026-11"].sort_values("month_end_date")

        if len(bal) > 0:
            # Numeric conversion
            for nc in ["inbound_qty_order_linked", "customer_stock_proj", "confirmed_order_qty",
                       "forecast_qty", "production_use_qty", "min_qty", "max_qty"]:
                if nc in bal.columns:
                    bal[nc] = pd.to_numeric(bal[nc], errors="coerce").fillna(0).astype(int)

            # Consumption calculation based on display mode
            if display_mode == "確定需要":
                bal["consumption_display"] = bal["confirmed_order_qty"]
            elif display_mode == "予測需要":
                bal["consumption_display"] = bal["forecast_qty"]
            else:  # 採用ロジック (default)
                bal["consumption_display"] = bal[["confirmed_order_qty", "forecast_qty"]].max(axis=1)

            # Recalculate customer_stock_proj based on display mode
            # Start from the first month's stock and re-project
            comp_row3 = comp_df[comp_df["component_id"] == sel_cid3]
            min_qty = int(comp_row3["min_stock"].iloc[0]) if len(comp_row3) > 0 else 0
            max_qty = int(comp_row3["max_stock"].iloc[0]) if len(comp_row3) > 0 else 0

            # Table display
            table_cols = ["month_end_date", "inbound_qty_order_linked", "customer_stock_proj",
                          "confirmed_order_qty", "forecast_qty", "consumption_display",
                          "min_qty", "max_qty", "policy_status"]
            avail_table = [c for c in table_cols if c in bal.columns]
            bal_disp = bal[avail_table].copy()
            bal_disp = bal_disp.rename(columns={
                "month_end_date": "月",
                "inbound_qty_order_linked": "入荷予定",
                "customer_stock_proj": "顧客在庫予測",
                "confirmed_order_qty": "確定需要",
                "forecast_qty": "予測需要",
                "consumption_display": "消費(採用)",
                "min_qty": "Min",
                "max_qty": "Max",
                "policy_status": "ステータス",
            })
            st.dataframe(bal_disp, use_container_width=True, height=320)

            # Bar + Line chart
            fig3 = go.Figure()

            # Inbound bars (green)
            fig3.add_trace(go.Bar(
                x=bal["month_end_date"], y=bal["inbound_qty_order_linked"],
                name="入荷予定", marker_color="#2ea043", opacity=0.85,
            ))
            # Consumption bars (red)
            fig3.add_trace(go.Bar(
                x=bal["month_end_date"], y=bal["consumption_display"],
                name="消費(採用)", marker_color="#ff4646", opacity=0.85,
            ))
            # Customer stock line (blue)
            fig3.add_trace(go.Scatter(
                x=bal["month_end_date"], y=bal["customer_stock_proj"],
                mode="lines+markers", name="顧客在庫予測",
                line=dict(color="#58a6ff", width=2.5), marker=dict(size=5),
            ))
            # FCST line (purple)
            fig3.add_trace(go.Scatter(
                x=bal["month_end_date"], y=bal["forecast_qty"],
                mode="lines+markers", name="FCST",
                line=dict(color="#bc8cff", width=2, dash="dot"), marker=dict(size=4),
            ))
            # Min line
            fig3.add_hline(
                y=min_qty, line_dash="dash", line_color="#ffa000", line_width=1,
                annotation_text=f"Min: {min_qty:,}",
                annotation_font_color="#ffa000",
                annotation_position="top left",
            )
            # Max line
            fig3.add_hline(
                y=max_qty, line_dash="dash", line_color="#58a6ff", line_width=1,
                annotation_text=f"Max: {max_qty:,}",
                annotation_font_color="#58a6ff",
                annotation_position="bottom left",
            )

            fig3.update_layout(
                barmode="group",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#c9d1d9"), margin=dict(l=0, r=0, t=20, b=0), height=380,
                xaxis=dict(gridcolor="#30363d", title="月", tickangle=-45),
                yaxis=dict(gridcolor="#30363d", title="数量"),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(color="#c9d1d9", size=11),
                ),
            )
            st.plotly_chart(fig3, use_container_width=True, key="balance_chart")

            # Explain panel for policy breaches
            breaches = bal[bal["policy_status"] != "OK"]
            if len(breaches) > 0:
                first_b = breaches.iloc[0]
                status = first_b["policy_status"]
                sev_map = {"ZERO": "Critical", "UNDER": "High", "OVER": "Mid"}
                action_map = {"ZERO": "緊急補充手配", "UNDER": "発注前倒し検討", "OVER": "発注抑制検討"}
                render_explain(
                    title=f"在庫ポリシー逸脱: {status}",
                    rationale=f"{first_b['month_end_date']} に顧客在庫予測 {int(first_b['customer_stock_proj']):,} (Min={int(first_b['min_qty']):,}, Max={int(first_b['max_qty']):,})",
                    action=action_map.get(status, "確認"),
                    severity=sev_map.get(status, "Low"),
                )
        else:
            st.info("選択部品のバランスデータがありません")
    else:
        st.info("部品を選択してください")
