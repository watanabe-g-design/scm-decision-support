"""
共通サイドバー — 全ページで同一のナビゲーションを表示
========================================================
新App構成（業務フロー順）:
  Top    : 📊 総合ダッシュボード        (app.py)
  Page 1 : 📅 製品FCST×部材必要量       (pages/1_demand_timeline.py)
  Page 2 : 🎯 調達アクションセンター    (pages/2_action_center.py) ★コア
  Page 3 : 🚨 緊急調達シミュレーター    (pages/3_emergency_simulator.py)
  Page 4 : 📦 マクニカフリー在庫        (pages/4_macnica_free_inventory.py)
  Page 5 : 🚚 物流トラッキング          (pages/5_logistics_geo.py)
"""
import streamlit as st

from services.config import get_as_of_date_label_jp


def render_sidebar():
    """全ページ共通のサイドバーを描画"""
    with st.sidebar:
        st.markdown(
            """<div style="padding:4px 0 12px 0;">
            <div style="font-size:15px;font-weight:700;color:#e6edf3;">SCM 調達意思決定支援</div>
            <div style="font-size:10px;color:#8b949e;">Customer Procurement Decision App</div>
        </div>""",
            unsafe_allow_html=True,
        )

        st.success("🟢 Databricks接続済み", icon=None)
        st.caption(f"📅 本日: {get_as_of_date_label_jp()}")

        st.markdown("---")

        st.page_link("app.py",                                 label="📊 総合ダッシュボード")
        st.page_link("pages/1_demand_timeline.py",             label="📅 製品FCST × 部材必要量")
        st.page_link("pages/2_action_center.py",               label="🎯 調達アクションセンター")
        st.page_link("pages/3_emergency_simulator.py",         label="🚨 緊急調達シミュレーター")
        st.page_link("pages/4_macnica_free_inventory.py",      label="📦 マクニカフリー在庫")
        st.page_link("pages/5_logistics_geo.py",               label="🚚 物流トラッキング")

        st.markdown("---")
        st.markdown(
            """<div style="font-size:10px;color:#8b949e;line-height:1.5;">
                <b>このアプリは</b><br>
                顧客（購買担当）と<br>マクニカ営業が<br>
                同じ画面で意思決定<br>するための統合UIです。
            </div>""",
            unsafe_allow_html=True,
        )
