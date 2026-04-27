"""
共通サイドバー — 全ページで同一のナビゲーションを表示
"""
import streamlit as st


def render_sidebar():
    """全ページ共通のサイドバーを描画"""
    with st.sidebar:
        st.markdown("""<div style="padding:4px 0 12px 0;">
            <div style="font-size:15px;font-weight:700;color:#e6edf3;">SCM 需給バランス</div>
            <div style="font-size:10px;color:#8b949e;">Supply Chain Decision Support</div>
        </div>""", unsafe_allow_html=True)

        st.success("🟢 Databricks接続済み", icon=None)

        st.markdown("---")

        st.page_link("app.py",                                label="📊 Overview")
        st.page_link("pages/1_forecast_risk.py",              label="📋 発注リスク確認")
        st.page_link("pages/2_order_delivery_risk.py",        label="🚚 受注・納品リスク")
        st.page_link("pages/3_monthly_balance.py",            label="📈 月次需給バランス")
        st.page_link("pages/4_inventory.py",                  label="📦 在庫確認")
        st.page_link("pages/5_inbound_outbound.py",           label="📝 入出庫リスト")
