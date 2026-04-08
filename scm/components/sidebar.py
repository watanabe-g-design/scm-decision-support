"""
共通サイドバー — 全ページで同一のナビゲーションを表示
"""
import streamlit as st


def render_sidebar():
    """全ページ共通のサイドバーを描画"""
    with st.sidebar:
        st.markdown("""<div style="padding:4px 0 12px 0;">
            <div style="font-size:15px;font-weight:700;color:#e6edf3;">SCM判断支援</div>
            <div style="font-size:10px;color:#8b949e;">Decision Support System</div>
        </div>""", unsafe_allow_html=True)

        st.success("🟢 Databricks接続済み", icon=None)

        st.markdown("---")

        st.page_link("app.py", label="🏠 経営コントロールタワー")
        st.page_link("pages/1_lt_intelligence.py", label="📊 LTインテリジェンス")
        st.page_link("pages/2_commit_supply_balance.py", label="⚖️ 納期コミット・需給バランス")
        st.page_link("pages/3_inventory_policy.py", label="📦 在庫基準逸脱レーダー")
        st.page_link("pages/4_network_warehouse.py", label="🗺️ 拠点・倉庫健全性")
        st.page_link("pages/5_data_reliability.py", label="🛠️ データ信頼性センター")
