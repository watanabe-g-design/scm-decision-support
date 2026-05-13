"""
共通サイドバー — 全ページで同一のナビゲーションを表示
========================================================
業務フロー順の構成:
  Top    : 📊 総合ダッシュボード
  Page 1 : 📅 製品FCST×部材必要量
  Page 2 : 🎯 調達アクションセンター     ★コア
  Page 3 : 🚨 緊急調達シミュレーター
  Page 4 : 📦 マクニカフリー在庫
  Page 5 : 🚚 物流トラッキング
  Page 6 : 🧩 製品BOM充足ビュー
  Page 7 : ⏳ リードタイム推移モニター
  Page 8 : 🏭 顧客在庫×安全在庫モニター
  Page 9 : 🔧 データパイプライン健全性
"""
import streamlit as st

from services.config import get_as_of_date_label_jp
from components.theme_toggle import render_theme_toggle


def render_sidebar():
    """全ページ共通のサイドバーを描画"""
    with st.sidebar:
        st.markdown(
            """<div style="padding:4px 0 12px 0;">
            <div style="font-size:15px;font-weight:700;">SCM 調達意思決定支援</div>
            <div style="font-size:10px;opacity:0.7;">Customer Procurement Decision App</div>
        </div>""",
            unsafe_allow_html=True,
        )

        st.success("🟢 Databricks接続済み", icon=None)
        st.caption(f"📅 本日: {get_as_of_date_label_jp()}")

        st.markdown("---")

        st.caption("【意思決定フロー】")
        st.page_link("app.py",                                 label="📊 総合ダッシュボード")
        st.page_link("pages/1_demand_timeline.py",             label="📅 製品FCST × 部材必要量")
        st.page_link("pages/2_action_center.py",               label="🎯 調達アクションセンター")
        st.page_link("pages/3_emergency_simulator.py",         label="🚨 緊急調達シミュレーター")

        st.markdown("")
        st.caption("【在庫・物流】")
        st.page_link("pages/4_macnica_free_inventory.py",      label="📦 マクニカフリー在庫")
        st.page_link("pages/5_logistics_geo.py",               label="🚚 物流トラッキング")
        st.page_link("pages/8_inventory_health.py",            label="🏭 顧客在庫×安全在庫")

        st.markdown("")
        st.caption("【供給能力モニター】")
        st.page_link("pages/6_bom_fulfillment.py",             label="🧩 製品BOM充足ビュー")
        st.page_link("pages/7_lead_time_trend.py",             label="⏳ リードタイム推移")

        st.markdown("")
        st.caption("【運用・保守】")
        st.page_link("pages/9_pipeline_health.py",             label="🔧 データパイプライン健全性")

        st.markdown("---")
        st.markdown(
            """<div style="font-size:10px;opacity:0.7;line-height:1.5;">
                <b>このアプリは</b><br>
                顧客（購買担当）と<br>マクニカ営業が<br>
                同じ画面で意思決定<br>するための統合UIです。
            </div>""",
            unsafe_allow_html=True,
        )
