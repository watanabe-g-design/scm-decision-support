"""
テーマ切替トグル
=================
サイドバーに配置するダーク/ライトテーマ切替ボタン。
セッションステート ``theme_mode`` で管理。
"""
import streamlit as st


def render_theme_toggle() -> None:
    """サイドバーに小さなテーマ切替UIを描画する"""
    if "theme_mode" not in st.session_state:
        st.session_state["theme_mode"] = "dark"

    current = st.session_state["theme_mode"]
    label = "🌙 ダークモード" if current == "dark" else "☀️ ライトモード"

    cols = st.columns([1, 1])
    with cols[0]:
        if st.button("🌙 ダーク", use_container_width=True, key="_theme_dark",
                     type="primary" if current == "dark" else "secondary"):
            st.session_state["theme_mode"] = "dark"
            st.rerun()
    with cols[1]:
        if st.button("☀️ ライト", use_container_width=True, key="_theme_light",
                     type="primary" if current == "light" else "secondary"):
            st.session_state["theme_mode"] = "light"
            st.rerun()
