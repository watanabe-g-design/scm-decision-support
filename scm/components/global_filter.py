"""
Global Filter Bar — 全画面共通フィルター
仕様書§6準拠: スナップショット日/顧客/メーカー/カテゴリ/倉庫/危険度/時間粒度/表示スコープ
フィルター状態はsession_stateで画面間を跨いで維持
"""
import streamlit as st
import pandas as pd


def init_filter_state():
    """フィルター状態の初期化 (未初期化時のみ)"""
    defaults = {
        "gf_customer": [],
        "gf_manufacturer": [],
        "gf_category": [],
        "gf_warehouse": [],
        "gf_priority": [],
        "gf_scope": "顧客在庫中心",
        "gf_active_text": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def render_global_filter(
    customers: list = None,
    manufacturers: list = None,
    categories: list = None,
    warehouses: list = None,
    show_priority: bool = True,
    show_scope: bool = True,
):
    """
    Global Filter Barを描画し、フィルター状態をsession_stateに保存する。
    返り値: dict of current filter values
    """
    init_filter_state()

    with st.container():
        st.markdown("""<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                    padding:8px 12px;margin-bottom:12px;">""", unsafe_allow_html=True)

        # 基本フィルター (常時表示: 最大6個)
        cols = st.columns([2, 2, 2, 2, 1, 1])

        with cols[0]:
            if manufacturers:
                st.session_state.gf_manufacturer = st.multiselect(
                    "メーカー", manufacturers, default=st.session_state.gf_manufacturer,
                    placeholder="全メーカー", key="gf_mfr_widget", label_visibility="collapsed")

        with cols[1]:
            if categories:
                st.session_state.gf_category = st.multiselect(
                    "カテゴリ", categories, default=st.session_state.gf_category,
                    placeholder="全カテゴリ", key="gf_cat_widget", label_visibility="collapsed")

        with cols[2]:
            if warehouses:
                st.session_state.gf_warehouse = st.multiselect(
                    "倉庫", warehouses, default=st.session_state.gf_warehouse,
                    placeholder="全倉庫", key="gf_wh_widget", label_visibility="collapsed")

        with cols[3]:
            if show_priority:
                st.session_state.gf_priority = st.multiselect(
                    "危険度", ["Critical","High","Mid","Low"],
                    default=st.session_state.gf_priority,
                    placeholder="全て", key="gf_prio_widget", label_visibility="collapsed")

        with cols[4]:
            if show_scope:
                st.session_state.gf_scope = st.selectbox(
                    "スコープ", ["顧客在庫中心","顧客+商社補助"],
                    index=0, key="gf_scope_widget", label_visibility="collapsed")

        with cols[5]:
            # フィルター状態表示
            active = []
            if st.session_state.gf_manufacturer: active.append(f"メーカー:{len(st.session_state.gf_manufacturer)}")
            if st.session_state.gf_category: active.append(f"カテゴリ:{len(st.session_state.gf_category)}")
            if st.session_state.gf_warehouse: active.append(f"倉庫:{len(st.session_state.gf_warehouse)}")
            if st.session_state.gf_priority: active.append(f"危険度:{len(st.session_state.gf_priority)}")
            text = " | ".join(active) if active else "フィルターなし"
            st.markdown(f"<span style='font-size:10px;color:#8b949e;'>{text}</span>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

    return get_current_filters()


def get_current_filters() -> dict:
    """現在のフィルター状態を取得"""
    init_filter_state()
    return {
        "customer": st.session_state.gf_customer,
        "manufacturer": st.session_state.gf_manufacturer,
        "category": st.session_state.gf_category,
        "warehouse": st.session_state.gf_warehouse,
        "priority": st.session_state.gf_priority,
        "scope": st.session_state.gf_scope,
    }


def apply_filters(df: pd.DataFrame, filters: dict,
                   manufacturer_col: str = "manufacturer_name",
                   category_col: str = "component_category",
                   warehouse_col: str = "warehouse_id",
                   priority_col: str = "priority_rank") -> pd.DataFrame:
    """DataFrameにGlobal Filterを適用"""
    result = df.copy()
    if filters.get("manufacturer") and manufacturer_col in result.columns:
        result = result[result[manufacturer_col].isin(filters["manufacturer"])]
    if filters.get("category") and category_col in result.columns:
        result = result[result[category_col].isin(filters["category"])]
    if filters.get("warehouse") and warehouse_col in result.columns:
        result = result[result[warehouse_col].isin(filters["warehouse"])]
    if filters.get("priority") and priority_col in result.columns:
        result = result[result[priority_col].isin(filters["priority"])]
    return result
