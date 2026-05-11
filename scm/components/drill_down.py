"""
ドリルダウン・ボタン
====================
KPIカードの直下に「詳細を見る」ボタンを配置し、
クリックすると指定ページに遷移しつつフィルター状態をセッションに渡す。
"""
from __future__ import annotations

from typing import Optional

import streamlit as st


def render_drill_down_button(
    label: str,
    target_page: str,
    filter_payload: Optional[dict] = None,
    key: str = "drill",
) -> None:
    """
    KPIカードの下に「詳細を見る」ボタンを配置。

    Parameters
    ----------
    label : str
        ボタンに表示するテキスト
    target_page : str
        遷移先ページパス（例: "pages/2_action_center.py"）
    filter_payload : dict
        遷移先ページに渡したいフィルター状態。
        受け取り側で st.session_state["drill_filter"] から読み取る。
    key : str
        ボタンの一意キー
    """
    if st.button(label, key=key, use_container_width=True):
        if filter_payload:
            st.session_state["drill_filter"] = filter_payload
        st.switch_page(target_page)


def pop_drill_filter() -> dict:
    """
    遷移先ページで初期フィルター状態を読み取り、消費する。
    呼び出し後は session_state から削除される。
    """
    payload = st.session_state.get("drill_filter", None)
    if payload is not None:
        # 1回読み取ったら破棄（リロードでループしないよう）
        del st.session_state["drill_filter"]
        return payload
    return {}
