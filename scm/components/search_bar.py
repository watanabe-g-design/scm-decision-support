"""
検索バーコンポーネント (型番/部材名/需要IDで横断検索)
======================================================
各ページのヘッダーに配置し、品番(part_number)で素早く目的部材を見つける。
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


def render_search_bar(
    components_df: pd.DataFrame,
    key: str = "search_query",
    placeholder: str = "🔍 品番 / 部材名 / 部材ID で検索",
) -> str:
    """検索バーを描画し、入力値を返す"""
    val = st.text_input(
        "🔍 部材検索（品番・部材名・部材ID）",
        value=st.session_state.get(key, ""),
        placeholder=placeholder,
        key=key,
        help="品番(例: FP-MC-0001)、部材名の一部、または部材ID(例: C001)で部分一致検索します",
    )
    return val


def apply_component_search(
    df: pd.DataFrame,
    query: str,
    component_cols: tuple[str, ...] = ("component_id", "part_number", "component_name"),
) -> pd.DataFrame:
    """検索クエリに一致する行に絞り込む（OR条件、部分一致、大文字小文字区別なし）"""
    if not query or df.empty:
        return df
    q = query.lower().strip()
    mask = pd.Series(False, index=df.index)
    for col in component_cols:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.lower().str.contains(q, na=False)
    return df[mask]
