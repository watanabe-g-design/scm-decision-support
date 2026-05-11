"""
検索バー / 部材セレクタコンポーネント
======================================================
各ページのヘッダーに配置し、部材を素早く絞り込むためのUI部品。

Phase 7 改修:
  - 部材カテゴリフィルターは全ページから撤廃
  - 代わりに「部材セレクタ (multi-select)」を提供
  - キーワード検索バーは継続提供
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
        "🔍 部材検索（品番・部材名・部材ID で部分一致）",
        value=st.session_state.get(key, ""),
        placeholder=placeholder,
        key=key,
        help="品番(例: FP-MC-0001)、部材名の一部、または部材ID(例: C001)で部分一致検索します",
    )
    return val


def render_component_selector(
    components_df: pd.DataFrame,
    *,
    key: str = "comp_select",
    label: str = "🔧 部材で絞り込み（部材を直接選択、空欄で全て表示）",
    placeholder: str = "部材を選択（複数選択可、空欄=全部材）",
) -> list[str]:
    """
    部材を直接選択するマルチセレクトUI。返り値は component_id のリスト。
    """
    if components_df is None or components_df.empty:
        return []
    df = components_df.copy()
    # ラベル: 品番 ｜ 部材名 (ID)
    df["_label"] = (
        df.get("part_number", pd.Series("")).fillna("").astype(str)
        + "  ｜  " + df.get("component_name", pd.Series("")).fillna("").astype(str)
        + " (" + df.get("component_id", pd.Series("")).fillna("").astype(str) + ")"
    )
    df = df.sort_values("_label")

    selected = st.multiselect(
        label,
        options=df["_label"].tolist(),
        default=[],
        placeholder=placeholder,
        key=key,
    )
    if not selected:
        return []
    sel_ids = []
    for s in selected:
        # 末尾の "(C001)" から ID を抽出
        if "(" in s and s.endswith(")"):
            sel_ids.append(s.rsplit("(", 1)[1].rstrip(")"))
    return sel_ids


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


def apply_component_id_filter(
    df: pd.DataFrame,
    selected_ids: list[str],
    id_col: str = "component_id",
) -> pd.DataFrame:
    """component_idリストに合致する行のみ返す。空リストならフィルターしない。"""
    if not selected_ids or df.empty or id_col not in df.columns:
        return df
    return df[df[id_col].isin(selected_ids)]
