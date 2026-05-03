"""
在庫種別ラベル（バッジ）コンポーネント
======================================
本Appには2種類の在庫が存在し、混同すると業務判断を誤る:

  - 顧客在庫        (CUSTOMER): 顧客自身が顧客倉庫で保有
  - マクニカフリー在庫 (MACNICA): マクニカが顧客向けに引当済 (マクニカ倉庫保有)

両者を視覚的に明確に区別するための小さなバッジを提供する。
"""
from __future__ import annotations

import streamlit as st


# 在庫種別の定義
INVENTORY_KIND_META: dict[str, dict[str, str]] = {
    "CUSTOMER": {
        "label_jp": "顧客在庫",
        "color":    "#58a6ff",   # 青
        "icon":     "🏭",
        "desc":     "顧客自身が保有する自社倉庫内の在庫",
    },
    "MACNICA": {
        "label_jp": "マクニカフリー在庫",
        "color":    "#2ea043",   # 緑
        "icon":     "📦",
        "desc":     "マクニカが顧客向けに引当済の在庫 (マクニカ倉庫保有)",
    },
}


def inventory_badge_html(kind: str, *, with_desc: bool = False) -> str:
    """
    在庫種別バッジを HTML 文字列として返す（テーブルセル等に直接埋め込み可能）。

    Parameters
    ----------
    kind : str
        "CUSTOMER" または "MACNICA"
    with_desc : bool
        True の場合、ラベル右に説明文を併記
    """
    meta = INVENTORY_KIND_META.get(kind, {})
    label = meta.get("label_jp", kind)
    color = meta.get("color", "#8b949e")
    icon = meta.get("icon", "•")

    badge = (
        f'<span style="background:{color}22;color:{color};'
        f'padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;'
        f'border:1px solid {color}55;">'
        f"{icon} {label}</span>"
    )

    if with_desc:
        desc = meta.get("desc", "")
        return badge + f' <span style="font-size:10px;color:#8b949e;">{desc}</span>'
    return badge


def render_inventory_badge(kind: str, *, with_desc: bool = False) -> None:
    """在庫種別バッジを Streamlit 上に直接描画する"""
    st.markdown(inventory_badge_html(kind, with_desc=with_desc), unsafe_allow_html=True)


def render_inventory_legend() -> None:
    """
    在庫種別の凡例を描画する（ページ上部や在庫モニターで使用）。
    顧客 vs マクニカの違いを視覚的に明示する。
    """
    cols = st.columns(2)
    with cols[0]:
        render_inventory_badge("CUSTOMER", with_desc=True)
    with cols[1]:
        render_inventory_badge("MACNICA", with_desc=True)
