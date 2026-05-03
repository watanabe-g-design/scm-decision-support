"""
4ルート調達比較表コンポーネント
================================
gold_procurement_options の1需要分（最大4ルート）を視覚的に比較表示する。

業務上の役割:
- 顧客（購買担当）が「どのルートで調達するか」を判断する中核UI
- ルートの優劣を自動判定せず、すべての選択肢を並列に提示
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st


# ルートタイプの日本語ラベル + 色
ROUTE_META: dict[str, dict[str, str]] = {
    "CUSTOMER_STOCK": {
        "label_jp": "① 顧客側在庫",
        "color":    "#58a6ff",   # 青
        "icon":     "🏭",
        "desc":     "自社倉庫の在庫から引当",
    },
    "MACNICA_FREE": {
        "label_jp": "② マクニカフリー在庫",
        "color":    "#2ea043",   # 緑
        "icon":     "📦",
        "desc":     "マクニカが顧客向けに引当済の在庫",
    },
    "EXISTING_PO": {
        "label_jp": "③ 既存発注残BL",
        "color":    "#ffa000",   # オレンジ
        "icon":     "📑",
        "desc":     "マクニカからメーカーへの既存発注を催促",
    },
    "NEW_ORDER": {
        "label_jp": "④ 新規追加発注",
        "color":    "#bc8cff",   # 紫
        "icon":     "🆕",
        "desc":     "新規にメーカーへ追加発注 (LT考慮)",
    },
}


def _confidence_badge(confidence: str) -> str:
    """確実度をHTMLバッジに変換"""
    color_map = {
        "確実":   "#2ea043",
        "見込み": "#ffa000",
        "要相談": "#ff4646",
    }
    color = color_map.get(confidence, "#8b949e")
    return (
        f'<span style="background:{color}22;color:{color};'
        f'padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;">'
        f"{confidence}</span>"
    )


def render_route_comparison(
    options_df: pd.DataFrame,
    requested_qty: int,
    requested_date: date,
) -> None:
    """
    4ルート比較を縦4枚カードで描画する。

    Parameters
    ----------
    options_df : pd.DataFrame
        gold_procurement_options を1需要に絞ったもの。
        必須列: route_type, available_qty, eta_date, confidence,
                shortage_qty, is_in_time, days_late, note
    requested_qty : int
        要求数量（カード上部に再表示）
    requested_date : date
        希望納期（カード上部に再表示）
    """
    # ルート順序を ROUTE_META のキー順に固定
    df = options_df.copy()
    df["_order"] = df["route_type"].map(
        {k: i for i, k in enumerate(ROUTE_META.keys())}
    )
    df = df.sort_values("_order").drop(columns="_order")

    # 要求情報のサマリ
    st.markdown(
        f"""
        <div style="
            background:#161b22;
            border:1px solid #30363d;
            padding:10px 14px;
            border-radius:6px;
            margin-bottom:14px;
        ">
            <span style="font-size:12px;color:#8b949e;">要求</span>
            <span style="font-size:14px;color:#e6edf3;margin-left:10px;">
                必要数量 <b>{requested_qty:,}</b> 個
            </span>
            <span style="font-size:14px;color:#e6edf3;margin-left:18px;">
                希望納期 <b>{requested_date.isoformat()}</b>
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 4列でカード表示
    cols = st.columns(4)
    for col, (_, row) in zip(cols, df.iterrows()):
        meta = ROUTE_META.get(row["route_type"], {})
        label = meta.get("label_jp", row["route_type"])
        color = meta.get("color", "#8b949e")
        icon = meta.get("icon", "•")
        desc = meta.get("desc", "")

        avail = int(row.get("available_qty", 0) or 0)
        eta = row.get("eta_date")
        eta_str = eta.isoformat() if hasattr(eta, "isoformat") else str(eta)
        confidence = str(row.get("confidence", "—"))
        shortage = int(row.get("shortage_qty", 0) or 0)
        is_in_time = bool(row.get("is_in_time", False))
        days_late = int(row.get("days_late", 0) or 0)
        note = str(row.get("note", "") or "")

        # 充足判定アイコン
        if shortage <= 0 and is_in_time:
            status_icon = "🟢"
            status_text = "充足"
        elif shortage <= 0 and not is_in_time:
            status_icon = "🟡"
            status_text = f"数量充足・{days_late}日遅延"
        elif shortage > 0 and is_in_time:
            status_icon = "🟡"
            status_text = f"間に合うが {shortage:,} 不足"
        else:
            status_icon = "🔴"
            status_text = f"不足 {shortage:,} ・ {days_late}日遅延"

        with col:
            st.markdown(
                f"""
                <div style="
                    background:#1c2128;
                    border:1px solid #30363d;
                    border-top:3px solid {color};
                    border-radius:6px;
                    padding:14px;
                    height:100%;
                    min-height:230px;
                ">
                    <div style="font-size:13px;font-weight:700;color:{color};margin-bottom:4px;">
                        {icon} {label}
                    </div>
                    <div style="font-size:10px;color:#8b949e;margin-bottom:12px;">
                        {desc}
                    </div>
                    <div style="font-size:11px;color:#8b949e;">確保可能数量</div>
                    <div style="font-size:22px;font-weight:700;color:#e6edf3;line-height:1.1;">
                        {avail:,}<span style="font-size:11px;color:#8b949e;"> 個</span>
                    </div>
                    <div style="font-size:11px;color:#8b949e;margin-top:10px;">到着予定日</div>
                    <div style="font-size:14px;color:#e6edf3;font-weight:600;">{eta_str}</div>
                    <div style="margin-top:10px;">{_confidence_badge(confidence)}</div>
                    <div style="
                        margin-top:10px;
                        padding-top:8px;
                        border-top:1px solid #30363d;
                        font-size:12px;
                        color:#e6edf3;
                    ">
                        {status_icon} {status_text}
                    </div>
                    <div style="font-size:10px;color:#8b949e;margin-top:6px;font-style:italic;">
                        {note}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
