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

from styles import is_light_theme


# ルートタイプの日本語ラベル + 色
ROUTE_META: dict[str, dict[str, str]] = {
    "CUSTOMER_STOCK": {
        "label_jp": "① 顧客側在庫",
        "color":    "#58a6ff",
        "icon":     "🏭",
        "desc":     "自社倉庫の在庫から引当（即時）",
        "tooltip":  "顧客の自社倉庫が現在保有している在庫。今日すぐ利用可能。今後の他需要による消費見込みを差し引いた『実効在庫』を表示。",
    },
    "MACNICA_FREE": {
        "label_jp": "② マクニカフリー在庫",
        "color":    "#2ea043",
        "icon":     "📦",
        "desc":     "マクニカが顧客向けに引当済の在庫",
        "tooltip":  "マクニカ側がこの顧客向けに事前に引当済の在庫。マクニカ営業に相談すれば通常LTを待たずに出荷手配可能。",
    },
    "EXISTING_PO": {
        "label_jp": "③ 既存発注残BL",
        "color":    "#ffa000",
        "icon":     "📑",
        "desc":     "マクニカからメーカーへ既発注分を催促",
        "tooltip":  "マクニカが既にメーカーへ発注済の未入荷分。最早の入荷予定日を表示。遅延がある場合は『要相談』表示。",
    },
    "NEW_ORDER": {
        "label_jp": "④ 新規追加発注",
        "color":    "#bc8cff",
        "icon":     "🆕",
        "desc":     "新規にメーカーへ追加発注（LT考慮）",
        "tooltip":  "今から追加でメーカーへ発注する場合のオプション。部材ごとのリードタイム（base_lead_time_weeks）を考慮したETAを表示。",
    },
}


def _confidence_badge(confidence: str) -> str:
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


def render_route_legend() -> None:
    """4ルートの説明凡例を描画 (各ページ冒頭で1度呼ぶ)"""
    with st.expander("ℹ️ 4つの調達ルートの説明", expanded=False):
        for k, meta in ROUTE_META.items():
            st.markdown(
                f"<div style='margin-bottom:6px;'>"
                f"<span style='color:{meta['color']};font-weight:600;'>{meta['icon']} {meta['label_jp']}</span>"
                f": {meta['tooltip']}"
                f"</div>",
                unsafe_allow_html=True,
            )


def render_route_comparison(
    options_df: pd.DataFrame,
    requested_qty: int,
    requested_date: date,
) -> None:
    """4ルート比較を縦4枚カードで描画"""
    df = options_df.copy()
    df["_order"] = df["route_type"].map({k: i for i, k in enumerate(ROUTE_META.keys())})
    df = df.sort_values("_order").drop(columns="_order")

    # テーマカラー
    if is_light_theme():
        card_bg = "#f6f8fa"
        border = "#d0d7de"
        text_main = "#1f2328"
        text_sub = "#656d76"
    else:
        card_bg = "#1c2128"
        border = "#30363d"
        text_main = "#e6edf3"
        text_sub = "#8b949e"

    st.markdown(
        f"""
        <div style="
            background:{card_bg};
            border:1px solid {border};
            padding:10px 14px;
            border-radius:6px;
            margin-bottom:14px;
        ">
            <span style="font-size:12px;color:{text_sub};">要求</span>
            <span style="font-size:14px;color:{text_main};margin-left:10px;">
                必要数量 <b>{requested_qty:,}</b> 個
            </span>
            <span style="font-size:14px;color:{text_main};margin-left:18px;">
                希望納期 <b>{requested_date.isoformat()}</b>
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

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

        if shortage <= 0 and is_in_time:
            status_icon = "🟢"
            status_text = "充足"
        elif shortage <= 0 and not is_in_time:
            status_icon = "🟡"
            status_text = f"数量充足・{days_late}日遅延"
        elif shortage > 0:
            # 数量が不足 → 「間に合わない」のと同義として扱う (Phase 7修正)
            status_icon = "🔴"
            if days_late > 0:
                status_text = f"不足 {shortage:,} ・ {days_late}日遅延"
            else:
                status_text = f"不足 {shortage:,}"
        else:
            status_icon = "🔴"
            status_text = f"不足 {shortage:,} ・ {days_late}日遅延"

        with col:
            st.markdown(
                f"""
                <div style="
                    background:{card_bg};
                    border:1px solid {border};
                    border-top:3px solid {color};
                    border-radius:6px;
                    padding:14px;
                    height:100%;
                    min-height:240px;
                ">
                    <div style="font-size:13px;font-weight:700;color:{color};margin-bottom:4px;">
                        {icon} {label}
                    </div>
                    <div style="font-size:10px;color:{text_sub};margin-bottom:12px;">
                        {desc}
                    </div>
                    <div style="font-size:11px;color:{text_sub};">確保可能数量</div>
                    <div style="font-size:22px;font-weight:700;color:{text_main};line-height:1.1;">
                        {avail:,}<span style="font-size:11px;color:{text_sub};"> 個</span>
                    </div>
                    <div style="font-size:11px;color:{text_sub};margin-top:10px;">到着予定日</div>
                    <div style="font-size:14px;color:{text_main};font-weight:600;">{eta_str}</div>
                    <div style="margin-top:10px;">{_confidence_badge(confidence)}</div>
                    <div style="
                        margin-top:10px;
                        padding-top:8px;
                        border-top:1px solid {border};
                        font-size:12px;
                        color:{text_main};
                    ">
                        {status_icon} {status_text}
                    </div>
                    <div style="font-size:10px;color:{text_sub};margin-top:6px;font-style:italic;">
                        {note}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
