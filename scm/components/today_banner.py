"""
「今日」バナーコンポーネント
==============================
全ページの最上部に固定表示し、デモの基準日（「今日」）をユーザーに明示する。

業務上の重要性:
- 時系列グラフでどこが「実績」でどこが「予測」かを判別する基準点
- 「いつ時点の話か？」という顧客の混乱を防止
"""
import streamlit as st

from services.config import get_as_of_date, get_as_of_date_label_jp
from styles import is_light_theme


def render_today_banner(extra_note: str | None = None) -> None:
    """
    「今日」バナーを描画する。全ページで `inject_css` の直後に呼ぶ。
    """
    label = get_as_of_date_label_jp()
    iso = get_as_of_date().isoformat()

    # ライト専用 (Phase 8 ダークモード廃止)
    bg = "linear-gradient(90deg, #eff6ff 0%, #f8fafc 100%)"
    border = "#bfdbfe"
    accent = "#2563eb"
    text_main = "#0f172a"
    text_sub = "#475569"

    extra_html = ""
    if extra_note:
        extra_html = (
            f'<span style="font-size:12px;color:{text_sub};margin-left:14px;">'
            f"｜ {extra_note}</span>"
        )

    st.markdown(
        f"""
        <div style="
            background: {bg};
            border: 1px solid {border};
            border-left: 4px solid {accent};
            padding: 10px 16px;
            margin: 0 0 18px 0;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        ">
            <div>
                <span style="font-size:11px;color:{text_sub};letter-spacing:1px;">AS OF</span>
                <span style="
                    font-size:16px;
                    font-weight:700;
                    color:{text_main};
                    margin-left:10px;
                ">📅 本日: {label}</span>
                <span style="font-size:11px;color:{text_sub};margin-left:10px;">({iso})</span>
                {extra_html}
            </div>
            <div style="font-size:11px;color:{text_sub};">
                ← 実績 ｜ 予測 →
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
