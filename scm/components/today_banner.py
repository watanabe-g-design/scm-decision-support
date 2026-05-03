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


def render_today_banner(extra_note: str | None = None) -> None:
    """
    「今日」バナーを描画する。全ページで `inject_css` の直後に呼ぶ。

    Parameters
    ----------
    extra_note : str | None
        バナー右側に表示する補足テキスト（例: ページ特有の注意書き）。
    """
    label = get_as_of_date_label_jp()
    iso = get_as_of_date().isoformat()

    extra_html = ""
    if extra_note:
        extra_html = (
            f'<span style="font-size:12px;color:#8b949e;margin-left:14px;">'
            f"｜ {extra_note}</span>"
        )

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #1c2128 0%, #161b22 100%);
            border: 1px solid #30363d;
            border-left: 4px solid #58a6ff;
            padding: 10px 16px;
            margin: 0 0 18px 0;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        ">
            <div>
                <span style="font-size:11px;color:#8b949e;letter-spacing:1px;">AS OF</span>
                <span style="
                    font-size:16px;
                    font-weight:700;
                    color:#e6edf3;
                    margin-left:10px;
                ">📅 本日: {label}</span>
                <span style="font-size:11px;color:#8b949e;margin-left:10px;">({iso})</span>
                {extra_html}
            </div>
            <div style="font-size:11px;color:#8b949e;">
                ← 実績 ｜ 予測 →
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
