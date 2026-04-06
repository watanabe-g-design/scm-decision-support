"""
Explain Panel — 根拠説明パネル
仕様書§7準拠: 各KPI・各行の根拠を1文で説明する横断コンポーネント
"""
import streamlit as st


def render_explain(title: str, rationale: str, action: str = None,
                   due: str = None, severity: str = None):
    """
    根拠説明パネルを描画
    仕様書: "Every alert should have: severity, rationale, due timing, and suggested action"
    """
    sev_colors = {
        "Critical": "#ff4646", "High": "#ffa000", "Mid": "#58a6ff",
        "Low": "#2ea043", "ZERO": "#ff4646", "UNDER": "#ffa000", "OVER": "#58a6ff",
    }
    color = sev_colors.get(severity, "#8b949e")

    parts = []
    if severity:
        parts.append(f'<span style="color:{color};font-weight:600;">[{severity}]</span>')
    parts.append(f'<span style="color:#e6edf3;">{rationale}</span>')
    if due:
        parts.append(f'<span style="color:#8b949e;">期限: {due}</span>')
    if action:
        parts.append(f'→ <span style="color:#58a6ff;font-weight:600;">{action}</span>')

    st.markdown(f"""
    <div style="background:rgba(255,255,255,0.02);border-left:3px solid {color};
                border-radius:4px;padding:8px 12px;margin-bottom:4px;font-size:12px;">
        <div style="font-size:13px;font-weight:500;color:#e6edf3;margin-bottom:2px;">{title}</div>
        <div>{" | ".join(parts)}</div>
    </div>""", unsafe_allow_html=True)


def render_metric_explain(metric_name: str, value: str, formula: str,
                           source: str, threshold: str = None):
    """
    KPI定義の説明パネル
    仕様書: "No KPI cards without definitions"
    """
    parts = [f"<b>{metric_name}</b>: {value}"]
    parts.append(f'<span style="color:#8b949e;">計算式: {formula}</span>')
    parts.append(f'<span style="color:#8b949e;">ソース: {source}</span>')
    if threshold:
        parts.append(f'<span style="color:#ffa000;">閾値: {threshold}</span>')

    st.markdown(f"""
    <div style="background:rgba(88,166,255,0.04);border:1px solid rgba(88,166,255,0.15);
                border-radius:6px;padding:8px 12px;margin-bottom:4px;font-size:11px;">
        {"<br>".join(parts)}
    </div>""", unsafe_allow_html=True)
