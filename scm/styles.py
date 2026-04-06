"""
SCMデモアプリ用 ダークテーマCSS
GeoGenieのスタイリングを踏襲
"""

DARK_THEME_CSS = """
<style>
/* ── Foundation ───────────────────────────── */
:root {
    --bg-primary: #0d1117;
    --bg-secondary: #161b22;
    --bg-tertiary: #1c2128;
    --border: #30363d;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --accent-blue: #58a6ff;
    --accent-green: #2ea043;
    --accent-orange: #ffa000;
    --accent-red: #ff4646;
    --accent-purple: #bc8cff;
}

/* 背景・テキスト基本設定 */
.stApp, [data-testid="stAppViewContainer"],
[data-testid="stHeader"],
section[data-testid="stSidebar"] {
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}

/* Streamlit ヘッダー・フッター非表示 */
header[data-testid="stHeader"] {
    background: var(--bg-primary) !important;
    border-bottom: 1px solid var(--border) !important;
}
footer { display: none !important; }
#MainMenu { visibility: hidden !important; }

/* ── サイドバー ───────────────────────────── */
section[data-testid="stSidebar"] {
    background-color: var(--bg-secondary) !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span {
    color: var(--text-primary) !important;
}

/* サイドバーのナビゲーション */
[data-testid="stSidebarNavItems"] a {
    color: var(--text-secondary) !important;
    border-radius: 6px !important;
    padding: 6px 12px !important;
    transition: all 0.15s ease !important;
}
[data-testid="stSidebarNavItems"] a:hover {
    color: var(--text-primary) !important;
    background: rgba(88,166,255,0.08) !important;
}
[data-testid="stSidebarNavItems"] a[aria-current="page"] {
    background: rgba(88,166,255,0.12) !important;
    color: var(--accent-blue) !important;
    font-weight: 600 !important;
}

/* ── タイトルバー ─────────────────────────── */
.title-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0 16px 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
}
.title-bar .logo {
    font-size: 28px;
    line-height: 1;
}
.title-bar .title {
    font-size: 22px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.4px;
}
.title-bar .subtitle {
    font-size: 12px;
    color: var(--text-secondary);
    margin-top: 2px;
}
.title-bar .badge {
    margin-left: auto;
    background: rgba(88,166,255,0.12);
    color: var(--accent-blue);
    padding: 4px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    border: 1px solid rgba(88,166,255,0.2);
}

/* ── メトリクスカード ─────────────────────── */
[data-testid="stMetric"] {
    background: var(--bg-secondary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 12px 16px !important;
}
[data-testid="stMetricLabel"] {
    color: var(--text-secondary) !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.6px !important;
}
[data-testid="stMetricValue"] {
    color: var(--text-primary) !important;
    font-weight: 700 !important;
}
[data-testid="stMetricDelta"] {
    font-size: 11px !important;
}

/* ── ボタン ───────────────────────────────── */
.stButton > button {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    transition: all 0.15s ease !important;
}
.stButton > button:hover {
    background: var(--bg-tertiary) !important;
    border-color: var(--accent-blue) !important;
}
.stDownloadButton > button {
    background: rgba(88,166,255,0.1) !important;
    color: var(--accent-blue) !important;
    border: 1px solid rgba(88,166,255,0.3) !important;
}
.stDownloadButton > button:hover {
    background: rgba(88,166,255,0.18) !important;
}

/* ── タブ ─────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    gap: 2px !important;
    border-bottom: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--text-secondary) !important;
    border-radius: 6px 6px 0 0 !important;
    padding: 8px 16px !important;
    font-size: 13px !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: rgba(88,166,255,0.08) !important;
    color: var(--accent-blue) !important;
    font-weight: 600 !important;
    border-bottom: 2px solid var(--accent-blue) !important;
}

/* ── セレクトボックス / マルチセレクト ─────── */
[data-testid="stMultiSelect"],
[data-testid="stSelectbox"] {
    background: var(--bg-secondary) !important;
}
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border-color: var(--border) !important;
}

/* ── データフレーム ───────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}

/* ── エクスパンダー ───────────────────────── */
[data-testid="stExpander"] {
    background: var(--bg-secondary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    margin-bottom: 4px !important;
}
[data-testid="stExpander"] summary {
    color: var(--text-primary) !important;
    font-size: 13px !important;
}

/* ── Plotlyチャート背景 ──────────────────── */
.js-plotly-plot .plotly .bg {
    fill: transparent !important;
}

/* ── アラート ─────────────────────────────── */
.stAlert {
    border-radius: 8px !important;
    border-width: 1px !important;
}

/* ── スピナー ─────────────────────────────── */
[data-testid="stSpinner"] {
    color: var(--accent-blue) !important;
}

/* ── 区切り線 ─────────────────────────────── */
hr {
    border-color: var(--border) !important;
    margin: 16px 0 !important;
}

/* ── チャット入力 (Genie用) ────────────────── */
[data-testid="stChatInput"] textarea {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}

/* ── スクロールバー ───────────────────────── */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}
::-webkit-scrollbar-track {
    background: var(--bg-primary);
}
::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
    background: var(--text-secondary);
}

/* ── Genieパネル ──────────────────────────── */
.genie-panel {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px;
    margin: 12px 0;
}
.genie-panel .panel-title {
    font-size: 14px;
    font-weight: 600;
    color: var(--accent-purple);
    margin-bottom: 10px;
}
.genie-pill {
    display: inline-block;
    background: rgba(188,140,255,0.08);
    color: var(--accent-purple);
    border: 1px solid rgba(188,140,255,0.2);
    border-radius: 16px;
    padding: 4px 12px;
    font-size: 11px;
    margin: 3px;
    cursor: pointer;
    transition: all 0.15s ease;
}
.genie-pill:hover {
    background: rgba(188,140,255,0.16);
    border-color: var(--accent-purple);
}
</style>
"""


def inject_css():
    """Streamlitページにダークテーマを注入"""
    import streamlit as st
    st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)
