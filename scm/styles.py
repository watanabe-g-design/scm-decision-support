"""
SCMデモアプリ用 テーマCSS（ダーク / ライト切替対応）
=========================================================
GitHub-style ダーク + 業務向けライトテーマの2系統を提供。
セッションステート ``theme_mode`` で切替可能。

Phase 6 修正: ライトテーマ時に文字色が変わらず白字のまま読めない問題に対応。
  - CSS変数を全文字要素 (h1〜h6/p/span/div/li/strong/em/code) に強制適用
  - Plotly キャンバスも背景色を切替対象に
"""

# ════════════════════════════════════════════════════════
# ダークテーマ (デフォルト)
# ════════════════════════════════════════════════════════
DARK_THEME_CSS = """
<style>
:root {
    --bg-primary: #0d1117;
    --bg-secondary: #161b22;
    --bg-tertiary: #1c2128;
    --border: #30363d;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --text-strong: #ffffff;
    --accent-blue: #58a6ff;
    --accent-green: #2ea043;
    --accent-orange: #ffa000;
    --accent-red: #ff4646;
    --accent-purple: #bc8cff;
    --plot-bg: #0d1117;
    --code-bg: #161b22;
}
</style>
"""

# ════════════════════════════════════════════════════════
# ライトテーマ (業務文書向け)
# ════════════════════════════════════════════════════════
LIGHT_THEME_CSS = """
<style>
:root {
    --bg-primary: #ffffff;
    --bg-secondary: #f6f8fa;
    --bg-tertiary: #eaeef2;
    --border: #d0d7de;
    --text-primary: #1f2328;
    --text-secondary: #656d76;
    --text-strong: #000000;
    --accent-blue: #0969da;
    --accent-green: #1a7f37;
    --accent-orange: #bf8700;
    --accent-red: #cf222e;
    --accent-purple: #8250df;
    --plot-bg: #ffffff;
    --code-bg: #f6f8fa;
}
</style>
"""

# ════════════════════════════════════════════════════════
# 共通スタイル (両テーマで共通利用、CSS変数を参照)
# ════════════════════════════════════════════════════════
COMMON_CSS = """
<style>
/* 背景・テキスト基本設定 */
.stApp, [data-testid="stAppViewContainer"],
[data-testid="stHeader"],
section[data-testid="stSidebar"] {
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}

/* ── 強制的に文字色をテーマ変数化（Phase 6 ライトモード対応） ── */
.stApp, .stApp p, .stApp span, .stApp div, .stApp li, .stApp label,
.stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
.stApp strong, .stApp em, .stApp small {
    color: var(--text-primary);
}
.stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown li,
.stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4, .stMarkdown h5, .stMarkdown h6 {
    color: var(--text-primary) !important;
}
.stCaption, [data-testid="stCaptionContainer"], small {
    color: var(--text-secondary) !important;
}
.stApp code, .stApp pre {
    background: var(--code-bg) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
}

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
section[data-testid="stSidebar"] *,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] div {
    color: var(--text-primary) !important;
}

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
.title-bar .logo { font-size: 28px; line-height: 1; }
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
.stButton > button p {
    color: var(--text-primary) !important;
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
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border-color: var(--border) !important;
}
.stSelectbox label, .stMultiSelect label, .stRadio label, .stCheckbox label,
.stDateInput label, .stNumberInput label, .stTextInput label, .stTextArea label,
.stSlider label {
    color: var(--text-primary) !important;
}
input[type="text"], input[type="number"], textarea {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
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
[data-testid="stExpander"] summary,
[data-testid="stExpander"] p,
[data-testid="stExpander"] span,
[data-testid="stExpander"] div {
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
.stAlert p, .stAlert div, .stAlert span {
    color: inherit !important;
}

/* ── ラジオボタン ─────────────────────────── */
.stRadio [role="radiogroup"] label p {
    color: var(--text-primary) !important;
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

/* ── チャット入力 ────────────────────────── */
[data-testid="stChatInput"] textarea {
    background: var(--bg-secondary) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}

/* ── スクロールバー ───────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover { background: var(--text-secondary); }
</style>
"""


def get_theme_mode() -> str:
    """セッションステートからテーマモードを取得 (デフォルト: dark)"""
    import streamlit as st
    return st.session_state.get("theme_mode", "dark")


def is_light_theme() -> bool:
    return get_theme_mode() == "light"


def inject_css():
    """Streamlit ページに現在テーマのCSSを注入"""
    import streamlit as st
    theme = get_theme_mode()
    if theme == "light":
        st.markdown(LIGHT_THEME_CSS, unsafe_allow_html=True)
    else:
        st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)
    st.markdown(COMMON_CSS, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════
# Plotly 用カラー (テーマで切替)
# ════════════════════════════════════════════════════════
def plot_colors() -> dict:
    """Plotly チャートで使う色をテーマに合わせて返す"""
    if is_light_theme():
        return {
            "bg":        "#ffffff",
            "paper":     "#ffffff",
            "text":      "#1f2328",
            "grid":      "#d0d7de",
            "blue":      "#0969da",
            "green":     "#1a7f37",
            "orange":    "#bf8700",
            "red":       "#cf222e",
            "purple":    "#8250df",
        }
    else:
        return {
            "bg":        "#0d1117",
            "paper":     "#0d1117",
            "text":      "#e6edf3",
            "grid":      "#30363d",
            "blue":      "#58a6ff",
            "green":     "#2ea043",
            "orange":    "#ffa000",
            "red":       "#ff4646",
            "purple":    "#bc8cff",
        }
