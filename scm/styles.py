"""
SCMデモアプリ テーマCSS (Phase 7 全面リファイン)
=====================================================
博報堂的「洗練・余白多め・タイポグラフィ階層」を意識した
ダーク / ライト 両モードの統合スタイル。

設計方針:
  - フォント: Inter, Hiragino Sans (system fallback)
  - 余白: 16-24px をデフォルトに、密度を抑える
  - 階層: heading は太字 + サイズ、本文は読みやすさ重視
  - アクセント: 1色 (青) のみを少量
  - ライトモード時の DataFrame / Plotly 背景を白系に強制
"""

# ════════════════════════════════════════════════════════
# テーマ別 CSS 変数
# ════════════════════════════════════════════════════════
DARK_THEME_CSS = """
<style>
:root {
    --bg-primary:     #0d1117;
    --bg-secondary:   #161b22;
    --bg-tertiary:    #1c2128;
    --bg-panel:       #161b22;
    --bg-elevated:    #21262d;
    --border:         #30363d;
    --border-strong:  #484f58;
    --text-primary:   #e6edf3;
    --text-secondary: #8b949e;
    --text-muted:     #6e7681;
    --text-strong:    #ffffff;
    --accent-blue:    #58a6ff;
    --accent-green:   #3fb950;
    --accent-orange:  #f0883e;
    --accent-red:     #ff7b72;
    --accent-purple:  #bc8cff;
    --plot-bg:        #0d1117;
    --code-bg:        #161b22;
    --table-row-alt:  rgba(255,255,255,0.02);
}
</style>
"""

LIGHT_THEME_CSS = """
<style>
:root {
    --bg-primary:     #ffffff;
    --bg-secondary:   #fafbfc;
    --bg-tertiary:    #f6f8fa;
    --bg-panel:       #ffffff;
    --bg-elevated:    #f6f8fa;
    --border:         #d8dee4;
    --border-strong:  #afb8c1;
    --text-primary:   #1f2328;
    --text-secondary: #656d76;
    --text-muted:     #8c959f;
    --text-strong:    #0d1117;
    --accent-blue:    #0969da;
    --accent-green:   #1a7f37;
    --accent-orange:  #bf8700;
    --accent-red:     #cf222e;
    --accent-purple:  #8250df;
    --plot-bg:        #ffffff;
    --code-bg:        #f6f8fa;
    --table-row-alt:  rgba(0,0,0,0.025);
}
</style>
"""

# ════════════════════════════════════════════════════════
# 共通スタイル
# ════════════════════════════════════════════════════════
COMMON_CSS = """
<style>
/* ────── 基本: アプリ背景・タイポ ────── */
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stHeader"] {
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                 'Hiragino Sans', 'Yu Gothic UI', 'Meiryo', sans-serif !important;
    letter-spacing: -0.01em;
}

/* メインコンテンツ余白を広めに */
.main .block-container {
    padding-top: 1.5rem !important;
    padding-bottom: 3rem !important;
    max-width: 1380px;
}

/* ────── 文字色を強制テーマ化 ────── */
.stApp p, .stApp span, .stApp div, .stApp li, .stApp label,
.stApp strong, .stApp em, .stApp small,
.stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown li,
.stMarkdown strong, .stMarkdown em {
    color: var(--text-primary);
}

/* 見出し階層 (博報堂風: 細めウェイト + 余白) */
.stApp h1, .stMarkdown h1 {
    color: var(--text-strong) !important;
    font-weight: 700 !important;
    font-size: 26px !important;
    letter-spacing: -0.02em !important;
    margin: 0 0 12px 0 !important;
}
.stApp h2, .stMarkdown h2 {
    color: var(--text-strong) !important;
    font-weight: 600 !important;
    font-size: 22px !important;
    letter-spacing: -0.015em !important;
    margin: 4px 0 8px 0 !important;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
}
.stApp h3, .stMarkdown h3 {
    color: var(--text-primary) !important;
    font-weight: 600 !important;
    font-size: 16px !important;
    letter-spacing: -0.01em !important;
    margin: 20px 0 8px 0 !important;
}
.stApp h4, .stMarkdown h4 {
    color: var(--text-primary) !important;
    font-weight: 600 !important;
    font-size: 14px !important;
    margin: 12px 0 6px 0 !important;
}

/* caption / small text */
.stCaption, [data-testid="stCaptionContainer"], small {
    color: var(--text-secondary) !important;
    font-size: 12px !important;
    line-height: 1.55 !important;
}

/* code / pre */
.stApp code, .stApp pre {
    background: var(--code-bg) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 4px;
    padding: 1px 6px;
    font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace !important;
    font-size: 12px;
}
.stApp pre {
    padding: 12px 16px;
    line-height: 1.5;
}

/* ────── サイドバー ────── */
header[data-testid="stHeader"] {
    background: var(--bg-primary) !important;
    border-bottom: 1px solid var(--border) !important;
    height: 0 !important;
}
footer { display: none !important; }
#MainMenu { visibility: hidden !important; }

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
    padding: 7px 12px !important;
    font-size: 13px !important;
    transition: all 0.15s ease !important;
}
[data-testid="stSidebarNavItems"] a:hover {
    color: var(--text-primary) !important;
    background: rgba(88,166,255,0.08) !important;
}
[data-testid="stSidebarNavItems"] a[aria-current="page"] {
    background: rgba(88,166,255,0.13) !important;
    color: var(--accent-blue) !important;
    font-weight: 600 !important;
}

/* ────── メトリクスカード (洗練) ────── */
[data-testid="stMetric"] {
    background: var(--bg-panel) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    padding: 14px 18px !important;
    transition: border-color 0.2s ease;
}
[data-testid="stMetric"]:hover {
    border-color: var(--border-strong) !important;
}
[data-testid="stMetricLabel"] {
    color: var(--text-secondary) !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.6px !important;
    font-weight: 500 !important;
}
[data-testid="stMetricValue"] {
    color: var(--text-strong) !important;
    font-weight: 700 !important;
    font-size: 28px !important;
    letter-spacing: -0.02em !important;
    margin-top: 4px !important;
}
[data-testid="stMetricDelta"] {
    font-size: 11px !important;
    margin-top: 4px !important;
}

/* ────── ボタン (洗練) ────── */
.stButton > button {
    background: var(--bg-panel) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 6px 14px !important;
    font-weight: 500 !important;
    font-size: 13px !important;
    transition: all 0.15s ease !important;
}
.stButton > button:hover {
    background: var(--bg-elevated) !important;
    border-color: var(--accent-blue) !important;
    color: var(--accent-blue) !important;
}
.stButton > button[kind="primary"] {
    background: var(--accent-blue) !important;
    color: var(--bg-primary) !important;
    border: 1px solid var(--accent-blue) !important;
}
.stButton > button[kind="primary"]:hover {
    background: var(--accent-blue) !important;
    filter: brightness(0.9);
}
.stButton > button p { color: inherit !important; }

/* ────── タブ ────── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    gap: 4px !important;
    border-bottom: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--text-secondary) !important;
    border-radius: 6px 6px 0 0 !important;
    padding: 8px 14px !important;
    font-size: 13px !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: var(--accent-blue) !important;
    font-weight: 600 !important;
    border-bottom: 2px solid var(--accent-blue) !important;
}

/* ────── 入力フィールド ────── */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stDateInput > div > div,
.stNumberInput > div > div,
.stTextInput > div > div,
.stTextArea > div > div {
    background: var(--bg-panel) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}
.stSelectbox label, .stMultiSelect label, .stRadio label, .stCheckbox label,
.stDateInput label, .stNumberInput label, .stTextInput label, .stTextArea label,
.stSlider label, .stFileUploader label, .stColorPicker label {
    color: var(--text-primary) !important;
    font-weight: 500 !important;
    font-size: 13px !important;
}
input[type="text"], input[type="number"], input[type="date"], textarea {
    background: var(--bg-panel) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}

/* セレクトメニューのドロップダウンも */
div[data-baseweb="popover"] {
    background: var(--bg-panel) !important;
    border: 1px solid var(--border) !important;
}
div[data-baseweb="popover"] li {
    color: var(--text-primary) !important;
}
div[data-baseweb="popover"] li:hover {
    background: var(--bg-elevated) !important;
}

/* ────── DataFrame (重要: ライトモード時に黒背景にならないように) ────── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    overflow: hidden !important;
    background: var(--bg-panel) !important;
}
[data-testid="stDataFrame"] > div {
    background: var(--bg-panel) !important;
}
[data-testid="stDataFrameResizable"] {
    background: var(--bg-panel) !important;
}

/* ────── Expander (洗練) ────── */
[data-testid="stExpander"] {
    background: var(--bg-panel) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    margin-bottom: 6px !important;
    overflow: hidden;
}
[data-testid="stExpander"] summary {
    color: var(--text-primary) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 8px 14px !important;
}
[data-testid="stExpander"] summary:hover {
    background: var(--bg-elevated) !important;
}
[data-testid="stExpander"] p,
[data-testid="stExpander"] span,
[data-testid="stExpander"] div {
    color: var(--text-primary) !important;
}

/* ────── Plotly チャート背景透過 ────── */
.js-plotly-plot .plotly .bg {
    fill: transparent !important;
}
.user-select-none.svg-container {
    background: var(--bg-panel) !important;
    border-radius: 10px;
}
.stPlotlyChart {
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 10px 4px 10px;
}

/* ────── アラート ────── */
.stAlert {
    border-radius: 10px !important;
    border-width: 1px !important;
    padding: 12px 16px !important;
}
.stAlert p, .stAlert div, .stAlert span {
    color: inherit !important;
}
[data-baseweb="notification"] {
    background: var(--bg-panel) !important;
    border: 1px solid var(--border) !important;
}

/* ────── ラジオボタン ────── */
.stRadio [role="radiogroup"] {
    gap: 6px !important;
}
.stRadio [role="radiogroup"] label {
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 6px 14px;
    margin: 0 !important;
}
.stRadio [role="radiogroup"] label:hover {
    border-color: var(--accent-blue);
}
.stRadio [role="radiogroup"] label p {
    color: var(--text-primary) !important;
    font-size: 13px !important;
}

/* ────── スピナー ────── */
[data-testid="stSpinner"] {
    color: var(--accent-blue) !important;
}

/* ────── 区切り線 (細く繊細に) ────── */
hr {
    border-color: var(--border) !important;
    margin: 28px 0 18px 0 !important;
    opacity: 0.6;
}

/* ────── チャット入力 ────── */
[data-testid="stChatInput"] textarea {
    background: var(--bg-panel) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
}

/* ────── スクロールバー ────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb {
    background: var(--border-strong);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }

/* ────── カスタム: セクションパネル ────── */
.section-panel {
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 14px;
}
.section-panel-title {
    font-size: 14px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 6px;
}
.section-panel-sub {
    font-size: 11px;
    color: var(--text-secondary);
    line-height: 1.5;
}

/* ────── インライン badge ────── */
.kpi-pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    margin-right: 4px;
}
.kpi-pill.blue   { background: rgba(88,166,255,0.15); color: var(--accent-blue); }
.kpi-pill.green  { background: rgba(63,185,80,0.15);  color: var(--accent-green); }
.kpi-pill.orange { background: rgba(240,136,62,0.15); color: var(--accent-orange); }
.kpi-pill.red    { background: rgba(255,123,114,0.15); color: var(--accent-red); }

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
# Plotly 用カラー (テーマで切替) — 後方互換
# ════════════════════════════════════════════════════════
def plot_colors() -> dict:
    """Plotly チャートで使う色をテーマに合わせて返す (後方互換用)"""
    if is_light_theme():
        return {
            "bg":     "#ffffff",
            "paper":  "#ffffff",
            "text":   "#1f2328",
            "grid":   "#eaeef2",
            "blue":   "#0969da",
            "green":  "#1a7f37",
            "orange": "#bf8700",
            "red":    "#cf222e",
            "purple": "#8250df",
        }
    else:
        return {
            "bg":     "#0d1117",
            "paper":  "#0d1117",
            "text":   "#e6edf3",
            "grid":   "#21262d",
            "blue":   "#58a6ff",
            "green":  "#3fb950",
            "orange": "#f0883e",
            "red":    "#ff7b72",
            "purple": "#bc8cff",
        }
