"""
SCMデモアプリ テーマCSS (Phase 8 — Light Only / Hakuhodo Style)
================================================================
ダークモードを完全廃止。博報堂風ライト専用UIを提供。

設計哲学:
  - "Less is More": 情報密度は高いが視覚的ノイズは排除
  - タイポグラフィ階層: bold headline → medium body → muted caption
  - 余白: 2remのブリージングルームで業務システムらしくない洗練感
  - アクセントカラー: 1色 (#2563eb ブルー) のみ、感情誘導に限定使用
  - データ: DataFrame/Chart は純白背景 — 情報そのものを主役に
"""

# ════════════════════════════════════════════════════════
# デザイントークン (単一ライトテーマ)
# ════════════════════════════════════════════════════════
TOKENS = {
    "bg":        "#ffffff",    # メイン背景
    "bg_card":   "#f8fafc",    # カード・パネル背景
    "bg_subtle": "#f1f5f9",    # 薄いハイライト
    "border":    "#e2e8f0",    # 標準ボーダー
    "border_strong": "#cbd5e1",# 強調ボーダー
    "text":      "#0f172a",    # メインテキスト (暖かみのある黒)
    "text_sub":  "#475569",    # サブテキスト
    "text_muted":"#94a3b8",    # ミュートテキスト
    "accent":    "#2563eb",    # アクセントブルー
    "success":   "#059669",    # 成功グリーン
    "warning":   "#d97706",    # 警告アンバー
    "error":     "#dc2626",    # エラーレッド
    "purple":    "#7c3aed",    # パープル
}

GLOBAL_CSS = f"""
<style>
/* ── リセット & 基本 ────────────────────────────────────── */
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stHeader"] {{
    background: {TOKENS["bg"]} !important;
    color: {TOKENS["text"]} !important;
    font-family: 'Inter', 'Noto Sans JP', -apple-system, BlinkMacSystemFont,
                 'Hiragino Sans', 'Yu Gothic UI', system-ui, sans-serif !important;
    -webkit-font-smoothing: antialiased;
    letter-spacing: -0.01em;
}}

/* メインコンテンツの余白 */
.main .block-container {{
    padding-top: 1.75rem !important;
    padding-bottom: 3rem !important;
    max-width: 1400px;
}}

/* ── 文字色の完全テーマ強制 ─────────────────────────────── */
.stApp p, .stApp li, .stApp label, .stApp td, .stApp th,
.stApp strong, .stApp em, .stApp small, .stApp span,
.stMarkdown, .stMarkdown p, .stMarkdown li,
.stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4,
.stMarkdown strong, .stMarkdown em {{
    color: {TOKENS["text"]} !important;
}}
.stCaption, [data-testid="stCaptionContainer"], small {{
    color: {TOKENS["text_sub"]} !important;
    font-size: 12px !important;
    line-height: 1.6;
}}

/* ── 見出し ─────────────────────────────────────────────── */
.stApp h1, .stMarkdown h1 {{
    font-size: 28px !important;
    font-weight: 700 !important;
    color: {TOKENS["text"]} !important;
    letter-spacing: -0.025em !important;
    margin: 0 0 12px !important;
}}
.stApp h2, .stMarkdown h2 {{
    font-size: 22px !important;
    font-weight: 600 !important;
    color: {TOKENS["text"]} !important;
    letter-spacing: -0.02em !important;
    margin: 8px 0 10px !important;
    padding-bottom: 10px;
    border-bottom: 1px solid {TOKENS["border"]};
}}
.stApp h3, .stMarkdown h3 {{
    font-size: 16px !important;
    font-weight: 600 !important;
    color: {TOKENS["text"]} !important;
    letter-spacing: -0.01em !important;
    margin: 24px 0 8px !important;
}}
.stApp h4, .stMarkdown h4 {{
    font-size: 14px !important;
    font-weight: 600 !important;
    color: {TOKENS["text"]} !important;
    margin: 14px 0 6px !important;
}}

/* ── コード ─────────────────────────────────────────────── */
.stApp code, .stApp pre {{
    background: {TOKENS["bg_subtle"]} !important;
    color: {TOKENS["text"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 5px;
    font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace !important;
    font-size: 12.5px;
}}
.stApp code {{ padding: 1px 5px; }}
.stApp pre {{ padding: 12px 16px; line-height: 1.5; }}

/* ── ヘッダー/フッター ─────────────────────────────────── */
header[data-testid="stHeader"] {{
    background: {TOKENS["bg"]} !important;
    border-bottom: 1px solid {TOKENS["border"]} !important;
    height: 0 !important;
}}
footer, #MainMenu {{ display: none !important; visibility: hidden !important; }}

/* ── サイドバー ─────────────────────────────────────────── */
section[data-testid="stSidebar"] {{
    background: {TOKENS["bg_card"]} !important;
    border-right: 1px solid {TOKENS["border"]} !important;
}}
section[data-testid="stSidebar"] * {{
    color: {TOKENS["text"]} !important;
}}
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] small {{
    color: {TOKENS["text_sub"]} !important;
    font-size: 12px !important;
}}
[data-testid="stSidebarNavItems"] a {{
    color: {TOKENS["text_sub"]} !important;
    border-radius: 8px !important;
    padding: 7px 12px !important;
    font-size: 13px !important;
    transition: all 0.12s ease !important;
    display: block;
}}
[data-testid="stSidebarNavItems"] a:hover {{
    color: {TOKENS["accent"]} !important;
    background: rgba(37,99,235,0.06) !important;
}}
[data-testid="stSidebarNavItems"] a[aria-current="page"] {{
    background: rgba(37,99,235,0.10) !important;
    color: {TOKENS["accent"]} !important;
    font-weight: 600 !important;
}}

/* ── メトリクスカード ───────────────────────────────────── */
[data-testid="stMetric"] {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 12px !important;
    padding: 16px 20px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
    transition: box-shadow 0.2s ease, border-color 0.2s ease;
}}
[data-testid="stMetric"]:hover {{
    box-shadow: 0 3px 8px rgba(0,0,0,0.09) !important;
    border-color: {TOKENS["border_strong"]} !important;
}}
[data-testid="stMetricLabel"] {{
    color: {TOKENS["text_sub"]} !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.7px !important;
}}
[data-testid="stMetricValue"] {{
    color: {TOKENS["text"]} !important;
    font-weight: 700 !important;
    font-size: 30px !important;
    letter-spacing: -0.025em !important;
    margin-top: 2px !important;
    line-height: 1.1 !important;
}}
[data-testid="stMetricDelta"] {{
    font-size: 11px !important;
    margin-top: 4px !important;
}}

/* ── ボタン ─────────────────────────────────────────────── */
.stButton > button {{
    background: {TOKENS["bg"]} !important;
    color: {TOKENS["text_sub"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 8px !important;
    padding: 7px 16px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    transition: all 0.14s ease !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}}
.stButton > button:hover {{
    background: {TOKENS["bg_subtle"]} !important;
    border-color: {TOKENS["accent"]} !important;
    color: {TOKENS["accent"]} !important;
    box-shadow: 0 2px 5px rgba(37,99,235,0.15) !important;
}}
.stButton > button[kind="primary"] {{
    background: {TOKENS["accent"]} !important;
    color: #ffffff !important;
    border-color: {TOKENS["accent"]} !important;
    box-shadow: 0 2px 5px rgba(37,99,235,0.25);
}}
.stButton > button[kind="primary"]:hover {{
    background: #1d4ed8 !important;
    filter: none !important;
}}
.stButton > button p {{
    color: inherit !important;
    font-weight: inherit !important;
}}

/* ── タブ ───────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{
    background: transparent !important;
    gap: 2px !important;
    border-bottom: 1px solid {TOKENS["border"]} !important;
}}
.stTabs [data-baseweb="tab"] {{
    background: transparent !important;
    color: {TOKENS["text_sub"]} !important;
    border-radius: 6px 6px 0 0 !important;
    padding: 8px 16px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    transition: color 0.12s !important;
}}
.stTabs [data-baseweb="tab"]:hover {{
    color: {TOKENS["text"]} !important;
}}
.stTabs [data-baseweb="tab"][aria-selected="true"] {{
    color: {TOKENS["accent"]} !important;
    font-weight: 600 !important;
    border-bottom: 2px solid {TOKENS["accent"]} !important;
}}

/* ── 入力フィールド ─────────────────────────────────────── */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stDateInput > div > div,
.stNumberInput > div > div,
.stTextInput > div > div,
.stTextArea > div > div {{
    background: {TOKENS["bg"]} !important;
    color: {TOKENS["text"]} !important;
    border: 1px solid {TOKENS["border_strong"]} !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}}
.stSelectbox label, .stMultiSelect label, .stRadio label, .stCheckbox label,
.stDateInput label, .stNumberInput label, .stTextInput label, .stTextArea label,
.stSlider label, .stFileUploader label {{
    color: {TOKENS["text"]} !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    margin-bottom: 4px !important;
}}
input[type="text"], input[type="number"], input[type="date"], textarea {{
    background: {TOKENS["bg"]} !important;
    color: {TOKENS["text"]} !important;
    border: 1px solid {TOKENS["border_strong"]} !important;
    border-radius: 8px !important;
}}
div[data-baseweb="popover"] {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    box-shadow: 0 4px 12px rgba(0,0,0,0.10) !important;
    border-radius: 8px !important;
}}
div[data-baseweb="popover"] li {{
    color: {TOKENS["text"]} !important;
    font-size: 13px !important;
}}
div[data-baseweb="popover"] li:hover {{
    background: {TOKENS["bg_subtle"]} !important;
}}

/* ── DataFrame ──────────────────────────────────────────── */
[data-testid="stDataFrame"],
[data-testid="stDataFrameResizable"],
[data-testid="stDataFrame"] > div {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}}

/* ── Plotly チャート ─────────────────────────────────────── */
.stPlotlyChart {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 12px !important;
    padding: 16px 12px 8px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
}}
.js-plotly-plot .plotly .bg {{
    fill: transparent !important;
}}

/* ── Expander ───────────────────────────────────────────── */
[data-testid="stExpander"] {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border"]} !important;
    border-radius: 10px !important;
    margin-bottom: 6px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    overflow: hidden;
}}
[data-testid="stExpander"] summary {{
    color: {TOKENS["text"]} !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 10px 16px !important;
}}
[data-testid="stExpander"] summary:hover {{
    background: {TOKENS["bg_subtle"]} !important;
}}
[data-testid="stExpander"] p,
[data-testid="stExpander"] span,
[data-testid="stExpander"] div {{
    color: {TOKENS["text"]} !important;
}}

/* ── アラート ───────────────────────────────────────────── */
.stAlert {{
    border-radius: 10px !important;
    padding: 12px 16px !important;
    border-width: 1px !important;
    font-size: 13px !important;
}}
.stAlert p, .stAlert div, .stAlert span {{
    color: inherit !important;
    font-size: 13px !important;
}}

/* ── ラジオボタン ─────────────────────────────────────────── */
.stRadio [role="radiogroup"] {{
    gap: 6px !important;
}}
.stRadio [role="radiogroup"] label {{
    background: {TOKENS["bg"]} !important;
    border: 1px solid {TOKENS["border_strong"]} !important;
    border-radius: 8px !important;
    padding: 7px 14px !important;
    margin: 0 !important;
    transition: border-color 0.12s !important;
}}
.stRadio [role="radiogroup"] label:hover {{
    border-color: {TOKENS["accent"]} !important;
}}
.stRadio [role="radiogroup"] label p {{
    color: {TOKENS["text_sub"]} !important;
    font-size: 13px !important;
}}

/* ── 区切り線 ───────────────────────────────────────────── */
hr {{
    border: none !important;
    border-top: 1px solid {TOKENS["border"]} !important;
    margin: 28px 0 20px !important;
    opacity: 0.7;
}}

/* ── スクロールバー ─────────────────────────────────────── */
::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: {TOKENS["bg_subtle"]}; }}
::-webkit-scrollbar-thumb {{
    background: {TOKENS["border_strong"]};
    border-radius: 3px;
}}
::-webkit-scrollbar-thumb:hover {{ background: {TOKENS["text_muted"]}; }}

/* ── カスタム: ビジネスカード ──────────────────────────── */
.biz-card {{
    background: {TOKENS["bg"]};
    border: 1px solid {TOKENS["border"]};
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    transition: box-shadow 0.2s, border-color 0.2s;
}}
.biz-card:hover {{
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-color: {TOKENS["border_strong"]};
}}
.biz-card-title {{
    font-size: 15px;
    font-weight: 600;
    color: {TOKENS["text"]};
    margin-bottom: 10px;
}}

/* ── アクション優先度バッジ ──────────────────────────────── */
.priority-critical {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: #fef2f2;
    color: {TOKENS["error"]};
    border: 1px solid #fecaca;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.02em;
}}
.priority-high {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: #fffbeb;
    color: {TOKENS["warning"]};
    border: 1px solid #fde68a;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    font-weight: 600;
}}
.priority-medium {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: #fffbeb;
    color: #b45309;
    border: 1px solid #fde68a;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    font-weight: 600;
}}
.priority-ok {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: #f0fdf4;
    color: {TOKENS["success"]};
    border: 1px solid #bbf7d0;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    font-weight: 600;
}}

/* ── アクションカード ──────────────────────────────────── */
.action-card {{
    background: {TOKENS["bg"]};
    border: 1px solid {TOKENS["border"]};
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    transition: box-shadow 0.18s ease;
}}
.action-card:hover {{
    box-shadow: 0 4px 14px rgba(0,0,0,0.09);
}}
.action-card-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
}}
.action-card-title {{
    font-size: 15px;
    font-weight: 600;
    color: {TOKENS["text"]};
    flex: 1;
}}
.action-card-meta {{
    display: flex;
    gap: 20px;
    margin-bottom: 12px;
    padding-bottom: 12px;
    border-bottom: 1px solid {TOKENS["border"]};
}}
.action-card-meta-item {{
    display: flex;
    flex-direction: column;
    gap: 2px;
}}
.action-card-meta-label {{
    font-size: 10px;
    font-weight: 500;
    color: {TOKENS["text_muted"]};
    text-transform: uppercase;
    letter-spacing: 0.6px;
}}
.action-card-meta-value {{
    font-size: 18px;
    font-weight: 700;
    color: {TOKENS["text"]};
    letter-spacing: -0.02em;
}}
.action-card-steps {{
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 6px;
}}
.action-card-step {{
    display: flex;
    align-items: flex-start;
    gap: 8px;
    font-size: 13px;
    color: {TOKENS["text_sub"]};
    line-height: 1.5;
}}
.action-card-step-num {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    background: {TOKENS["bg_subtle"]};
    border-radius: 50%;
    font-size: 10px;
    font-weight: 700;
    color: {TOKENS["text_sub"]};
    flex-shrink: 0;
    margin-top: 2px;
}}

/* ── LT急騰アラートカード ─────────────────────────────── */
.lt-alert-card {{
    background: #fef2f2;
    border: 1px solid #fecaca;
    border-left: 4px solid {TOKENS["error"]};
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 8px;
}}
.lt-alert-card-title {{
    font-size: 14px;
    font-weight: 600;
    color: {TOKENS["error"]};
    margin-bottom: 6px;
}}
.lt-alert-card-body {{
    font-size: 13px;
    color: #7f1d1d;
    line-height: 1.5;
}}

</style>
"""


# ════════════════════════════════════════════════════════
# パブリック API
# ════════════════════════════════════════════════════════
def inject_css():
    """全ページで呼ぶ。ライト専用CSSを注入。"""
    import streamlit as st
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def is_light_theme() -> bool:
    """常に True (ダークモードは廃止)。後方互換のために残す。"""
    return True


def get_theme_mode() -> str:
    """後方互換。常に 'light' を返す。"""
    return "light"


def plot_colors() -> dict:
    """Plotly チャートで使う色辞書 (後方互換)。"""
    return {
        "bg":     TOKENS["bg"],
        "paper":  TOKENS["bg"],
        "text":   TOKENS["text"],
        "grid":   TOKENS["border"],
        "blue":   TOKENS["accent"],
        "green":  TOKENS["success"],
        "orange": TOKENS["warning"],
        "red":    TOKENS["error"],
        "purple": TOKENS["purple"],
    }
