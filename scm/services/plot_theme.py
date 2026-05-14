"""
Plotly 共通レイアウトモジュール (Phase 8 — Light Only)
========================================================
全ページのPlotlyチャートに一貫したスタイルを提供。
ダークモード廃止につきライト値固定。

設計方針 (Hakuhodo-style):
  - 背景: 純白、グリッドは極薄 (視覚ノイズ排除)
  - フォント: Inter → Hiragino fallback
  - グリッド: 水平方向のみ表示 (棒グラフ等)
  - 余白: タイトルと軸ラベルに明確なスペース
"""
from __future__ import annotations


# ── ライト専用トークン ──────────────────────────────────────
_T = {
    "bg":       "#ffffff",
    "paper":    "#ffffff",
    "panel":    "#f8fafc",   # カード/パネル背景
    "text":     "#0f172a",
    "text_sub": "#475569",   # text_sub エイリアス
    "sub":      "#475569",
    "text_muted": "#94a3b8",
    "grid":     "#f1f5f9",   # 極薄グリッド
    "border":   "#e2e8f0",
    "border_strong": "#cbd5e1",
    "blue":     "#2563eb",
    "green":    "#059669",
    "orange":   "#d97706",
    "red":      "#dc2626",
    "purple":   "#7c3aed",
    "teal":     "#0891b2",
    "amber":    "#b45309",
    "pink":     "#db2777",
}

_FONT = "Inter, 'Noto Sans JP', -apple-system, 'Hiragino Sans', sans-serif"


def get_theme_tokens() -> dict:
    """全トークンを返す (後方互換)。"""
    return _T


def base_layout(
    *,
    height: int = 380,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
    show_legend: bool = True,
    horizontal_grid_only: bool = False,
    margin_left: int = 52,
) -> dict:
    """
    全チャート共通レイアウト辞書。
    use: fig.update_layout(**base_layout(title="..."))
    """
    layout: dict = dict(
        height=height,
        plot_bgcolor=_T["bg"],
        paper_bgcolor=_T["paper"],
        font=dict(
            family=_FONT,
            color=_T["text"],
            size=12,
        ),
        xaxis=dict(
            title=dict(text=x_title or "", font=dict(color=_T["sub"], size=11)),
            gridcolor=_T["grid"] if not horizontal_grid_only else "rgba(0,0,0,0)",
            linecolor=_T["border"],
            tickfont=dict(color=_T["sub"], size=11),
            zeroline=False,
            showgrid=not horizontal_grid_only,
            showspikes=False,
        ),
        yaxis=dict(
            title=dict(text=y_title or "", font=dict(color=_T["sub"], size=11)),
            gridcolor=_T["grid"],
            linecolor=_T["border"],
            tickfont=dict(color=_T["sub"], size=11),
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1.0,
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor=_T["border"],
            borderwidth=1,
            font=dict(color=_T["sub"], size=11, family=_FONT),
        ) if show_legend else dict(visible=False),
        hoverlabel=dict(
            bgcolor=_T["paper"],
            bordercolor=_T["border"],
            font=dict(color=_T["text"], size=12, family=_FONT),
            namelength=-1,
        ),
        margin=dict(l=margin_left, r=20, t=46 if title else 28, b=44),
        colorway=palette(),
    )
    if title:
        layout["title"] = dict(
            text=title,
            font=dict(color=_T["text"], size=15, family=_FONT, weight=600),
            x=0.0, xanchor="left",
            y=0.98, yanchor="top",
        )
    return layout


def palette() -> list[str]:
    """系列カラーパレット (順序固定, Hakuhodo-style)。"""
    return [
        _T["blue"],   # #2563eb
        _T["orange"], # #d97706
        _T["green"],  # #059669
        _T["purple"], # #7c3aed
        _T["red"],    # #dc2626
        _T["teal"],   # #0891b2
        _T["amber"],  # #b45309
        _T["pink"],   # #db2777
        "#374151",    # グレー
        "#065f46",    # ダークグリーン
    ]
