"""
Plotly 共通レイアウト (ダーク / ライト 両対応)
================================================
全ページで統一感のあるチャートスタイルを提供する。

設計方針 (博報堂的・洗練UI):
  - 余白多め、グリッドは控えめ
  - フォントは Inter / Hiragino Sans (system-ui)
  - 軸線は薄く、データに集中させる
  - レジェンドは右上 / hover label もテーマ準拠
"""
from __future__ import annotations

from styles import is_light_theme


# ════════════════════════════════════════════════════════
# テーマトークン
# ════════════════════════════════════════════════════════
def get_theme_tokens() -> dict:
    """現在テーマの全カラートークンを返す"""
    if is_light_theme():
        return {
            "bg":            "#ffffff",
            "paper":         "#ffffff",
            "panel":         "#fafbfc",
            "panel2":        "#f6f8fa",
            "border":        "#d8dee4",
            "border_strong": "#afb8c1",
            "text":          "#1f2328",
            "text_sub":      "#656d76",
            "text_muted":    "#8c959f",
            "grid":          "#eaeef2",
            "grid_strong":   "#d0d7de",
            "blue":          "#0969da",
            "green":         "#1a7f37",
            "orange":        "#bf8700",
            "red":           "#cf222e",
            "purple":        "#8250df",
            "blue_soft":     "#dbeafe",
            "red_soft":      "#ffe2e2",
            "green_soft":    "#dafbe1",
        }
    else:
        return {
            "bg":            "#0d1117",
            "paper":         "#0d1117",
            "panel":         "#161b22",
            "panel2":        "#1c2128",
            "border":        "#30363d",
            "border_strong": "#484f58",
            "text":          "#e6edf3",
            "text_sub":      "#8b949e",
            "text_muted":    "#6e7681",
            "grid":          "#21262d",
            "grid_strong":   "#30363d",
            "blue":          "#58a6ff",
            "green":         "#3fb950",
            "orange":        "#f0883e",
            "red":           "#ff7b72",
            "purple":        "#bc8cff",
            "blue_soft":     "#1c2c4d",
            "red_soft":      "#3d1d20",
            "green_soft":    "#1a2e1f",
        }


# ════════════════════════════════════════════════════════
# Plotly 共通レイアウト (returns dict, mergeable)
# ════════════════════════════════════════════════════════
def base_layout(
    *,
    height: int = 380,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
    show_legend: bool = True,
    horizontal_grid_only: bool = False,
) -> dict:
    """
    全チャート共通のレイアウト辞書を返す。
    use: fig.update_layout(**base_layout(title="..."))
    """
    t = get_theme_tokens()
    layout = dict(
        height=height,
        plot_bgcolor=t["bg"],
        paper_bgcolor=t["paper"],
        font=dict(
            family="Inter, -apple-system, 'Hiragino Sans', 'Yu Gothic UI', sans-serif",
            color=t["text"],
            size=12,
        ),
        xaxis=dict(
            title=dict(text=x_title or "", font=dict(color=t["text_sub"], size=11)),
            gridcolor=t["grid"] if not horizontal_grid_only else "rgba(0,0,0,0)",
            linecolor=t["border_strong"],
            tickfont=dict(color=t["text_sub"], size=11),
            zeroline=False,
            showspikes=False,
        ),
        yaxis=dict(
            title=dict(text=y_title or "", font=dict(color=t["text_sub"], size=11)),
            gridcolor=t["grid"],
            linecolor=t["border_strong"],
            tickfont=dict(color=t["text_sub"], size=11),
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1.0,
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=t["text_sub"], size=11),
        ) if show_legend else dict(visible=False),
        hoverlabel=dict(
            bgcolor=t["panel"],
            bordercolor=t["border"],
            font=dict(color=t["text"], size=12, family="Inter, sans-serif"),
        ),
        margin=dict(l=50, r=20, t=46 if title else 26, b=44),
    )
    if title:
        layout["title"] = dict(
            text=title,
            font=dict(color=t["text"], size=15, family="Inter, sans-serif"),
            x=0.0, xanchor="left",
            y=0.98, yanchor="top",
        )
    return layout


def palette() -> list[str]:
    """系列色パレット (順序固定)"""
    t = get_theme_tokens()
    return [t["blue"], t["orange"], t["green"], t["purple"], t["red"],
            "#0a85a4", "#9e4f00", "#3c5af8", "#c08c47", "#9b1b30"]
