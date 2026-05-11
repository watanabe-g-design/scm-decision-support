"""
時系列グラフ ヘルパー
======================
plotly グラフに「今日」基準の実績/予測を視覚的に区別する付加要素を提供。

提供機能:
  1. add_today_vline             — 「今日」を縦線で明示
  2. split_actual_forecast       — 時系列データを実績(<=今日) と 予測(>今日) に分割
  3. style_actual_forecast_lines — 1本の系列を実績=実線、予測=破線で2系列化
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, Tuple

import pandas as pd
import plotly.graph_objects as go

from services.config import get_as_of_date


# ════════════════════════════════════════════════════════
# 今日の縦線
# ════════════════════════════════════════════════════════
def add_today_vline(
    fig: go.Figure,
    *,
    line_color: str | None = None,
    annotation_text: str = "本日",
) -> go.Figure:
    """
    plotly Figure に「今日」を示す縦線とアノテーションを追加。
    テーマ(ダーク/ライト)に応じた背景色で描画。
    """
    from styles import plot_colors
    colors = plot_colors()
    line = line_color or colors["red"]
    today = get_as_of_date()
    fig.add_vline(
        x=today,
        line_width=1.5,
        line_dash="dash",
        line_color=line,
    )
    fig.add_annotation(
        x=today,
        y=1.02,
        yref="paper",
        text=f"📅 {annotation_text} ({today.isoformat()})",
        showarrow=False,
        font=dict(size=11, color=line),
        bgcolor=colors["paper"],
        bordercolor=line,
        borderwidth=1,
        borderpad=3,
    )
    return fig


# ════════════════════════════════════════════════════════
# 実績 / 予測の分割
# ════════════════════════════════════════════════════════
def split_actual_forecast(
    df: pd.DataFrame,
    date_col: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    DataFrame を「今日」より前(=実績) と「今日」以降(=予測) の2つに分割。
    境界の「今日」レコードは両方に含めてグラフが連続するようにする。

    Returns
    -------
    (actual_df, forecast_df) : (pd.DataFrame, pd.DataFrame)
    """
    today = get_as_of_date()
    # date_col を date 型に変換
    parsed = pd.to_datetime(df[date_col], errors="coerce")
    # date オブジェクトに正規化
    series_date = parsed.dt.date

    actual = df[series_date <= today].copy()
    forecast = df[series_date >= today].copy()
    return actual, forecast


# ════════════════════════════════════════════════════════
# 実績/予測 を1系列を2系列に変換して描画
# ════════════════════════════════════════════════════════
def add_actual_forecast_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    *,
    date_col: str,
    value_col: str,
    name: str,
    color: str = "#58a6ff",
) -> go.Figure:
    """
    1本の時系列を「実績(実線) + 予測(破線)」の2トレースとして fig に追加。

    Parameters
    ----------
    fig : go.Figure
    df : pd.DataFrame
        時系列データ (date_col, value_col を含む)
    date_col, value_col : str
    name : str
        凡例名 (実績/予測の suffix を自動付与)
    color : str
        系列色
    """
    actual, forecast = split_actual_forecast(df, date_col)

    if not actual.empty:
        fig.add_trace(
            go.Scatter(
                x=actual[date_col],
                y=actual[value_col],
                name=f"{name} (実績)",
                mode="lines+markers",
                line=dict(color=color, width=2),
                marker=dict(size=6),
            )
        )
    if not forecast.empty:
        fig.add_trace(
            go.Scatter(
                x=forecast[date_col],
                y=forecast[value_col],
                name=f"{name} (予測)",
                mode="lines+markers",
                line=dict(color=color, width=2, dash="dash"),
                marker=dict(size=6, symbol="circle-open"),
            )
        )
    return fig


# ════════════════════════════════════════════════════════
# DataFrame 表示用: 実績/予測フラグを付与
# ════════════════════════════════════════════════════════
def add_actual_forecast_flag(
    df: pd.DataFrame,
    date_col: str,
    *,
    flag_col: str = "区分",
) -> pd.DataFrame:
    """
    DataFrame に「実績 / 予測」フラグ列を追加して返す（テーブル表示用）。
    """
    today = get_as_of_date()
    parsed = pd.to_datetime(df[date_col], errors="coerce").dt.date
    out = df.copy()
    out[flag_col] = parsed.apply(lambda d: "実績" if d is not pd.NaT and d <= today else "予測")
    return out
