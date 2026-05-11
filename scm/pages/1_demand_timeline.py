"""
📅 製品FCST × 部材必要量タイムライン
====================================
業務フロー: 営業FCST → 生産計画 → PSI/MRP → 部材必要量

このページの役割:
  - 製品FCSTから自動展開された部材レベルの希望納期を時系列で可視化
  - 緊急手動入力（FCSTから漏れた突発需要）も同じタイムラインに重ねて表示
  - 「いつ・どの部材が・どれだけ必要か」を月別に俯瞰
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.timeline_helper import add_today_vline, add_actual_forecast_flag
from services.config import get_as_of_date
from services.database import (
    get_silver_demand_plan_components,
    get_silver_components,
    get_silver_products,
    get_silver_forecasts,
)

st.set_page_config(page_title="製品FCST×部材必要量 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="営業FCSTからの部材展開と緊急需要を重ねて表示")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    components = get_silver_components()
    products   = get_silver_products()
    forecasts  = get_silver_forecasts()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 📅 製品FCST × 部材必要量タイムライン")
st.caption(
    "営業からの製品FCSTをBOM展開して算出した部材レベル需要 + 緊急手動入力。"
    "FCSTは外れる前提なので、緊急需要が発生したときに早期察知することが重要です。"
)

if demand.empty:
    st.warning("需要計画データ (silver_demand_plan_components) が空です。")
    st.stop()

# 日付正規化
demand = demand.copy()
demand["requested_date"] = pd.to_datetime(demand["requested_date"], errors="coerce").dt.date
demand["requested_qty"] = pd.to_numeric(demand["requested_qty"], errors="coerce").fillna(0).astype(int)

# 部材名結合
if not components.empty:
    comp_lite = components[["component_id", "part_number", "component_name", "component_category"]]
    demand = demand.merge(comp_lite, on="component_id", how="left")

# ────────────────────────────────────────────────────────
# フィルター（タイトル明確化）
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（タイムラインに集計する需要を絞る）")
fc1, fc2, fc3 = st.columns([2, 2, 1])

with fc1:
    cat_options = ["（すべて）"] + sorted([c for c in demand["component_category"].dropna().unique()])
    sel_cat = st.selectbox(
        "🔍 部材カテゴリで絞り込み（例: MCU、SiC、CAN等）",
        cat_options, index=0,
    )

with fc2:
    src_options = ["（すべて）", "営業FCSTから自動展開", "緊急手動入力"]
    sel_src = st.selectbox(
        "📋 需要発生源（FCST自動展開 or 緊急手動入力）",
        src_options, index=0,
    )

with fc3:
    months_ahead = st.slider(
        "📅 表示月数（今日から何ヶ月先まで）",
        1, 12, 6, help="今日からの先読み月数。グラフのX軸範囲に対応",
    )

# フィルター適用
df = demand.copy()
if sel_cat != "（すべて）":
    df = df[df["component_category"] == sel_cat]
if sel_src != "（すべて）":
    src_map = {"営業FCSTから自動展開": "FCST_AUTO", "緊急手動入力": "EMERGENCY_MANUAL"}
    df = df[df["source_type"] == src_map[sel_src]]

# 期間フィルター
end_date = today + pd.Timedelta(days=months_ahead * 30).to_pytimedelta()
df = df[(df["requested_date"] >= today - pd.Timedelta(days=60).to_pytimedelta()) & (df["requested_date"] <= end_date)]

# ────────────────────────────────────────────────────────
# 月次集計タイムライン
# ────────────────────────────────────────────────────────
st.markdown("### 📊 月次需要量推移")

if df.empty:
    st.info("条件に該当する需要がありません。")
else:
    df["month"] = pd.to_datetime(df["requested_date"]).dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby(["month", "source_type"], as_index=False)["requested_qty"].sum()

    fig = go.Figure()
    # FCST_AUTO を青の棒、EMERGENCY_MANUAL を赤の棒で重ねる
    for src, color, name in [
        ("FCST_AUTO",        "#58a6ff", "FCST自動展開"),
        ("EMERGENCY_MANUAL", "#ff4646", "緊急手動入力"),
    ]:
        sub = monthly[monthly["source_type"] == src]
        if not sub.empty:
            fig.add_trace(go.Bar(
                x=sub["month"], y=sub["requested_qty"],
                name=name, marker_color=color,
            ))

    fig.update_layout(
        barmode="stack",
        height=380,
        plot_bgcolor="#0d1117",
        paper_bgcolor="#0d1117",
        font=dict(color="#e6edf3", size=12),
        xaxis=dict(title="希望納期（月）", gridcolor="#30363d"),
        yaxis=dict(title="必要数量（個）", gridcolor="#30363d"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=30, b=40),
    )
    add_today_vline(fig)
    st.plotly_chart(fig, use_container_width=True)

# ────────────────────────────────────────────────────────
# 部材別 Top10 ランキング
# ────────────────────────────────────────────────────────
st.markdown("### 🏆 部材別 必要数 Top 10")
if not df.empty:
    by_comp = (
        df.groupby(["component_id", "part_number", "component_name"], as_index=False)
        .agg(
            必要数合計=("requested_qty", "sum"),
            需要件数=("demand_id", "count"),
            緊急件数=("source_type", lambda s: (s == "EMERGENCY_MANUAL").sum()),
        )
        .sort_values("必要数合計", ascending=False)
        .head(10)
    )
    by_comp = by_comp.rename(columns={
        "component_id": "部材ID",
        "part_number":  "品番",
        "component_name": "部材名",
    })
    st.dataframe(by_comp, hide_index=True, use_container_width=True)

# ────────────────────────────────────────────────────────
# 緊急手動入力の明細
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 緊急手動入力の明細")
emerg = df[df["source_type"] == "EMERGENCY_MANUAL"].copy()
if emerg.empty:
    st.info("条件に該当する緊急手動入力はありません。")
else:
    emerg = add_actual_forecast_flag(emerg, "requested_date", flag_col="区分")
    show = emerg[[
        "demand_id", "part_number", "component_name",
        "requested_date", "requested_qty", "区分", "note",
    ]].rename(columns={
        "demand_id":      "需要ID",
        "part_number":    "品番",
        "component_name": "部材名",
        "requested_date": "希望納期",
        "requested_qty":  "必要数量",
        "note":           "発生理由",
    })
    st.dataframe(show, hide_index=True, use_container_width=True)
    st.caption(f"💡 緊急需要 {len(emerg)} 件。各需要の調達ルート評価は「🎯 調達アクションセンター」で確認。")

# ────────────────────────────────────────────────────────
# 製品別FCST精度（参考情報）
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 📈 参考: 製品別 FCST精度の推移")
st.caption("過去のFCST精度。営業FCSTがどの程度実績と乖離しているかの参考値。")

if not forecasts.empty:
    fc = forecasts.copy()
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], errors="coerce").dt.date
    fc["forecast_accuracy"] = pd.to_numeric(fc["forecast_accuracy"], errors="coerce")
    # 過去24ヶ月の精度推移
    cutoff_back = today - pd.Timedelta(days=24 * 30).to_pytimedelta()
    fc_recent = fc[(fc["forecast_month"] >= cutoff_back) & (fc["forecast_month"] <= today)]
    if not fc_recent.empty:
        acc_monthly = fc_recent.groupby(
            pd.to_datetime(fc_recent["forecast_month"]).dt.to_period("M").dt.to_timestamp()
        )["forecast_accuracy"].mean().reset_index()
        acc_monthly.columns = ["month", "accuracy"]

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=acc_monthly["month"], y=acc_monthly["accuracy"] * 100,
            mode="lines+markers", line=dict(color="#bc8cff", width=2),
            marker=dict(size=6), name="平均精度（％）",
        ))
        fig2.update_layout(
            height=280,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3", size=12),
            xaxis=dict(title="年月", gridcolor="#30363d"),
            yaxis=dict(title="平均FCST精度（%）", gridcolor="#30363d", range=[0, 100]),
            margin=dict(l=40, r=20, t=20, b=40),
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("FCST実績データが不足しています。")
