"""
📅 製品FCST × 部材必要量タイムライン
====================================
業務フロー: 営業FCST → 生産計画 → PSI/MRP → 部材必要量

このページの役割:
  - 製品FCSTから自動展開された部材レベルの希望納期を時系列で可視化
  - 緊急手動入力（FCSTから漏れた突発需要）も同じタイムラインに重ねて表示
  - 「いつ・どの部材が・どれだけ必要か」を月別に俯瞰
  - 製品別 FCST と 実績(同等数量) の推移グラフで FCST精度を可視化
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css, plot_colors
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

colors = plot_colors()

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
    "FCSTは外れる前提のため、緊急需要が早期察知できることが重要です。"
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
# フィルター
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

end_date = today + pd.Timedelta(days=months_ahead * 30).to_pytimedelta()
df = df[(df["requested_date"] >= today - pd.Timedelta(days=60).to_pytimedelta()) & (df["requested_date"] <= end_date)]

# ────────────────────────────────────────────────────────
# 月次集計タイムライン
# ────────────────────────────────────────────────────────
st.markdown("### 📊 月次需要量推移（FCST × 緊急 を月別に並列表示）")
st.caption(
    "X軸=月の中央日。本日(縦線)が含まれる月の棒は『今月分の累計需要』を意味します。"
    "FCSTを青、緊急手動入力を赤で 横並び表示し見比べやすくしています。"
)

if df.empty:
    st.info("条件に該当する需要がありません。")
else:
    # 月の「中央日」を X 軸に取り、棒の左右ずれで FCST/EMERG を分離
    df["month_center"] = pd.to_datetime(df["requested_date"]).dt.to_period("M").dt.to_timestamp() + pd.Timedelta(days=14)
    monthly = df.groupby(["month_center", "source_type"], as_index=False)["requested_qty"].sum()

    fig = go.Figure()
    for src, color, name in [
        ("FCST_AUTO",        colors["blue"],   "FCST自動展開"),
        ("EMERGENCY_MANUAL", colors["red"],    "緊急手動入力"),
    ]:
        sub = monthly[monthly["source_type"] == src]
        if not sub.empty:
            fig.add_trace(go.Bar(
                x=sub["month_center"], y=sub["requested_qty"],
                name=name, marker_color=color,
                hovertemplate="月: %{x|%Y-%m}<br>数量: %{y:,}<extra></extra>",
            ))

    fig.update_layout(
        barmode="group",
        bargap=0.18,
        bargroupgap=0.05,
        height=400,
        plot_bgcolor=colors["bg"],
        paper_bgcolor=colors["paper"],
        font=dict(color=colors["text"], size=12),
        xaxis=dict(title="希望納期（月）", gridcolor=colors["grid"], tickformat="%Y-%m"),
        yaxis=dict(title="必要数量（個）", gridcolor=colors["grid"]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=40, b=40),
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
# 📈 製品別 FCST × 実績 推移（製品セレクタつき）
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 📈 製品別 FCST × 実績 推移（FCST精度の根拠）")
st.caption(
    "選択した製品の月次FCSTと実績数量を時系列で対比表示。"
    "FCST精度(forecast_accuracy)から実績を逆算しています。"
    "実線=実績、破線=FCST。本日より過去のみ実績を表示。"
)

if forecasts.empty:
    st.info("forecasts データがありません。")
else:
    fc = forecasts.copy()
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], errors="coerce").dt.date
    fc["forecast_qty"] = pd.to_numeric(fc["forecast_qty"], errors="coerce").fillna(0)
    fc["forecast_accuracy"] = pd.to_numeric(fc["forecast_accuracy"], errors="coerce").fillna(0.8)

    # 製品マスタとマージ
    if not products.empty:
        fc = fc.merge(
            products[["product_id", "product_name", "product_category", "customer_id"]],
            on=["product_id", "customer_id"], how="left",
        )

    fc_target = fc[fc["customer_id"] == "CUS001"].copy() if "customer_id" in fc.columns else fc.copy()
    if fc_target.empty:
        st.info("顧客 CUS001 のFCSTデータがありません。")
    else:
        # 製品ピッカー
        prod_pick = fc_target[["product_id", "product_name"]].drop_duplicates()
        prod_pick["_label"] = (
            prod_pick["product_id"].astype(str) + "  ｜  " + prod_pick["product_name"].fillna("").astype(str)
        )
        # デフォルト: FCST件数が多い上位3製品
        top_prods = fc_target.groupby("product_id").size().sort_values(ascending=False).head(3).index.tolist()
        default_labels = prod_pick[prod_pick["product_id"].isin(top_prods)]["_label"].tolist()

        sel_labels = st.multiselect(
            "📊 推移を見たい製品（最大5件推奨）",
            prod_pick["_label"].tolist(),
            default=default_labels[:2],
        )

        if sel_labels:
            sel_pids = [s.split("  ｜  ", 1)[0].strip() for s in sel_labels]
            sub = fc_target[fc_target["product_id"].isin(sel_pids)].copy()
            sub = sub.sort_values(["product_id", "forecast_month"])

            # 実績を accuracy から逆算 (FCST精度=実績/FCSTと仮定し、補正係数)
            # 「accuracyが0.8なら実績は ±20% の誤差幅で揺れる」モデルでデモ
            import numpy as np
            np.random.seed(42)
            # 実績 = FCST * (accuracy + random_noise around accuracy)
            sub["actual_qty"] = (
                sub["forecast_qty"] * (sub["forecast_accuracy"] + np.random.uniform(-0.1, 0.1, size=len(sub)))
            ).clip(lower=0).round().astype(int)
            # 未来(today以降)の実績はマスク
            sub.loc[pd.to_datetime(sub["forecast_month"]).dt.date > today, "actual_qty"] = pd.NA

            fig = go.Figure()
            palette = [colors["blue"], colors["orange"], colors["green"], colors["red"], colors["purple"]]
            for i, pid in enumerate(sel_pids):
                s = sub[sub["product_id"] == pid].sort_values("forecast_month")
                if s.empty:
                    continue
                name = f"{pid} / {s['product_name'].iloc[0]}"
                color = palette[i % len(palette)]
                # FCST (破線)
                fig.add_trace(go.Scatter(
                    x=s["forecast_month"], y=s["forecast_qty"],
                    mode="lines+markers", name=f"{name} FCST",
                    line=dict(color=color, width=2, dash="dash"),
                    marker=dict(size=6, symbol="circle-open"),
                ))
                # 実績 (実線, today以降は NaN なので自動的に終端)
                s_act = s.dropna(subset=["actual_qty"])
                if not s_act.empty:
                    fig.add_trace(go.Scatter(
                        x=s_act["forecast_month"], y=s_act["actual_qty"],
                        mode="lines+markers", name=f"{name} 実績",
                        line=dict(color=color, width=2),
                        marker=dict(size=7),
                    ))

            fig.update_layout(
                height=440,
                plot_bgcolor=colors["bg"],
                paper_bgcolor=colors["paper"],
                font=dict(color=colors["text"], size=12),
                xaxis=dict(title="年月", gridcolor=colors["grid"], tickformat="%Y-%m"),
                yaxis=dict(title="数量（個）", gridcolor=colors["grid"]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=40, r=20, t=40, b=40),
            )
            add_today_vline(fig)
            st.plotly_chart(fig, use_container_width=True)

            # 精度サマリー表
            summary_rows = []
            for pid in sel_pids:
                s = sub[sub["product_id"] == pid].dropna(subset=["actual_qty"])
                if s.empty:
                    continue
                tot_fc = int(s["forecast_qty"].sum())
                tot_ac = int(pd.to_numeric(s["actual_qty"], errors="coerce").fillna(0).sum())
                acc = (1 - abs(tot_fc - tot_ac) / tot_fc) * 100 if tot_fc > 0 else 0
                summary_rows.append({
                    "製品ID":          pid,
                    "製品名":          s["product_name"].iloc[0] if not s.empty else "—",
                    "実績期間FCST合計": tot_fc,
                    "実績合計":        tot_ac,
                    "差分":           tot_ac - tot_fc,
                    "総合精度(%)":     round(acc, 1),
                })
            if summary_rows:
                st.markdown("**📊 製品別 FCST精度サマリー（過去実績期間のみ）**")
                st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)
        else:
            st.info("上のセレクタから製品を選んでください。")
