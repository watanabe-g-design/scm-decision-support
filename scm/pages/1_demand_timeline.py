"""
📅 製品FCST × 部材必要量タイムライン
====================================
業務フロー: 営業FCST → 生産計画 → PSI/MRP → 部材必要量

このページの役割:
  - 製品FCSTから自動展開された部材レベルの希望納期を時系列で可視化
  - 緊急手動入力（FCSTから漏れた突発需要）も同じタイムラインに重ねて表示
  - 「いつ・どの部材が・どれだけ必要か」を月別に俯瞰
  - 製品別 FCST と 実績(同等数量) の推移グラフで FCST精度を可視化

Phase 7 改修:
  ✅ 部材カテゴリフィルター撤廃 → 部材セレクタへ
  ✅ 月次推移グラフを「積み上げエリア + 各月マーカー」のクリーンな表現に
  ✅ 部材別Top10 を削除 → 代わりに「アクション必要な需要 Top10」を明示
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.timeline_helper import add_today_vline, add_actual_forecast_flag
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens, palette
from services.database import (
    get_silver_demand_plan_components,
    get_silver_components,
    get_silver_products,
    get_silver_forecasts,
    get_procurement_options,
)

st.set_page_config(page_title="製品FCST×部材必要量 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="営業FCSTからの部材展開と緊急需要を重ねて表示")

t = get_theme_tokens()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    components = get_silver_components()
    products   = get_silver_products()
    forecasts  = get_silver_forecasts()
    options    = get_procurement_options()

today = get_as_of_date()

st.markdown("## 📅 製品FCST × 部材必要量タイムライン")
st.caption(
    "営業からの製品FCSTをBOM展開して算出した部材レベル需要 + 緊急手動入力。"
    "FCSTは外れる前提のため、緊急需要が早期察知できることが重要です。"
)

if demand.empty:
    st.warning("需要計画データ (silver_demand_plan_components) が空です。")
    st.stop()

# 日付正規化 + 過去除外
demand = demand.copy()
demand["requested_date"] = pd.to_datetime(demand["requested_date"], errors="coerce").dt.date
demand["requested_qty"] = pd.to_numeric(demand["requested_qty"], errors="coerce").fillna(0).astype(int)
demand = demand[demand["requested_date"] >= today]

if not components.empty:
    demand = demand.merge(
        components[["component_id", "part_number", "component_name"]],
        on="component_id", how="left",
    )

# ────────────────────────────────────────────────────────
# フィルター (部材カテゴリ廃止 → 検索 + セレクタ)
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター")
fc1, fc2 = st.columns(2)
with fc1:
    search_query = render_search_bar(components, key="dt_search")
with fc2:
    src_options_list = ["（すべて）", "営業FCSTから自動展開", "緊急手動入力"]
    sel_src = st.selectbox(
        "📋 需要発生源（FCST自動展開 or 緊急手動入力）",
        src_options_list, index=0,
    )

selected_ids = render_component_selector(components, key="dt_comp_select")

months_ahead = st.slider(
    "📅 表示月数（今日から何ヶ月先まで）",
    1, 12, 9,
    help="今日からの先読み月数。本データは最大9ヶ月先まで網羅。",
)

# フィルター適用
df = demand.copy()
if search_query:
    df = apply_component_search(df, search_query)
if selected_ids:
    df = apply_component_id_filter(df, selected_ids)
if sel_src != "（すべて）":
    src_map = {"営業FCSTから自動展開": "FCST_AUTO", "緊急手動入力": "EMERGENCY_MANUAL"}
    df = df[df["source_type"] == src_map[sel_src]]

end_date = today + timedelta(days=months_ahead * 30)
df = df[df["requested_date"] <= end_date]

st.markdown("---")

# ────────────────────────────────────────────────────────
# 月次需要量推移 (Phase 7: クリーンなエリア+棒で見やすく)
# ────────────────────────────────────────────────────────
st.markdown("### 📊 月次需要量推移")
st.caption(
    "月別の合計必要数量を時系列で表示。**FCST自動展開**(青)と**緊急手動入力**(赤)を併記し、"
    "ある月の需要総量と緊急発生比率が一目で分かるよう設計しています。"
    "縦の点線が本日です。"
)

if df.empty:
    st.info("条件に該当する需要がありません。")
else:
    # 月単位に集計
    df_chart = df.copy()
    df_chart["month"] = pd.to_datetime(df_chart["requested_date"]).dt.to_period("M").dt.to_timestamp()
    df_chart["month_center"] = df_chart["month"] + pd.Timedelta(days=14)
    monthly = df_chart.groupby(["month_center", "source_type"], as_index=False)["requested_qty"].sum()

    # ピボット (月 × source) - 系列ごとに表現
    months_unique = sorted(monthly["month_center"].unique())
    fcst_series = monthly[monthly["source_type"] == "FCST_AUTO"].set_index("month_center")["requested_qty"]
    emerg_series = monthly[monthly["source_type"] == "EMERGENCY_MANUAL"].set_index("month_center")["requested_qty"]
    fcst_vals = [int(fcst_series.get(m, 0)) for m in months_unique]
    emerg_vals = [int(emerg_series.get(m, 0)) for m in months_unique]

    fig = go.Figure()
    # FCST_AUTO: 棒
    fig.add_trace(go.Bar(
        x=months_unique, y=fcst_vals,
        name="営業FCSTから自動展開",
        marker=dict(color=t["blue"], line=dict(width=0)),
        hovertemplate="%{x|%Y年%-m月}<br>FCST需要: %{y:,}個<extra></extra>",
        width=18 * 24 * 60 * 60 * 1000,  # 18 days width in ms
    ))
    # EMERGENCY_MANUAL: 棒 (赤)
    if any(emerg_vals):
        fig.add_trace(go.Bar(
            x=months_unique, y=emerg_vals,
            name="緊急手動入力",
            marker=dict(color=t["red"], line=dict(width=0)),
            hovertemplate="%{x|%Y年%-m月}<br>緊急需要: %{y:,}個<extra></extra>",
            width=18 * 24 * 60 * 60 * 1000,
        ))

    fig.update_layout(
        barmode="stack",
        **base_layout(height=400, x_title="希望納期（月）", y_title="必要数量（個）"),
    )
    fig.update_xaxes(tickformat="%Y-%m")
    add_today_vline(fig)
    st.plotly_chart(fig, use_container_width=True)

# ────────────────────────────────────────────────────────
# 緊急手動入力の明細
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 緊急手動入力の明細")
st.caption("営業FCSTから漏れた、突発的に発生した需要のリスト。各案件の調達ルート評価は調達アクションセンターで確認できます。")

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
# 直近30日で要対応 Top 10 (Phase 7: Top10置換)
# ────────────────────────────────────────────────────────
st.markdown("### 🎯 直近30日で要対応の需要 Top 10")
st.caption(
    "希望納期が**今日から30日以内**で、**顧客在庫だけでは賄えない**需要を絞り込み、"
    "不足数量が大きい順に Top 10 を表示。**朝一の優先対応リスト**として使えます。"
)

if not options.empty and not df.empty:
    opt = options.copy()
    opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date
    opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0)
    opt["days_late"] = pd.to_numeric(opt["days_late"], errors="coerce").fillna(0)
    # demand_id ごとに最良ルート1行に集約
    opt["_score"] = opt["shortage_qty"].clip(lower=0) * 1000 + opt["days_late"]
    best = opt.sort_values("_score").groupby("demand_id").first().reset_index()
    # 直近30日 & 要対応
    cutoff = today + timedelta(days=30)
    needs = best[
        (best["requested_date"] >= today)
        & (best["requested_date"] <= cutoff)
    ].copy()
    if "action_level" in needs.columns:
        needs = needs[needs["action_level"] != "不要"]
    needs = needs.sort_values("shortage_qty", ascending=False).head(10)

    if needs.empty:
        st.success("✅ 直近30日に顧客在庫だけでは賄えない需要はありません。")
    else:
        if not components.empty:
            needs = needs.merge(
                components[["component_id", "part_number", "component_name"]],
                on="component_id", how="left",
            )
        action_label = {"重": "🔴 新規発注必要", "中": "🟠 組合せ要相談", "軽": "🟡 単一ルートで対応可"}
        if "action_level" in needs.columns:
            needs["対応レベル"] = needs["action_level"].map(action_label).fillna(needs["action_level"])
        show = needs.head(10)
        show_cols = [
            ("demand_id",      "需要ID"),
            ("part_number",    "品番"),
            ("component_name", "部材名"),
            ("requested_date", "希望納期"),
            ("requested_qty",  "必要数"),
            ("shortage_qty",   "不足数"),
            ("対応レベル",      "対応レベル"),
        ]
        cols_present = [(k, v) for k, v in show_cols if k in show.columns]
        st.dataframe(
            show[[k for k, _ in cols_present]].rename(columns=dict(cols_present)),
            hide_index=True, use_container_width=True,
        )
        st.caption(f"💡 上記10件すべて確認 + 詳細評価は「🎯 調達アクションセンター」へ。")

# ────────────────────────────────────────────────────────
# 📈 製品別 FCST × 実績 推移
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

    if not products.empty:
        fc = fc.merge(
            products[["product_id", "product_name", "product_category", "customer_id"]],
            on=["product_id", "customer_id"], how="left",
        )

    fc_target = fc[fc["customer_id"] == "CUS001"].copy() if "customer_id" in fc.columns else fc.copy()
    if fc_target.empty:
        st.info("顧客 CUS001 のFCSTデータがありません。")
    else:
        prod_pick = fc_target[["product_id", "product_name"]].drop_duplicates()
        prod_pick["_label"] = (
            prod_pick["product_id"].astype(str) + "  ｜  " + prod_pick["product_name"].fillna("").astype(str)
        )
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

            import numpy as np
            np.random.seed(42)
            sub["actual_qty"] = (
                sub["forecast_qty"] * (sub["forecast_accuracy"] + np.random.uniform(-0.1, 0.1, size=len(sub)))
            ).clip(lower=0).round().astype(int)
            sub.loc[pd.to_datetime(sub["forecast_month"]).dt.date > today, "actual_qty"] = pd.NA

            fig = go.Figure()
            pal = palette()
            for i, pid in enumerate(sel_pids):
                s = sub[sub["product_id"] == pid].sort_values("forecast_month")
                if s.empty:
                    continue
                name = f"{pid} / {s['product_name'].iloc[0]}"
                color = pal[i % len(pal)]
                fig.add_trace(go.Scatter(
                    x=s["forecast_month"], y=s["forecast_qty"],
                    mode="lines+markers", name=f"{name} FCST",
                    line=dict(color=color, width=2, dash="dash"),
                    marker=dict(size=7, symbol="circle-open"),
                ))
                s_act = s.dropna(subset=["actual_qty"])
                if not s_act.empty:
                    fig.add_trace(go.Scatter(
                        x=s_act["forecast_month"], y=s_act["actual_qty"],
                        mode="lines+markers", name=f"{name} 実績",
                        line=dict(color=color, width=2.5),
                        marker=dict(size=8),
                    ))

            fig.update_layout(**base_layout(height=440, x_title="年月", y_title="数量（個）"))
            fig.update_xaxes(tickformat="%Y-%m")
            add_today_vline(fig)
            st.plotly_chart(fig, use_container_width=True)

            # 精度サマリー
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
